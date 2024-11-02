import time
from threading import Lock, Timer
from concurrent.futures import ThreadPoolExecutor
import hashlib
from lossy_socket import LossyUDP
from socket import INADDR_ANY
import struct

class Streamer:
    def __init__(self, dst_ip, dst_port, src_ip=INADDR_ANY, src_port=0):
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port

        # Packet and window parameters
        self.maxSize = 1472
        self.window_base = 0
        self.sendNumber = 0
        self.receiveNumber = 0
        self.window_size = 10
        self.retransmit_interval = 0.3
        self.unacked_packets = {}
        self.closed = False
        self.receiveBuffer = {}
        self.dataQueue = []
        self.ack_store = {}

        # Thread locks
        self.ack_lock = Lock()
        self.lock = Lock()

        # Thread pool for listener
        self.executor = ThreadPoolExecutor(max_workers=1)
        self.executor.submit(self.listener)
        
        # Retransmission timer
        self.retransmit_timer = None

    def _compute_hash(self, flags: int, sequence: int, data: bytes) -> bytes:
        """Compute hash over flags, sequence number, and data payload."""
        hash_input = struct.pack('!BI', flags, sequence) + data
        return hashlib.md5(hash_input).digest()[:4]

    def send(self, data_bytes: bytes) -> None:
        chunkSize = self.maxSize - 9
        for i in range(0, len(data_bytes), chunkSize):
            current = data_bytes[i:i + chunkSize]
            flags = 0  # Flag for data packet
            header = struct.pack('!BI', flags, self.sendNumber)
            hash_value = self._compute_hash(flags, self.sendNumber, current)
            packet = header + hash_value + current

            # Store packet in unacked_packets for potential retransmission
            with self.ack_lock:
                self.unacked_packets[self.sendNumber] = packet

            # Send packet
            self.socket.sendto(packet, (self.dst_ip, self.dst_port))
            print(f"[Sender] Sent data packet {self.sendNumber} with hash {hash_value.hex()}")

            # Start retransmit timer if it's not already running
            if not self.retransmit_timer:
                self._start_retransmit_timer()

            self.sendNumber += 1

    def _start_retransmit_timer(self):
        self.retransmit_timer = Timer(self.retransmit_interval, self._retransmit_packets)
        self.retransmit_timer.start()

    def _retransmit_packets(self):
        max_retries = 5
        with self.ack_lock:
            retries = 0
            for seq_num in range(self.window_base, min(self.window_base + self.window_size, self.sendNumber)):
                if seq_num in self.unacked_packets:
                    retries += 1
                    self.socket.sendto(self.unacked_packets[seq_num], (self.dst_ip, self.dst_port))
                    print(f"[Sender] Retransmitting packet {seq_num}")
            if retries >= max_retries:
                self.retransmit_interval = min(self.retransmit_interval * 2, 1.0)
            else:
                self.retransmit_interval = 0.3
        if self.window_base < self.sendNumber:
            self._start_retransmit_timer()
        else:
            self.retransmit_timer = None

    def _wait_for_ack(self, expected_sequence: int, timeout: float) -> bool:
        start_time = time.time()
        while time.time() - start_time < timeout:
            with self.ack_lock:
                if expected_sequence in self.ack_store:
                    print(f"[ACK Check] ACK found for packet {expected_sequence}")
                    del self.ack_store[expected_sequence]
                    return True
            time.sleep(0.05)
        print(f"[ACK Check] Timeout reached for packet {expected_sequence}, no ACK received")
        return False
    
    def listener(self):
        while not self.closed:
            try:
                packet, addr = self.socket.recvfrom()
                flags, sequenceNumber = struct.unpack('!BI', packet[:5])
                received_hash = packet[5:9]
                data = packet[9:]

                # Validate packet with hash
                expected_hash = self._compute_hash(flags, sequenceNumber, data)
                if expected_hash != received_hash:
                    print(f"[Listener] Discarded corrupted packet {sequenceNumber}")
                    continue

                if flags == 0:  # Data packet
                    ack_flags = 1
                    ack_packet = struct.pack('!BI', ack_flags, sequenceNumber) + self._compute_hash(ack_flags, sequenceNumber, b'')
                    self.socket.sendto(ack_packet, addr)
                    print(f"[Listener] Sent ACK for packet {sequenceNumber}")

                    if sequenceNumber == self.receiveNumber:
                        self.dataQueue.append(data)
                        self.receiveNumber += 1
                        while self.receiveNumber in self.receiveBuffer:
                            self.dataQueue.append(self.receiveBuffer.pop(self.receiveNumber))
                            self.receiveNumber += 1
                    elif sequenceNumber > self.receiveNumber:
                        self.receiveBuffer[sequenceNumber] = data

                elif flags == 1:  # ACK packet
                    print(f"[Listener] Received ACK for packet {sequenceNumber}")
                    with self.ack_lock:
                        if sequenceNumber >= self.window_base:
                            self.window_base = sequenceNumber + 1
                            del self.unacked_packets[sequenceNumber]
            except Exception as e:
                print("[Listener Error]", e)

    def recv(self) -> bytes:
        result = bytearray()
        while True:
            if self.dataQueue:
                result.extend(self.dataQueue.pop(0))
            while self.receiveNumber in self.receiveBuffer:
                result.extend(self.receiveBuffer.pop(self.receiveNumber))
                self.receiveNumber += 1
            if result:
                return bytes(result)
            
    def close(self) -> None:
        retry_count = 0
        max_retries = 10 
        
        while self.window_base < self.sendNumber and retry_count < max_retries:
            time.sleep(0.1)  
            retry_count += 1

        fin_packet = struct.pack('!BI', 2, self.sendNumber)
        self.socket.sendto(fin_packet, (self.dst_ip, self.dst_port))
        self.closed = True

        if self.retransmit_timer:
            self.retransmit_timer.cancel()
        self.socket.stoprecv()
        self.executor.shutdown(wait=True)
