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
        self.maxSize = 1472
        self.window_base = 0
        self.sendNumber = 0
        self.receiveNumber = 0
        self.closed = False
        self.receiveBuffer = {}
        self.dataQueue = []
        self.unacked_packets = {}
        self.ack_lock = Lock()
        self.retransmit_timer = None
        self.retransmit_interval = 0.1
        self.window_size = 10
        self.retry_count = {}
        self.max_retries = 5
        self.acknowledged_packets = set()  # Track packets that are ACKed but not yet confirmed removed
        self.lock = Lock()
        self.executor = ThreadPoolExecutor(max_workers=1)
        self.executor.submit(self.listener)

    def _compute_hash(self, packet_type: int, sequenceNumber: int, data: bytes) -> bytes:
        """Compute a hash for the packet to verify data integrity."""
        hash_input = struct.pack('!BI', packet_type, sequenceNumber) + data
        return hashlib.md5(hash_input).digest()[:4]

    def send(self, data_bytes: bytes) -> None:
        chunkSize = self.maxSize - 9
        for i in range(0, len(data_bytes), chunkSize):
            current = data_bytes[i:i + chunkSize]
            packet_type = 0
            header = struct.pack('!BI', packet_type, self.sendNumber)
            hash_value = self._compute_hash(packet_type, self.sendNumber, current)
            packet = header + hash_value + current
            with self.ack_lock:
                self.unacked_packets[self.sendNumber] = packet
                self.retry_count[self.sendNumber] = 0  # Initialize retry count
            self.socket.sendto(packet, (self.dst_ip, self.dst_port))
            print(f"Sent data packet with sequence number: {self.sendNumber}")
            if not self.retransmit_timer:
                self._start_retransmit_timer()
            self.sendNumber += 1

    def _start_retransmit_timer(self):
        self.retransmit_timer = Timer(self.retransmit_interval, self._retransmit_packets)
        self.retransmit_timer.start()

    def _retransmit_packets(self):
        with self.ack_lock:
            # Determine packets genuinely missing based on ACK gaps
            unacked = [
                seq_num for seq_num in range(self.window_base, min(self.window_base + self.window_size, self.sendNumber))
                if seq_num in self.unacked_packets and seq_num not in self.acknowledged_packets
            ]
            if unacked:
                for seq_num in unacked:
                    if self.retry_count[seq_num] < self.max_retries:
                        self.retry_count[seq_num] += 1
                        self.socket.sendto(self.unacked_packets[seq_num], (self.dst_ip, self.dst_port))
                        print(f"Retransmitting packet {seq_num} (retry {self.retry_count[seq_num]})")
                    else:
                        print(f"Packet {seq_num} exceeded max retries and will not be retransmitted.")
                        del self.unacked_packets[seq_num]
                        del self.retry_count[seq_num]

            # Dynamic adjustment of window size
            if len(unacked) < self.window_size // 2:
                self.window_size = min(self.window_size + 1, 50)
            else:
                self.window_size = max(self.window_size - 1, 10)

        if self.unacked_packets:
            self._start_retransmit_timer()
        else:
            self.retransmit_timer = None

    def listener(self):
        while not self.closed:
            try:
                packet, addr = self.socket.recvfrom()
                packet_type, sequenceNumber = struct.unpack('!BI', packet[:5])
                received_hash = packet[5:9]
                data = packet[9:]
                
                # Verify packet integrity
                expected_hash = self._compute_hash(packet_type, sequenceNumber, data)
                if received_hash != expected_hash:
                    print(f"[Listener] Discarded corrupted packet {sequenceNumber}")
                    continue

                if packet_type == 0:
                    if sequenceNumber < self.receiveNumber:
                        ack_packet = struct.pack('!BI', 1, sequenceNumber) + self._compute_hash(1, sequenceNumber, b'')
                        self.socket.sendto(ack_packet, addr)
                    elif sequenceNumber == self.receiveNumber:
                        self.dataQueue.append(data)
                        self.receiveNumber += 1
                        ack_packet = struct.pack('!BI', 1, sequenceNumber) + self._compute_hash(1, sequenceNumber, b'')
                        self.socket.sendto(ack_packet, addr)
                    else:
                        self.receiveBuffer[sequenceNumber] = data

                elif packet_type == 1:
                    with self.ack_lock:
                        if sequenceNumber >= self.window_base:
                            self.window_base = sequenceNumber + 1
                            if sequenceNumber in self.unacked_packets:
                                del self.unacked_packets[sequenceNumber]
                                del self.retry_count[sequenceNumber]
                                self.acknowledged_packets.add(sequenceNumber)  # Track in acknowledged set
                    print(f"ACK received for packet {sequenceNumber}")

            except Exception as e:
                print("Listener error:", e)

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

        fin_packet = struct.pack('!BI', 2, self.sendNumber) + self._compute_hash(2, self.sendNumber, b'')
        self.socket.sendto(fin_packet, (self.dst_ip, self.dst_port))
        self.closed = True

        if self.retransmit_timer:
            self.retransmit_timer.cancel()
        self.socket.stoprecv()
        self.executor.shutdown(wait=True)
