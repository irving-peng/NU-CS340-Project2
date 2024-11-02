import time
from threading import Lock
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
        self.receiveBuffer = {}
        self.dataQueue = []
        self.sendNumber = 0
        self.receiveNumber = 0
        self.closed = False
        self.ack_store = {}
        self.ack_lock = Lock()
        self.executor = ThreadPoolExecutor(max_workers=1)
        self.executor.submit(self.listener)

    def _compute_hash(self, flags: int, sequence: int, data: bytes) -> bytes:
        """Compute hash over flags, sequence number, and data payload."""
        hash_input = struct.pack('!BI', flags, sequence) + data
        return hashlib.md5(hash_input).digest()[:4]

    def send(self, data_bytes: bytes) -> None:
        chunkSize = self.maxSize - 9
        for i in range(0, len(data_bytes), chunkSize):
            current = data_bytes[i:i + chunkSize]
            flags = 0 
            header = struct.pack('!BI', flags, self.sendNumber)
            hash_value = self._compute_hash(flags, self.sendNumber, current)
            packet = header + hash_value + current
            retries = 0
            while retries < 10:  
                self.socket.sendto(packet, (self.dst_ip, self.dst_port))
                print(f"[Sender] Sent data packet {self.sendNumber} with hash {hash_value.hex()}")
                if self._wait_for_ack(self.sendNumber, timeout=0.5):
                    print(f"[Sender] ACK received for packet {self.sendNumber}")
                    break
                print(f"[Sender] Resending packet {self.sendNumber} (attempt {retries + 1})")
                retries += 1
            else:
                print(f"[Sender] Max retries reached for packet {self.sendNumber}, stopping transmission")
            self.sendNumber += 1

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
                expected_hash = self._compute_hash(flags, sequenceNumber, data)
                if expected_hash != received_hash:
                    print(f"[Listener] Discarded corrupted packet {sequenceNumber}")
                    continue
                if flags == 0:  
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
                elif flags == 1: 
                    print(f"[Listener] Received ACK for packet {sequenceNumber}")
                    with self.ack_lock:
                        self.ack_store[sequenceNumber] = True
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
        self.closed = True
        self.socket.stoprecv()
        self.executor.shutdown(wait=True)
