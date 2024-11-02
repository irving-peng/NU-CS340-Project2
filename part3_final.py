from lossy_socket import LossyUDP
from socket import INADDR_ANY
import struct
import time
from threading import Lock
from concurrent.futures import ThreadPoolExecutor

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

    def send(self, data_bytes: bytes) -> None:
        chunkSize = self.maxSize - 5
        for i in range(0, len(data_bytes), chunkSize):
            current = data_bytes[i:i + chunkSize]
            header = struct.pack('!BI', 0, self.sendNumber)
            packet = header + current

            while True:
                self.socket.sendto(packet, (self.dst_ip, self.dst_port))
                print(f"Sent data packet with sequence number: {self.sendNumber}")

                if self._wait_for_ack(self.sendNumber):
                    print(f"ACK received for packet {self.sendNumber}")
                    break
                print(f"Resending packet {self.sendNumber}")

            self.sendNumber += 1

    def _wait_for_ack(self, expected_sequence: int) -> bool:
        start_time = time.time()
        while time.time() - start_time < 0.25:
            with self.ack_lock:
                if expected_sequence in self.ack_store:
                    del self.ack_store[expected_sequence]
                    return True
            time.sleep(0.05)
        return False

    def listener(self):
        while not self.closed:
            try:
                packet, addr = self.socket.recvfrom()
                packet_type, sequenceNumber = struct.unpack('!BI', packet[:5])
                data = packet[5:]

                if packet_type == 0:
                    if sequenceNumber < self.receiveNumber:
                        ack_packet = struct.pack('!BI', 1, sequenceNumber)
                        self.socket.sendto(ack_packet, addr)
                    elif sequenceNumber == self.receiveNumber:
                        self.dataQueue.append(data)
                        self.receiveNumber += 1
                        ack_packet = struct.pack('!BI', 1, sequenceNumber)
                        self.socket.sendto(ack_packet, addr)
                    else:
                        self.receiveBuffer[sequenceNumber] = data

                elif packet_type == 1:
                    with self.ack_lock:
                        self.ack_store[sequenceNumber] = True

            except Exception as e:
                print("Listener died!", e)

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