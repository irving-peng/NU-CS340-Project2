from lossy_socket import LossyUDP
from socket import INADDR_ANY
import struct
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Lock, Timer

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
            self.ack_store = {}
            self.unacked_packets = {} 
            self.ack_lock = Lock()
            self.retransmit_timer = None
            self.retransmit_interval = 0.3  
            self.window_size = 10 
            self.lock = Lock()
            self.a = 0
            self.b = 0
            self.executor = ThreadPoolExecutor(max_workers=1)
            self.executor.submit(self.listener)

    def send(self, data_bytes: bytes) -> None:
        chunkSize = self.maxSize - 5
        for i in range(0, len(data_bytes), chunkSize):
            current = data_bytes[i:i + chunkSize]
            header = struct.pack('!BI', 0, self.sendNumber)
            packet = header + current
            with self.ack_lock:
                self.unacked_packets[self.sendNumber] = packet
            self.socket.sendto(packet, (self.dst_ip, self.dst_port))
            print(f"Sent data packet with sequence number: {self.sendNumber}")
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
                    print(f"Retransmitting packet {seq_num}")
            if retries >= max_retries:

                self.retransmit_interval = min(self.retransmit_interval * 2, 1.0)
            else:
                self.retransmit_interval = 0.3  
        if self.window_base < self.sendNumber:
            self._start_retransmit_timer()
        else:
            self.retransmit_timer = None

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
                        if sequenceNumber >= self.window_base:
                            self.window_base = sequenceNumber + 1
                            del self.unacked_packets[sequenceNumber]
                    print(f"ACK received for packet {sequenceNumber}")

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

    def update(self):
        with self.lock:

            self.a += 1
            self.b -= 1

    def get_sum(self):
        with self.lock:
            return self.a + self.b
