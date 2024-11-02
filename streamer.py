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
        self.retry_count = {}
        self.dataQueue = []
        self.unacked_packets = {}
        self.ack_lock = Lock()
        self.retransmit_timer = None
        self.retransmit_interval = 0.1
        self.window_size = 10
        self.max_retries = 5
        self.acknowledged_packets = set()  # Track packets that are ACKed but not yet confirmed removed
        self.lock = Lock()

        self.nagleBuffer = bytearray()
        self.nagleTimeout = 0.05 
        self.nagleTimer = None

        self.pendingAck = None
        self.ackDelay = 0.05

        self.executor = ThreadPoolExecutor(max_workers=1)
        self.executor.submit(self.listener)

    def _compute_hash(self, packet_type, sequenceNumber, data: bytes):
        hash_input = struct.pack('!BI', packet_type, sequenceNumber) + data
        return hashlib.md5(hash_input).digest()[:4]

    def send(self, data_bytes) -> None:
        chunkSize = self.maxSize - 9

        for i in range(0, len(data_bytes), chunkSize):
            current = data_bytes[i:i + chunkSize]
            self.nagleBuffer.extend(current)

            if len(self.nagleBuffer) >= chunkSize or i + chunkSize >= len(data_bytes):
                self.handleNagleBuffer()
    
    def handleNagleBuffer(self):
        if self.nagleBuffer:
            packet_type = 0
            header = struct.pack('!BI', packet_type, self.sendNumber)
            hash_value = self._compute_hash(packet_type, self.sendNumber,self.nagleBuffer)
            packet = header + hash_value + self.nagleBuffer

            with self.ack_lock:
                self.unacked_packets[self.sendNumber] = packet
                self.retry_count[self.sendNumber] = 0  
        
        self.socket.sendto(packet, (self.dst_ip, self.dst_port))
        print(f"Sending Data... Sequence Number: {self.sendNumber}")

        if not self.retransmit_timer or not self.retransmit_timer.is_alive():
            self._start_retransmit_timer()

        self.sendNumber += 1
        self.nagleBuffer.clear()

    def _start_retransmit_timer(self):
        if self.retransmit_timer and self.retransmit_timer.is_alive():
            self.retransmit_timer.cancel()

        self.retransmit_timer = Timer(self.retransmit_interval, self._retransmit_packets)
        self.retransmit_timer.start()

    def _retransmit_packets(self):
        with self.ack_lock:

            unacked = [
                SN for SN in range(self.window_base, min(self.window_base + self.window_size, self.sendNumber))
                if SN in self.unacked_packets and SN not in self.acknowledged_packets
            ]
            if unacked:
                for SQN in unacked:
                    if self.retry_count.get(SQN, 0) < self.max_retries:
                        self.retry_count[SQN] += 1
                        self.socket.sendto(self.unacked_packets[SQN], (self.dst_ip, self.dst_port))
                        print(f"Retransmitting packet {SQN} (try again for:  {self.retry_count[SQN]})")
                    else:
                        print(f"Packet {SQN} reach max tries and will not be retransmitted.")
                        del self.unacked_packets[SQN]
                        del self.retry_count[SQN]

        if self.unacked_packets:
            self._start_retransmit_timer()
        else:
            self.retransmit_timer = None

    def listener(self):
        while not self.closed:
            try:
                packet, addr = self.socket.recvfrom()
                if len(packet) < 9:
                    print("[Client] incomplete packet --- discard")
                    continue

                packet_type, sequenceNumber = struct.unpack('!BI', packet[:5])
                received_hash = packet[5:9]
                data = packet[9:]
                
                expected_hash = self._compute_hash(packet_type, sequenceNumber, data)
                if not received_hash == expected_hash:
                    print(f"[Client] Discarded corrupted packet {sequenceNumber}")
                    continue

                if packet_type == 0:
                    self.handleDataPacket(sequenceNumber, data, addr)
                elif packet_type == 1:
                    self.handleAckPacket(sequenceNumber)

            except Exception as e:
                print("Client error:", e)
    
    def handleDataPacket(self, sequence_Number, data, addr):
        if sequence_Number < self.receiveNumber:
            self.sendDelayAck(sequence_Number, addr)
        elif sequence_Number == self.receiveNumber:
            self.dataQueue.append(data)
            self.receiveNumber += 1
            self.sendDelayAck(sequence_Number, addr)

            while self.receiveNumber in self.receiveBuffer:
                self.dataQueue.append(self.receiveBuffer.pop(self.receiveNumber))
                self.receiveNumber +=1
        else:
            self.receiveBuffer[sequence_Number] = data

    def sendDelayAck(self, sequenceNumber, addr):
        if not self.pendingAck : # NO ACK
            self.pendingAck = (sequenceNumber, addr)
            Timer(self.ackDelay, self.handleDelayedAck).start()

    def handleDelayedAck(self):
        if self.pendingAck:
            sequenceNumber, addr = self.pendingAck
            packetAck = struct.pack('!BI', 1, sequenceNumber) + self._compute_hash(1, sequenceNumber, b'')
            self.socket.sendto(packetAck,addr)
            print(f"Sending delayed ACK for packet {sequenceNumber}")
            self.pendingAck = None
    
    def handleAckPacket(self, sequenceNumber):
        with self.ack_lock:
            if sequenceNumber >= self.window_base:
                self.window_base = sequenceNumber + 1
                if sequenceNumber in self.unacked_packets:
                    del self.unacked_packets[sequenceNumber]
                    del self.retry_count[sequenceNumber]
                self.acknowledged_packets.add(sequenceNumber)
            print(f"ACK received for packet No.{sequenceNumber}")
            
    def recv(self):
        result = bytearray()
        while True:
            if self.dataQueue:
                result.extend(self.dataQueue.pop(0))
            while self.receiveNumber in self.receiveBuffer:
                result.extend(self.receiveBuffer.pop(self.receiveNumber))
                self.receiveNumber = self.receiveNumber + 1
            if result:
                return bytes(result)
    # for closing the project
    def close(self): 
        retry_count = 0
        max_retries = 10 
        
        while self.window_base < self.sendNumber and retry_count < max_retries:
            time.sleep(0.1)  
            retry_count = retry_count + 1

        fin_packet = struct.pack('!BI', 2, self.sendNumber) + self._compute_hash(2, self.sendNumber, b'')
        self.socket.sendto(fin_packet, (self.dst_ip, self.dst_port))
        self.closed = True

        if self.retransmit_timer:
            self.retransmit_timer.cancel()
        self.socket.stoprecv()
        self.executor.shutdown(wait=True)
