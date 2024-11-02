from lossy_socket import LossyUDP
from socket import INADDR_ANY
import struct
from concurrent.futures import ThreadPoolExecutor
import threading
import time

class Streamer:
    def __init__(self, dst_ip, dst_port,
                 src_ip=INADDR_ANY, src_port=0):
        """Default values listen on all network interfaces, chooses a random source port,
           and does not introduce any simulated packet loss."""
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port

        self.maxSize = 1472  # Max chunk size for a single UDP packet
        self.receiveBuffer = {}  # Buffer for out-of-order packets
        self.dataQueue = []      # Queue for ordered data for recv
        self.sendNumber = 0
        self.receiveNumber = 0
        self.ack_received = False  # Flag to check if ACK is received
        self.closed = False        # Control flag to stop the listener
        self.buffer_lock = threading.Lock()

        # Start the background listener thread
        self.executor = ThreadPoolExecutor(max_workers=1)
        self.executor.submit(self.listener)

    def send(self, data_bytes: bytes) -> None:
        """Sends data, segmenting it into chunks if it exceeds packet size."""
        chunkSize = self.maxSize - 5  # Reduced by 5 for the header (1 byte for type, 4 for sequence number)
        for i in range(0, len(data_bytes), chunkSize):
            startIndex = i
            endIndex = i + chunkSize
            current = data_bytes[startIndex: endIndex]  # Current chunk content

            # Create header with a type byte (0 for data, 1 for ACK) and 4 bytes for sequence number
            header = struct.pack('!BI', 0, self.sendNumber)  # 0 indicates a data packet
            packet = header + current
            self.socket.sendto(packet, (self.dst_ip, self.dst_port))
            print(f"Sent data packet with sequence number: {self.sendNumber}, Data: {current}")
            self.sendNumber += 1

            # Wait for ACK
            self.ack_received = False
            while not self.ack_received:
                time.sleep(0.01)  # Short sleep to avoid busy waiting

    def listener(self):
        """Background listener function that continuously receives packets."""
        while not self.closed:
            try:
                packet, _ = self.socket.recvfrom()
                packet_type, sequenceNumber = struct.unpack('!BI', packet[:5])
                data = packet[5:]

                with self.buffer_lock:
                    if packet_type == 0:  # Data packet
                        if sequenceNumber == self.receiveNumber:
                            print(f"Received in-order data packet with sequence number: {sequenceNumber}")
                            self.dataQueue.append(data)
                            self.receiveNumber += 1

                            # Process any consecutive packets in buffer
                            while self.receiveNumber in self.receiveBuffer:
                                self.dataQueue.append(self.receiveBuffer.pop(self.receiveNumber))
                                print(f"Processing buffered data packet with sequence number: {self.receiveNumber}")
                                self.receiveNumber += 1

                            # Send ACK for the received packet
                            ack_packet = struct.pack('!BI', 1, sequenceNumber)  # 1 indicates an ACK packet
                            self.socket.sendto(ack_packet, (self.dst_ip, self.dst_port))
                            print(f"Sent ACK for sequence number: {sequenceNumber}")

                        elif sequenceNumber > self.receiveNumber:
                            print(f"Buffered out-of-order data packet with sequence number: {sequenceNumber}")
                            self.receiveBuffer[sequenceNumber] = data

                    elif packet_type == 1:  # ACK packet
                        if sequenceNumber == self.sendNumber - 1:
                            print(f"Received ACK for sequence number: {sequenceNumber}")
                            self.ack_received = True  # Signal that ACK was received for the last sent packet

            except Exception as e:
                print("Listener died!")
                print(e)

    def recv(self) -> bytes:
        """Retrieves data from the receive buffer, blocking until data is available."""
        while True:
            with self.buffer_lock:
                if self.dataQueue:
                    return b''.join(self.dataQueue.pop(0) for _ in range(len(self.dataQueue)))

    def close(self) -> None:
        """Stops the listener thread and performs necessary cleanup."""
        self.closed = True
        self.socket.stoprecv()
        self.executor.shutdown(wait=True)