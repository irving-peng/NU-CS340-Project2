# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY
import struct
import time
from socket import timeout
import select

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
        self.receiveChunk = {}  # Buffer for out-of-order packets
        self.sendNumber = 0
        self.receiveNumber = 0

    def send(self, data_bytes: bytes) -> None:
        """Sends data with ACK handling and 0.25s timer for retransmission."""
        chunkSize = self.maxSize - 5  # Reduced by 5 for a header (1 byte for type, 4 for sequence number)
        for i in range(0, len(data_bytes), chunkSize):
            startIndex = i
            endIndex = i + chunkSize
            current = data_bytes[startIndex: endIndex]  # Current chunk content

            # Create header with a type byte (0 for data, 1 for ACK) and 4 bytes for sequence number
            header = struct.pack('!BI', 0, self.sendNumber)  # 0 indicates a data packet
            packet = header + current

            # Keep resending the packet until ACK is received
            while True:
                # Send the packet
                self.socket.sendto(packet, (self.dst_ip, self.dst_port))
                print(f"Sent data packet with sequence number: {self.sendNumber}, Data: {current}")

                # Wait for ACK for up to 0.25 seconds
                ack_received = self._wait_for_ack(self.sendNumber)
                
                # Check if ACK was received or if retransmission is needed
                if ack_received:
                    print(f"ACK received for packet {self.sendNumber}")
                    break  # ACK received, exit loop to send next packet
                else:
                    print(f"_wait_for_ack returned False for packet {self.sendNumber}. Timeout waiting for ACK, resending...")

            # Increment send number for the next packet
            self.sendNumber += 1

    def _wait_for_ack(self, expected_sequence: int) -> bool:
            """Helper function to wait for a specific ACK with a 0.25s timeout using select to avoid blocking."""
            start_time = time.time()
            
            while time.time() - start_time < 0.25:  # 0.25-second timeout for waiting for ACK
                try:
                    # Use select to check if data is available on the socket with a 0.1-second timeout
                    ready = select.select([self.socket], [], [], 0.1)
                    
                    if ready[0]:  # If data is available, only then call recvfrom
                        packet, addr = self.socket.recvfrom()  # Attempt to receive packet
                        packet_type, sequenceNumber = struct.unpack('!BI', packet[:5])

                        if packet_type == 1 and sequenceNumber == expected_sequence:  # Check for ACK packet
                            print(f"ACK received for sequence number: {sequenceNumber} in _wait_for_ack from {addr}")
                            return True  # ACK received for expected sequence number
                    else:
                        # No data available, timeout after 0.1s, retry until 0.25s expires
                        print(f"No data received for expected ACK {expected_sequence}, retrying within timeout...")

                except Exception as e:
                    print("Error in receiving ACK:", e)

            # Timeout reached without receiving the expected ACK
            print(f"ACK wait timed out for sequence number: {expected_sequence}, returning False from _wait_for_ack")
            return False

    # ... (remaining methods)
    def recv(self) -> bytes:
        """Receives data and sends ACKs for received packets."""
        while True:
            packet, addr = self.socket.recvfrom()
            packet_type, currentSequence = struct.unpack('!BI', packet[:5])
            data = packet[5:]

            if packet_type == 0:  # Data packet
                if currentSequence == self.receiveNumber:
                    print(f"Received in-order data packet with sequence number: {currentSequence}")
                    self.receiveNumber += 1
                    result = data

                    # Send ACK for the received packet
                    ack_packet = struct.pack('!BI', 1, currentSequence)  # 1 indicates an ACK packet
                    self.socket.sendto(ack_packet, addr)
                    print(f"Sent ACK for sequence number: {currentSequence}")

                    # Process any buffered, consecutive packets
                    while self.receiveNumber in self.receiveChunk:
                        result += self.receiveChunk.pop(self.receiveNumber)
                        print(f"Processing buffered data packet with sequence number: {self.receiveNumber}")
                        self.receiveNumber += 1
                    
                    return result
                elif currentSequence > self.receiveNumber:
                    # Buffer out-of-order packets
                    print(f"Buffered out-of-order data packet with sequence number: {currentSequence}")
                    self.receiveChunk[currentSequence] = data
            elif packet_type == 1:  # ACK packet
                # Ignore ACK packets in `recv`, as `send` handles ACK processing
                print(f"Ignored ACK packet in recv with sequence number: {currentSequence}")

    def close(self) -> None:
        """Cleans up by stopping socket reception."""
        self.socket.stoprecv()
