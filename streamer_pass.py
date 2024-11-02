# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY
import math
import struct


class Streamer:
    def __init__(self, dst_ip, dst_port,
                 src_ip=INADDR_ANY, src_port=0):
        """Default values listen on all network interfaces, chooses a random source port,
           and does not introduce any simulated packet loss."""
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port

        self.maxSize = 1472 # the max chunk size for the single UDP
        self.receiveChunk = {}
        self.sendNumber = 0
        self.receiveNumber = 0

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        # Your code goes here!  The code below should be changed!
        chunkSize = self.maxSize - 4
        for i in range(0, len(data_bytes), chunkSize):
            startIndex = i
            endIndex = i + chunkSize
            current = data_bytes[startIndex : endIndex] #current chunk content

            header = struct.pack('!I', self.sendNumber)

            packet = header + current
            self.socket.sendto(packet, (self.dst_ip, self.dst_port))
            print(f"Sent packet with sequence number: {self.sendNumber}, Data: {current}")
            self.sendNumber += 1


    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        # your code goes here!  The code below should be changed!

        while True:
            packet, _ = self.socket.recvfrom()
            currentSequence = struct.unpack('!I', packet[:4])[0]
            data = packet[4:]

            if currentSequence == self.receiveNumber:
                print(f"Received packet with sequence number: {currentSequence}")
                self.receiveNumber += 1

                result = data

                while self.receiveNumber in self.receiveChunk:
                    result += self.receiveChunk.pop(self.receiveNumber)
                    print(f"Processing packet with sequence number: {self.receiveNumber}")
                    self.receiveNumber += 1
                
                return result
            elif currentSequence > self.receiveNumber:
                
                print(f"Buffered out-of-order seq {currentSequence}")
                self.receiveChunk[currentSequence] = data 

            

        # this sample code just calls the recvfrom method on the LossySocket
        #data, addr = self.socket.recvfrom()
        # For now, I'll just pass the full UDP payload to the app
        #return data

    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        self.socket.stoprecv()
