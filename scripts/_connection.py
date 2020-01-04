"""@package connection
Connection module for interprocess communication
"""

import socket
from time import sleep

class Connection:
    write_count = 0

    def __init__(self, ip, port, timeout = None):
        self.ip = ip
        self.port = port
        self.timeout = timeout

        self.socket = None

    def __del__(self):
        if self.socket:
            self.socket.close()

    def connect(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self.socket.connect((self.ip, self.port))
        self.socket.settimeout(self.timeout)
        self.socket.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)

    def close(self):
        self.socket.close()

    def writeline(self, data):
        self.socket.send((data + '\n').encode())
        Connection.write_count += 1

    def readline(self):
        result = str()

        while True:
            try:
                characters = self.socket.recv(1024)
                result += characters.decode('utf-8')

                if len(result) == 0 or ord(result[-1]) == 10:
                    break

            except socket.timeout:
                break


        return result.strip()

    def flush(self):
        self.readline()

    def fileno(self):
        return self.socket.fileno()
