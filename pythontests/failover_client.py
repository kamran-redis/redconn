import argparse
import socket
import time
from datetime import datetime

class TestClient(object):
    def __init__(self, args):
        self.host = args.host
        self.port = args.port
        self.connect_timeout = args.connect_timeout
        self.connect_retry_interval = args.connect_retry_interval
        self.heartbeat_socket_timeout = args.heartbeat_socket_timeout
        self.heartbeat_interval = args.heartbeat_interval
        self.sock = None
        self.last_pong_time = None

    def log_event(self, text):
        print ('[%s] %s' % (datetime.now().strftime('%d-%b-%g %H:%M:%S.%f',), text))

    def connect(self):
        while True:
            try:
                addr = socket.gethostbyname(self.host)
                self.log_event('[I] Trying to connect %s (%s), port %s' % (self.host, addr, self.port))
                self.sock = socket.create_connection((addr, self.port), timeout=self.connect_timeout)
            except socket.error as err:
                self.log_event('[E] %s' % str(err))
            if self.sock is not None:
                self.log_event('[I] Connection established.')

                return
            time.sleep(self.connect_retry_interval)

    def heartbeat(self):
        self.sock.settimeout(self.heartbeat_socket_timeout)
        responses = 0
        while True:
            try:
                self.sock.send(b'*1\r\n$4\r\nPING\r\n')
                response = self.sock.recv(512)
                if not response:
                    self.log_event('[E] Server connection dropped')
                    break
                if response != b'+PONG\r\n':
                    self.log_event('[E] Unexpected protocol response: %s' % response)
                    break
            except socket.error as err:
                self.log_event('[E] %s' % str(err))
                break

            now = time.time()
            if responses == 0 and self.last_pong_time:
                self.log_event('[I] First successful response, %.2f after previous one' % (
                    now - self.last_pong_time))
            responses += 1
            self.last_pong_time = now
            time.sleep(self.heartbeat_interval)

    def run(self):
        while True:
            self.connect()
            self.heartbeat()
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except socket.error:
                pass
            self.sock = None

def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description='Redis failover test client')
    parser.add_argument(
        '--host', type=str, help='Server address',
        default='localhost')
    parser.add_argument(
        '--port', type=int, help='Server port',
        default=6379)
    parser.add_argument(
        '--connect-timeout', type=int, help='Timeout (secs) for individual connect attempts',
        default=5)
    parser.add_argument(
        '--connect-retry-interval', type=float, help='Connect (secs) retry interval time',
        default=0.5)
    parser.add_argument(
        '--heartbeat-socket-timeout', type=float, help='PING heartbeat socket timeout (secs)',
        default=3)
    parser.add_argument(
        '--heartbeat-interval', type=float, help='PING heartbeat interval time (secs)',
        default=1)
    args = parser.parse_args()

    TestClient(args).run()

if __name__ == '__main__':
    main()
