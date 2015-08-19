import zmq
import logging
import threading
import sys
import argparse
import time

class Handler(threading.Thread):
    manager = "MANAGER"
    def __init__(self, port="5000", bind_address="127.0.0.1", hid=1):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger('Handler_{0}'.format(hid))
        self.port = port
        self.bind_address = bind_address

        self.hid = "HANDLER{0}".format(hid)
        self.socket = None
        self.poller = None

    def setup(self):
        self.logger.debug('setup')
        self.socket = zmq.Context().socket(zmq.REQ)
        self.socket.setsockopt(zmq.IDENTITY, self.hid.encode('utf-8'))
        self.socket.connect("tcp://{0}:{1}".format(self.bind_address, self.port))
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)

    def send_msg(self, msg):
        self.socket.send_multipart([self.manager.encode('utf-8'), b"", msg.encode('utf-8')])

    def recv_msg(self):
        return self.socket.recv_multipart()

    def run(self):
        self.setup()
        self.send_msg("register")
        while True:
            sockets = dict(self.poller.poll(5000))
            if self.socket in sockets and sockets[self.socket] == zmq.POLLIN:
                msg = self.recv_msg()
                self.send_msg("ping")
                time.sleep(3)
def check_arg(args=None):
    parser = argparse.ArgumentParser(description='Drone handler service')
    parser.add_argument('-M', '--manager',  help='Manager host:port', required='True', default='127.0.0.1:5000')
    parser.add_argument('-I', '--id',  help='this handler id', required='True', default='1')

    results = parser.parse_args(args)
    return results.manager.split(":"), results.manager.split(":"), results.id


if __name__ == '__main__':

    manager_host, maneger_host, hid = check_arg(sys.argv[1:])

    handler = Handler(manager_host[1], maneger_host[0], hid)

    handler.start()
