import zmq
import logging
import threading
import argparse
import sys
import datetime

logging.basicConfig(level=logging.DEBUG, format='%(name)s: %(message)s')

class HRouter(threading.Thread):

    id = b"MANAGER"
    handlers_index = []
    handlers = {}
    timeout = 10

    def __init__(self, port="5000", bind_address="127.0.0.1"):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger('MANAGER')
        self.port = port
        self.bind_address = bind_address

        self.socket = None
        self.poller = None

    def setup(self):
        self.logger.debug('setup')
        self.socket = zmq.Context().socket(zmq.ROUTER)
        self.socket.setsockopt(zmq.IDENTITY, self.id)
        self.socket.bind("tcp://{0}:{1}".format(self.bind_address, self.port))
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)

    def get_handlers_count(self):
        return len(self.handlers_index)

    def remove_handler(self, hid):
        self.handlers_index.remove(hid)
        self.handlers.pop(hid)

    def expire(self):
        for handler in self.handlers_index:
            diff = (datetime.datetime.now() - self.handlers[handler]["time"]).seconds
            if diff >= self.timeout:
                self.remove_handler(handler)
                self.logger.debug("handler: {0}, expired".format(handler))
                self.logger.info("no heartbeat from hander id={0} received for 10 seconds, dropping ({1} available handler)".format(handler, self.get_handlers_count()))

    def renew(self, hid):
        if hid in self.handlers_index:
            self.handlers[hid]["time"] = datetime.datetime.now()
            self.logger.debug("handler: {0}, renewed".format(hid))

    def pong(self, toAddr):
        self.send_msg(b"pong", toAddr)

    def recv_msg(self):
        return self.socket.recv_multipart() #fromAddr, toAddr, msg

    def send_msg(self, msg, toAddr):
        self.socket.send_multipart([toAddr, b"", msg])

    def is_registered(self, hid):
        if hid in self.handlers_index:
            return True
        else:
            return False

    def run(self):
        self.setup()

        while True:
            self.expire()
            sockets = dict(self.poller.poll(100))
            if self.socket in sockets:
                if sockets[self.socket] == zmq.POLLIN:
                    fromAddr, empty, toAddr, empty, msg = self.recv_msg()

                    if msg == b"register":
                        if fromAddr in self.handlers:
                            self.send_msg(b"already_registered")
                        else:
                            self.handlers_index.append(fromAddr)
                            self.handlers[fromAddr] = {"time": datetime.datetime.now()}
                            self.logger.info("handler with id={0} connected ({1} available handlers)".format(fromAddr, self.get_handlers_count()))
                            self.send_msg(b"registered", fromAddr)

                    if msg == b"ping":
                        self.renew(fromAddr)
                        self.pong(fromAddr)

class DRouter(threading.Thread):

    def __init__(self, port="5000", bind_address="127.0.0.1"):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger('ROUTER')
        self.port = port
        self.bind_address = bind_address

        self.context = None
        self.socket = None
        self.poller = None

    def setup(self):
        self.logger.debug('setup')
        self.socket = zmq.Context().socket(zmq.ROUTER)
        self.socket.setsockopt(zmq.IDENTITY, b"DROUTER")
        self.socket.bind("tcp://{0}:{1}".format(self.bind_address, self.port))
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)

    def run(self):
        self.setup()

def check_arg(args=None):
    parser = argparse.ArgumentParser(description='Drone manager service')
    parser.add_argument('-H', '--listen_handlers',  help='Handlers host:port', required='True', default='127.0.0.1:5000')
    parser.add_argument('-D', '--listen_drones',  help='Drones host:port', required='False', default='127.0.0.1:5001')

    results = parser.parse_args(args)
    return results.listen_handlers.split(":"), results.listen_drones.split(":")


if __name__ == '__main__':

    handler_host, drones_host = check_arg(sys.argv[1:])

    hmanager = HRouter(handler_host[1], handler_host[0])
    hmanager.start()
