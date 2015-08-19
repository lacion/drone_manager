import zmq
import logging
import threading
import argparse
import sys
import datetime

logging.basicConfig(level=logging.DEBUG, format='%(name)s: %(message)s')

class Base(threading.Thread):
    """
        Base manager class that provides basic functionality for registering
        clients, and keeping a sane list of connected client
    """
    logger = logging.getLogger(__name__)
    clients_index = []
    clients = {}

    def __init__(self, port="5000", bind_address="127.0.0.1"):
        threading.Thread.__init__(self)
        self.port = port
        self.bind_address = bind_address

        self.socket = None
        self.poller = None

    def setup(self):
        """
            sets the the socket, poller and identity to handle this manager
        """
        self.logger.debug('setup')
        self.socket = zmq.Context().socket(zmq.ROUTER)
        self.socket.setsockopt(zmq.IDENTITY, self.id)
        self.socket.bind("tcp://{0}:{1}".format(self.bind_address, self.port))
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)

    def recv_msg(self):
        """
            returns received multipart messages from the socket
        """
        return self.socket.recv_multipart()

    def send_msg(self, msg, toAddr):
        """
            Sends multipart message to a specific client
        """
        self.socket.send_multipart([toAddr, b"", msg])

    def get_clients_count(self):
        """
            returns the number of active connected clients
        """
        return len(self.clients_index)

    def remove_client(self, hid):
        """
            remove a client from the active client list
        """
        self.clients_index.remove(hid)
        self.clients.pop(hid)

    def expire(self):
        """
            check if clients are still connected or they have not seem for timeout
            time before removing them from active client list
        """
        for handler in self.clients_index:
            diff = (datetime.datetime.now() - self.clients[handler]["time"]).seconds
            if diff >= self.timeout:
                self.remove_client(handler)
                self.logger.debug("client: {0}, expired".format(handler))
                self.logger.info("no heartbeat from id={0} received for 10 seconds, dropping ({1} available clients)".format(handler, self.get_clients_count()))

    def renew(self, hid):
        """
            updates the time of last seem for for a given client
        """
        if hid in self.clients_index:
            self.clients[hid]["time"] = datetime.datetime.now()
            self.logger.debug("client: {0}, renewed".format(hid))

    def pong(self, toAddr):
        """
            response for ping requests and renews last seem time
        """
        self.renew(toAddr)
        self.send_msg(b"pong", toAddr)

class HRouter(Base):

    id = b"HMANAGER"
    timeout = 10

    def run(self):
        self.setup()

        while True:
            self.expire()
            sockets = dict(self.poller.poll(100))
            if self.socket in sockets:
                if sockets[self.socket] == zmq.POLLIN:
                    fromAddr, empty, toAddr, empty, msg = self.recv_msg()

                    if msg == b"register":
                        if fromAddr in self.clients:
                            self.send_msg(b"already_registered")
                        else:
                            self.clients_index.append(fromAddr)
                            self.clients[fromAddr] = {"time": datetime.datetime.now()}
                            self.logger.info("handler with id={0} connected ({1} available clients)".format(fromAddr, self.get_clients_count()))
                            self.send_msg(b"registered", fromAddr)

                    if msg == b"ping":
                        self.pong(fromAddr)

class DRouter(Base):

    id = b"DMANAGER"
    timeout = 10

    def run(self):
        self.setup()

        while True:
            self.expire()
            sockets = dict(self.poller.poll(100))
            if self.socket in sockets:
                if sockets[self.socket] == zmq.POLLIN:
                    fromAddr, empty, toAddr, empty, msg = self.recv_msg()

                    if msg == b"register":
                        if fromAddr in self.clients:
                            self.send_msg(b"already_registered")
                        else:
                            self.clients_index.append(fromAddr)
                            self.clients[fromAddr] = {"time": datetime.datetime.now()}
                            self.logger.info("handler with id={0} connected ({1} available clients)".format(fromAddr, self.get_clients_count()))
                            self.send_msg(b"registered", fromAddr)

                    if msg == b"ping":
                        self.renew(fromAddr)
                        self.pong(fromAddr)

def check_arg(args=None):
    parser = argparse.ArgumentParser(description='Drone manager service')
    parser.add_argument('-H', '--listen_clients',  help='clients host:port', required='True', default='127.0.0.1:5000')
    parser.add_argument('-D', '--listen_drones',  help='Drones host:port', required='False', default='127.0.0.1:5001')

    results = parser.parse_args(args)
    return results.listen_clients.split(":"), results.listen_drones.split(":")


if __name__ == '__main__':

    handler_host, drones_host = check_arg(sys.argv[1:])

    hmanager = HRouter(handler_host[1], handler_host[0])
    hmanager.start()
