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
        handlers, and keeping a sane list of connected client
    """
    logger = logging.getLogger(__name__)
    handlers_index = []
    handlers = {}

    drones_index = []
    drones = {}

    drone_handler_rel = {}

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

    def get_handlers_count(self):
        """
            returns the number of active connected handlers
        """
        return len(self.handlers_index)

    def get_drones_count(self):
        """
            returns the number of active connected drones
        """
        return len(self.drones_index)

    def remove_handler(self, hid):
        """
            remove a handler from the active handler list
        """
        self.handlers_index.remove(hid)
        self.handlers.pop(hid)

    def remove_drone(self, did):
        """
            remove a drone from the active drone list
        """
        self.drones_index.remove(did)
        self.drones.pop(did)
        self.drone_handler_rel.pop(did)

        self.logger.info("Drone with id={0} has dropped ({1} available handlers)".format(did, self.availible_handlers_count()))

    def expire_handler(self):
        """
            check if handlers are still connected or they have not seem for timeout
            time before removing them from active handler list
        """
        for handler in self.handlers_index:
            diff = (datetime.datetime.now() - self.handlers[handler]["time"]).seconds
            if diff >= self.timeout:
                self.remove_handler(handler)
                self.logger.debug("handler: {0}, expirerd".format(handler))
                self.logger.info("no heartbeat from id={0} received for 10 seconds, dropping ({1} available handlers)".format(handler, self.availible_handlers_count()))

    def expire_drone(self):
        """
            check if drones are still connected or they have not seem for timeout
            time before removing them from active drone list
        """
        for drone in self.drones_index:
            diff = (datetime.datetime.now() - self.drones[drone]["time"]).seconds
            if diff >= self.timeout:
                self.remove_drone(drone)
                self.logger.debug("Drone: {0}, expirerd".format(drone))
                self.logger.info("no heartbeat from id={0} received for 10 seconds, dropping".format(drone))


    def renew_handler(self, hid):
        """
            updates the time of last seem for for a given client
        """
        if hid in self.handlers_index:
            self.handlers[hid]["time"] = datetime.datetime.now()
            self.logger.debug("client: {0}, renewed".format(hid))

    def renew_drone(self, did):
        """
            updates the time of last seem for for a given drone
        """
        if did in self.drones_index:
            self.drones[did]["time"] = datetime.datetime.now()
            self.logger.debug("drone: {0}, renewed".format(did))

    def pong(self, toAddr):
        """
            response for ping requests and renew_handlers last seem time
        """
        self.renew_handler(toAddr)
        self.send_msg(b"pong", toAddr)

    def availible_handlers_count(self):
        return len(self.handlers_index) - len(self.drone_handler_rel)

    def register_drone(self, did):
        """
            register a drone and assign it to a free handler, returns Handler
            or false is no one is availible
        """

        if self.availible_handlers_count() >= 1:
            self.drones_index.append(did)
            self.drones[did] = {"time": datetime.datetime.now()}
            self.logger.info("Drone with id={0} connected".format(did))
            hid = self.handlers_index[len(self.handlers_index) - 1]
            self.drone_handler_rel[did] = hid
            self.logger.info("Drone with id={0} connected and assigned to handler {1} ({2} available handlers)".format(did, hid, self.availible_handlers_count()))
            self.send_msg(hid, did)
        else:
            self.send_msg(b"none_avail", did)

class HRouter(Base):
    """
        service to handle Handlers
    """

    id = b"HMANAGER"
    timeout = 10
    logger = logging.getLogger("Hanlder Manager")

    def run(self):
        self.setup()

        while True:
            self.expire_handler()
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
                            self.logger.info("handler with id={0} connected ({1} available handlers)".format(fromAddr, self.availible_handlers_count()))
                            self.send_msg(b"registered", fromAddr)

                    if msg == b"ping":
                        self.pong(fromAddr)

class DRouter(Base):

    """
        Service to handle drones
    """

    id = b"DMANAGER"
    timeout = 10
    logger = logging.getLogger("Drone Manager")

    def run(self):
        self.setup()

        while True:
            self.expire_drone()
            sockets = dict(self.poller.poll(100))
            if self.socket in sockets:
                if sockets[self.socket] == zmq.POLLIN:
                    fromAddr, empty, toAddr, empty, msg = self.recv_msg()

                    if msg == b"register":
                        if fromAddr in self.handlers:
                            self.send_msg(b"already_registered")
                        else:

                            self.register_drone(fromAddr)

                    if msg == b"ping":
                        self.renew_drone(fromAddr)
                        self.pong(fromAddr)

def check_arg(args=None):
    parser = argparse.ArgumentParser(description='Drone manager service')
    parser.add_argument('-H', '--listen_handlers',  help='handlers host:port', required='True', default='127.0.0.1:5000')
    parser.add_argument('-D', '--listen_drones',  help='Drones host:port', required='False', default='127.0.0.1:5001')

    results = parser.parse_args(args)
    return results.listen_handlers.split(":"), results.listen_drones.split(":"),


if __name__ == '__main__':

    handler_host, drones_host = check_arg(sys.argv[1:])

    hmanager = HRouter(handler_host[1], handler_host[0])
    dmanager = DRouter(drones_host[1], drones_host[0])
    hmanager.start()
    dmanager.start()
