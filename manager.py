import zmq
import time
import datetime
import logging
import argparse
import sys

from zmq.error import Again

logging.basicConfig(level=logging.DEBUG, format='%(name)s: %(message)s')

class BaseManager(object):

    def __init__(self, port="5000", bind_address="127.0.0.1"):
        self.logger = logging.getLogger('BaseManager')
        self.port = port
        self.bind_address = bind_address

        self.context = None
        self.socket = None

    def setup(self):

        self.socket = zmq.Context().socket(zmq.DEALER)
        self.socket.setsockopt(zmq.IDENTITY, b"MANAGER")
        self.socket.bind("tcp://{0}:{1}".format(self.bind_address, self.port))

class HandlerWorker(object):

    def __init__(self):
        self.logger = logging.getLogger('HandleWorker')
        self.handlers = []
        self.timeout = 10

    def setup(self):
        self.socket = zmq.Context().socket(zmq.DEALER)
        self.socket.setsockopt(zmq.IDENTITY, b"WORKER")
        self.socket.bind("tcp://{0}:{1}".format(self.bind_address, self.port))

    def read(self):
        return self.socket.recv_json(zmq.NOBLOCK)

    def send_msg(self, payload):
        self.socket.send_json(payload)

    def get_handlers_count(self):
        return len(self.handlers)

    def search_handlers_by_id(self, id):
        return next((item for item in self.handlers if item["id"] == id), False)

    def remove_handler(self, id):
        for i in reversed(range(len(self.handlers))):
            if self.handlers[i].get('id') == id:
                self.handlers.pop(i)

    def expire(self):
        for handler in self.handlers:
            diff = (datetime.datetime.now() - handler["time"]).seconds
            if diff >= self.timeout:
                self.remove_handler(handler["id"])
                self.logger.debug("handler: {0}, expired".format(handler["id"]))
                self.logger.info("no heartbeat from hander id={0} received for 10 seconds, dropping ({1} available handler)".format(handler["id"], self.get_handlers_count()))

    def renew(self, id):
        handler = self.search_handlers_by_id(id)

        if handler:
            handler["time"] = datetime.datetime.now()
            self.logger.debug("handler: {0}, renewed".format(handler["id"]))

    def pong(self):
        self.send_msg({b"reply": True})

    def handle(self):
        self.expire()
        try:
            message = self.read()
            if message["msg_type"] == "register":
                if message["id"] in self.handlers:
                    self.send_msg({b"reply": False})
                else:
                    self.handlers.append({"id": message["id"], "time": datetime.datetime.now()})
                    self.logger.info("handler with id={0} connected ({1} available handlers)".format(message["id"], self.get_handlers_count()))
                    self.send_msg({b"reply": True})

            if message["msg_type"] == "ping":
                self.renew(message["id"])
                self.pong()
        except Again:
            self.logger.debug("move along, nothing to see here.")


class DroneManager(BaseManager):

    def __init__(self, port, bind_address):

        BaseManager.__init__(self, port, bind_address)

        self.drones = []

    def handle(self):
        pass


def check_arg(args=None):
    parser = argparse.ArgumentParser(description='Drone manager service')
    parser.add_argument('-H', '--listen_handlers',  help='Handlers host:port', required='True', default='127.0.0.1:5000')
    parser.add_argument('-D', '--listen_drones',  help='Drones host:port', required='False', default='127.0.0.1:5001')

    results = parser.parse_args(args)
    return results.listen_handlers.split(":"), results.listen_drones.split(":")


if __name__ == '__main__':

    handler_host, drones_host = check_arg(sys.argv[1:])

    handler = HandlerManager(handler_host[1], handler_host[0])
    handler.setup()
    handler.logger.info("manager started ({0} available handlers)".format(handler.get_handlers_count()))

    while True:
        handler.handle()
        time.sleep(1)
