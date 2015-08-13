import zmq
import time
import logging

logging.basicConfig(level=logging.DEBUG, format='%(name)s: %(message)s')

class BaseManager(object):

    def __init__(self, port="5000", bind_address="127.0.0.1"):
        self.logger = logging.getLogger('HandlerManager')
        self.logger.debug('__init__')
        self.port = port
        self.bind_address = bind_address

        self.context = None
        self.socket = None

    def setup(self):
        self.logger.debug('setup')
        self.socket = zmq.Context().socket(zmq.REP)
        self.socket.bind("tcp://{0}:{1}".format(self.bind_address, self.port))

    def read(self):
        return self.socket.recv_json()

    def send_msg(self, payload):
        self.socket.send_json(payload)

class HandlerManager(BaseManager):

    def __init__(self, port, bind_address):

        BaseManager.__init__(self, port, bind_address)

        self.managers = []

    def pong(self):
        self.send_msg({"reply": True})

    def handle(self):
        message = self.read()

        if message["msg_type"] == "register":
            if message["id"] in self.managers:
                self.send_msg({"reply": False})
            else:
                self.managers.append(message["id"])
                self.send_msg({"reply": True})

        if message["msg_type"] == "ping":
            self.pong()


class DroneManager(BaseManager):

    def __init__(self, port, bind_address):

        BaseManager.__init__(self, port, bind_address)

        self.drones = []

    def handle(self):
        pass

if __name__ == '__main__':

    handler = HandlerManager(5000, "127.0.0.1")
    handler.setup()

    while True:
        handler.handle()
        time.sleep(1)
