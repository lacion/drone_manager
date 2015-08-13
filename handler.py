import time
import logging
import zmq

from manager import BaseManager

logging.basicConfig(level=logging.DEBUG, format='%(name)s: %(message)s')

class Handler(BaseManager):

    def __init__(self, port, bind_address, handler_id):
        BaseManager.__init__(self, port, bind_address)
        self.id = handler_id

    def setup(self):
        self.logger.debug('setup')
        self.socket = zmq.Context().socket(zmq.REQ)
        self.socket.connect("tcp://{0}:{1}".format(self.bind_address, self.port))

    def register(self):
        payload = {
            "id": self.id,
            "msg_type": "register"
        }
        self.send_msg(payload)
        time.sleep(1)
        response = self.read()
        if response["reply"]:
            self.logger.debug("registered")

    def ping(self):
        payload = {
            "id": self.id,
            "msg_type": "ping"
        }
        self.send_msg(payload)
        time.sleep(3)
        response = self.read()
        if response["reply"]:
            self.logger.debug("received pong from manager")


if __name__ == '__main__':

    handler = Handler(5000, "127.0.0.1", 1)
    handler.setup()
    handler.register()

    while True:
        handler.ping()
