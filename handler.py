import time
import logging
import zmq
import argparse
import sys

from manager import BaseManager

logging.basicConfig(level=logging.DEBUG, format='%(name)s: %(message)s')

class Handler(BaseManager):

    def __init__(self, port, bind_address, handler_id):
        BaseManager.__init__(self, port, bind_address)
        self.logger = logging.getLogger('Handler')
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

def check_arg(args=None):
    parser = argparse.ArgumentParser(description='Drone handler service')
    parser.add_argument('-M', '--manager_address',  help='Manager host:port', required='True', default='127.0.0.1:5000')

    results = parser.parse_args(args)
    return results.manager_address.split(":")

if __name__ == '__main__':

    manager_host = check_arg(sys.argv[1:])

    handler = Handler(5000, "127.0.0.1", 1)
    handler.setup()
    handler.register()

    while True:
        handler.ping()
