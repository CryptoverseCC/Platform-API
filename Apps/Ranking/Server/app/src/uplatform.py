import logging
import zmq

logging.basicConfig(level=logging.DEBUG)


def zmqconnect(host, port, protocol='tcp'):
    logging.info("Connecting to {host}:{port}...".format(host=host, port=port))
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("{protocol}://{host}:{port}".format(host=host, port=port, protocol=protocol))
    logging.info("Connection established")
    return socket
