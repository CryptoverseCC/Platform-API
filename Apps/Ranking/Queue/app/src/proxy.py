import logging
import logging.config; logging.config.fileConfig('/app/logging.conf')
import zmq

def main():

    context = zmq.Context()

    frontend = context.socket(zmq.ROUTER)
    frontend.bind("tcp://*:6760")

    backend  = context.socket(zmq.DEALER)
    backend.bind("tcp://*:6761")

    logging.info("Ranking Queue Ready")

    zmq.proxy(frontend, backend)

    # We never get here…
    frontend.close()
    backend.close()
    context.term()

if __name__ == "__main__":
    main()
