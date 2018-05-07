#!/usr/bin/env python
import json
import logging
import logging.config; logging.config.fileConfig('/app/logging.conf')

import zmq
from algorithms import handlers as algorithms
from algorithms.utils import NotPipeableAlgorithm
from db.managers import ConnectionManager

logging.info("Ranking Worker - Start")

logging.info("Connecting to message queue...")

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.connect("tcp://queue:6761")

logging.info("Queue connection established")


def run(conn_mgr, request):
    logging.info("Start processing request {}".format(request))
    steps = []
    result = {}
    step_debug = {}

    for step_number, step in enumerate(request["flow"]):
        try:
            algorithm = step["algorithm"]
            params = step.get("params", {})
            try:
                algorithm_function = algorithms[algorithm]["run"]
            except KeyError as e:
                logging.exception(e)
                result = "InvalidAlgorithmError: `{}` does not exist".format(algorithm)
                return json.dumps(result).encode('utf8')

            algorithm_specification = getattr(algorithm_function, 'spec', {})

            if step_number > 0 and not algorithm_specification.get("pipeable"):
                raise NotPipeableAlgorithm("Algorithm '{}' needs to be used as primary algorithm".format(algorithm))

            step_debug = {
                "algorithm": algorithm,
                "params": params
            }
            result = algorithm_function(conn_mgr, result, **params)
            logging.debug(result)
        except Exception as e:
            logging.exception(e)
            result = str(e)
            return json.dumps(result).encode('utf8')
        finally:
            step_debug["out"] = json.loads(json.dumps(result))
            steps.append(step_debug)
    if request['debug']:
        result['debug'] = steps
    return json.dumps(result).encode('utf8')


def listen(conn_mgr):
    logging.info("Waiting for requests...")
    while True:
        request = json.loads(socket.recv().decode("utf8"))
        logging.info("Got request: {}".format(request))
        reply = run(conn_mgr, request)
        logging.info("Sending response...")
        socket.send(reply)
        logging.info("Response sent")


if __name__ == "__main__":
    conn_mgr = ConnectionManager()
    try:
        listen(conn_mgr)
    finally:
        conn_mgr.close()
