#!/usr/bin/env python
import logging.config; logging.config.fileConfig('/app/logging.conf')
from flask import Flask, request
from flask import jsonify
from flask_cors import CORS
from flows import path_to_flow, schema
from jsonschema import validate, ValidationError
import os
import worker
from db.managers import ConnectionManager


app = Flask(__name__)
app.config["JSONIFY_PRETTYPRINT_REGULAR"] = False
conn_mgr = ConnectionManager()

if os.environ.get('CORS_ALLOW_ALL', False):
    CORS(app)


@app.route('/<path:path>', methods=['GET'])
def get(path):
    params = request.args.to_dict(flat=True)
    flow = path_to_flow(path)
    flow['debug'] = 'debug' in params
    return run(flow)


@app.route('/', methods=['POST'])
def post():
    params = request.args.to_dict(flat=True)
    flow = request.get_json()
    flow['debug'] = 'debug' in params
    return run(flow)


def run(flow):
    logging.info("Processing flow: {}".format(flow))

    try:
        validate(flow, schema)
    except ValidationError as e:
        logging.info("Flow did not validate against schema")
        logging.exception(e)
        return jsonify(error=str(e)), 400

    try:
        response = worker.run(conn_mgr, flow)

        if isinstance(response, str):
            logging.info("Response for flow {} is an error.".format(flow))
            logging.error(response)
            return jsonify(error=response), 400

        logging.info("Sending response for flow: {}".format(flow))
        return jsonify(response), 200
    except Exception as e:
        logging.info("Exception occurred while processing flow {}".format(flow))
        logging.exception(e)
        return jsonify(error=str(e)), 400
