#!/usr/bin/env python
import logging.config; logging.config.fileConfig('/app/logging.conf')
from flask import Flask, request
from flask import jsonify
from flask_cors import CORS
from flows import path_to_flow, schema
from jsonschema import validate, ValidationError
from pprint import pformat
from werkzeug.routing import BaseConverter
import os
import worker
from db.managers import ConnectionManager


class RegexConverter(BaseConverter):
    def __init__(self, url_map, *items):
        super(RegexConverter, self).__init__(url_map)
        self.regex = items[0]


app = Flask(__name__)
app.config["JSONIFY_PRETTYPRINT_REGULAR"] = False
app.url_map.converters['regex'] = RegexConverter

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
    logging.info("Processing flow: {}".format(pformat(flow)))

    try:
        validate(flow, schema)
    except ValidationError as e:
        logging.info("Flow did not validate against schema")
        logging.exception(e)
        return jsonify(error=str(e)), 400

    try:
        conn_mgr = ConnectionManager()
        try:
            response = worker.run(conn_mgr, flow)
        finally:
            conn_mgr.close()

        if isinstance(response, str):
            logging.info("Response for flow {} is an error.".format(pformat(flow)))
            logging.error(response)
            return jsonify(error=response), 400

        logging.info("Sending response for flow: {}".format(pformat(flow)))
        return jsonify(response), 200
    except Exception as e:
        logging.info("Exception occurred while processing flow {}".format(pformat(flow)))
        logging.exception(e)
        return jsonify(error=str(e)), 400
