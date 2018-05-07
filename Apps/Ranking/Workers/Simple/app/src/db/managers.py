#!/usr/bin/env python
import logging;

logging.basicConfig(level=logging.DEBUG)
import os
import requests
from neo4j.v1 import GraphDatabase, basic_auth
from retry import retry

DB_GRAPH_USER, DB_GRAPH_PASS = os.environ["NEO4J_AUTH"].split('/')
RDB_GRAPH = os.environ["RDB_GRAPH"]


class ConnectionManager:

    def __init__(self):
        self.connect_graph()

    @retry(tries=5, delay=5)
    def connect_graph(self):
        logging.info("Connecting to Neo4j...")
        self.graph_driver = GraphDatabase.driver("bolt://%s:7687" % RDB_GRAPH,
                                                 auth=basic_auth(DB_GRAPH_USER, DB_GRAPH_PASS))
        logging.info("Connected to Neo4j")

    def get_latest_package(self, family):
        family_url_map = {
            "ethereum": "http://ethereum-monitoring.monitoring:9987/latestBlockNumber",
            "rinkeby": "http://rinkeby-monitoring.monitoring:9987/latestBlockNumber",
            "ropsten": "http://ropsten-monitoring.monitoring:9987/latestBlockNumber",
            "kovan": "http://kovan-monitoring.monitoring:9987/latestBlockNumber",
        }
        return requests.get(family_url_map[family]).json()["latestBlockNumber"]

    def close(self):
        pass

    def run_graph(self, query, params):
        with self.graph_driver.session() as session:
            result = session.read_transaction(self.run_graph_query, query, params)
        return result

    @staticmethod
    def run_graph_query(tx, query, params):
        return tx.run(query, params)
