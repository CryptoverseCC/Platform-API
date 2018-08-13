#!/usr/bin/env python
import logging;

logging.basicConfig(level=logging.DEBUG)
import os
import requests
from neo4j.v1 import GraphDatabase, basic_auth
from retry import retry
import psycopg2
from psycopg2.extras import DictCursor
from algorithms.utils import materialize_records

DB_GRAPH_USER, DB_GRAPH_PASS = os.environ["NEO4J_AUTH"].split('/')
NEO_HOST = os.environ["NEO4J_HOST"]
POSTGRES_HOST = os.environ["POSTGRES_HOST"]


class ConnectionManager:

    def __init__(self):
        self.connect_graph()
        self.connect_sql()

    @retry(tries=5, delay=5)
    def connect_graph(self):
        logging.info("Connecting to Neo4j...")
        self.graph_driver = GraphDatabase.driver("bolt://%s:7687" % NEO_HOST,
                                                 auth=basic_auth(DB_GRAPH_USER, DB_GRAPH_PASS))
        logging.info("Connected to Neo4j")

    @retry(tries=5, delay=5)
    def connect_sql(self):
        logging.info("Connecting to PostgreSQL...")
        self.sql_conn = psycopg2.connect(host=POSTGRES_HOST, dbname="postgres", user="postgres")
        logging.info("Connected to PostgreSQL")

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

    def run_rdb(self, query, params):
        try:
            with self.sql_conn.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(query, params)
                result = cursor.fetchall()
            return materialize_records(result)
        except psycopg2.Error as e:
            self.sql_conn.reset()
            raise e

    @staticmethod
    def run_graph_query(tx, query, params):
        return tx.run(query, params)
