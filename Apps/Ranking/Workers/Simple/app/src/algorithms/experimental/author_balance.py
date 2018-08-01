"""
Author balance
==============

Version: 0.1.0

Adds balance of given author as score.

"""

from algorithms.utils import param, pipeable, filter_debug
from algorithms.experimental import author_balance_graph as graph
from algorithms.experimental import author_balance_rdb as rdb

@pipeable
@filter_debug
@param("asset", required=True)
def run(conn_mgr, input, **params):
    if params["asset"] in rdb.supported_assets:
        return rdb.run(conn_mgr, input, **params)
    else:
        return graph.run(conn_mgr, input, **params)
