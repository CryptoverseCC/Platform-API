"""
Author balance
==============

Version: 0.1.0

Adds balance of given author as score.

"""

from algorithms.utils import param, pipeable, filter_debug
from algorithms.experimental import author_balance_graph as graph
from algorithms.experimental import author_balance_rdb as rdb

IS_SUPPORTED = """
SELECT count(*) = 2 as is_supported FROM pg_indexes as a
                JOIN pg_class as b on a.indexname = b.relname
                JOIN pg_index as c on b.oid = c.indexrelid
where a.indexdef like %(_asset)s
and c.indisvalid = true;
"""

@pipeable
@filter_debug
@param("asset", required=True)
def run(conn_mgr, input, **params):
    result = conn_mgr.run_rdb(IS_SUPPORTED, { "_asset": "%"+params["asset"]+"%" } )
    is_supported = result[0]["is_supported"]
    if is_supported:
        return rdb.run(conn_mgr, input, **params)
    else:
        return graph.run(conn_mgr, input, **params)
