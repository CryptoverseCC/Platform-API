"""
Traded assets
=============

Version: 0.1.0

Shows assets which were sent or received by given address at any time.
Displays first/last block sequence together with volume of traded asset.
"""

from algorithms.utils import materialize_records
from algorithms.utils import param

QUERY = """
MATCH (id:Identity{id: {address} })<-[]-(t:Transfer)-[:IN]->(p:Package)
RETURN DISTINCT 
    t.asset as asset,
    min(p.sequence) as min_sequence, 
    max(p.sequence) as max_sequence, 
    sum(toFloat(t.amount)) as volume
"""


@param("address", required=True)
def run(conn_mgr, input, **params):
    results = conn_mgr.run_graph(QUERY, params)
    return {"items": materialize_records(results)}
