"""
Transfers
=========

Version: 0.1.0

"""

from algorithms.utils import param


TRANSFERS_WITH_SEQUENCE = """
MATCH (identity:Entity:Identity { id: {context} })
MATCH (t:Entity:Transfer { asset: {asset} })-[:RECEIVER]->(identity)
MATCH (t)-[:IN]->(p:Entity:Package)
RETURN t.amount AS amount, p.sequence AS sequence
UNION ALL
MATCH (identity:Entity:Identity { id: {context} })
MATCH (t:Entity:Transfer { asset: {asset} })-[:SENDER]->(identity)
MATCH (t)-[:IN]->(p:Entity:Package)
RETURN '-' + t.amount AS amount, p.sequence AS sequence
"""


@param("context", required=True)
@param("asset", required=True)
def run(conn_mgr, input, **params):
    results = conn_mgr.run_graph(TRANSFERS_WITH_SEQUENCE, params)
    transfers = [{
        "amount": int(i["amount"]),
        "sequence": i["sequence"]
    } for i in results]

    return {"items": transfers}
