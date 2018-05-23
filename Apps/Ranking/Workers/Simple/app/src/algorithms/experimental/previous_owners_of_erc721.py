"""
Previous owners of given erc721
===============================

Version: 0.1.0

Find all previous owners of given token_id within given erc721 contract address

`ranking/experimental_previous_owners_of_erc721;asset=ethereum:0x0601...a266d;id=545235 <http://localhost:8000/experimental_previous_owners_of_erc721;id=545235;asset=ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d>`_

"""

from algorithms.utils import materialize_records
from algorithms.utils import param

QUERY = """
MATCH (identity:Identity)<-[receiver:RECEIVER]-(t:Transfer{asset: {asset}})-[:IN]->(p:PackageEthereum)
WHERE t.amount = {token_id}
RETURN identity.id as id, p.sequence as sequence 
ORDER BY sequence DESC
"""


@param("id", required=True)
@param("asset", required=True)
def run(conn_mgr, input, **params):
    results = conn_mgr.run_graph(QUERY, {"token_id": params["id"], "asset": params["asset"]})
    return {"items": materialize_records(results)}
