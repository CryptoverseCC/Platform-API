"""
Hodl
====

Returns hodl for identity where hodl is amount of token possessed multiplied by how long (for how many blocks) it was possessed.

In example if you hodl 1 eth for 10 blocks you have 10ethblocks of hodl.

If you hodl 0.1 eth for 100 blocks you also have 10ethblocks of hodl.

Version: 0.1.0

Example:

`ranking/experimental_hodl;identity=0x223edbc8166ba1b514729261ff53fb8c73ab4d79 <https://api.userfeeds.io/ranking/experimental_hodl;identity=0x223edbc8166ba1b514729261ff53fb8c73ab4d79>`_
"""

from algorithms.utils import param
from collections import defaultdict

# TODO: take into account gas fees
QUERY_ALL = """
MATCH (identity:Entity:Identity { id: {identity} })
MATCH (t:Entity:Transfer)-[:RECEIVER]->(identity)
MATCH (t)-[:IN]->(p:Entity:Package)
RETURN t.amount AS amount, p.sequence AS sequence, t.asset as asset
UNION ALL
MATCH (identity:Entity:Identity { id: {identity} })
MATCH (t:Entity:Transfer)-[:SENDER]->(identity)
MATCH (t)-[:IN]->(p:Entity:Package)
RETURN '-' + t.amount AS amount, p.sequence AS sequence, t.asset as asset
"""

QUERY_ASSET = """
MATCH (identity:Entity:Identity { id: {identity} })
MATCH (t:Entity:Transfer { asset: {asset} })-[:RECEIVER]->(identity)
MATCH (t)-[:IN]->(p:Entity:Package)
RETURN t.amount AS amount, p.sequence AS sequence, t.asset as asset
UNION ALL
MATCH (identity:Entity:Identity { id: {identity} })
MATCH (t:Entity:Transfer { asset: {asset} })-[:SENDER]->(identity)
MATCH (t)-[:IN]->(p:Entity:Package)
RETURN '-' + t.amount AS amount, p.sequence AS sequence, t.asset as asset
"""


@param("identity", required=True)
@param("asset", required=False)
def run(conn_mgr, input, **params):
    if 'asset' in params:
        results = conn_mgr.run_graph(QUERY_ASSET, params)
    else:
        results = conn_mgr.run_graph(QUERY_ALL, params)

    results = list(results)

    families = set([t["asset"].split(':')[0] for t in results])
    max_packages = get_max_packages(conn_mgr, families)

    items = calculate_hodl(max_packages, results)
    items = sorted(items, key=lambda x: x['value'])
    return {"items": items}


def get_max_packages(conn_mgr, families):
    packages = {}
    for family in families:
        packages[family] = conn_mgr.get_latest_package(family)
    return packages


def calculate_hodl(max_packages, transfers):
    hodl = defaultdict(int)
    processedAmount = defaultdict(int)
    processedSequence = defaultdict(int)
    for transfer in transfers:
        asset = transfer["asset"]
        hodl[asset] += processedAmount[asset] * (transfer["sequence"] - processedSequence[asset])
        processedAmount[asset] += int(transfer["amount"])
        processedSequence[asset] = transfer["sequence"]

    # Add HODL up until latest package
    for asset, value in hodl.items():
        family = asset.split(':')[0]
        hodl[asset] += processedAmount[asset] * (max_packages[family] - processedSequence[asset])

    return [
        {'asset': asset, 'value': value}
        for asset, value in hodl.items()
    ]
