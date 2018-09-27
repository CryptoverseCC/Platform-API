"""
Cryptoverse thread ads
======================

"""

from algorithms.utils import param

ADS_QUERY = """
MATCH
    (claim:Claim)-[:ABOUT]->(:Claim { id: {id} }),
    (claim)-[:TARGET]->(target),
    (claim)-[:TYPE]->(:Entity { id: 'ad' }),
    (claim)<-[:AUTHORED]-(author),
    (claim)-[:IN]->(package),
    (claim)-[:CONNECTED_WITH]->(sent)-[:RECEIVER]->(contract:Identity { id: '0x53b7c52090750c30a40babd50024588e527292c3' })
OPTIONAL MATCH (claim)-[:CONNECTED_WITH]->(returned)-[:RECEIVER]->(forwarder { id: '0xfcd0b4035f0d4f97d171a28d8256842fedfdcdeb' }), (returned)-[:SENDER]->(contract)
RETURN
    claim.id AS id,
    target.id AS target,
    author.id AS author,
    package.sequence AS sequence,
    package.family AS family,
    package.timestamp AS created_at,
    sent.amount AS sent_amount,
    returned.amount AS returned_amount
"""

BOOST_QUERY = """
MATCH
    (claim:Claim)-[:TARGET]->(ad)-[:ABOUT]->(:Claim { id: {id} }),
    (claim)-[:TYPE]->(:Entity { id: 'boost' }),
    (claim)-[:CONNECTED_WITH]->(sent)-[:RECEIVER]->(contract:Identity { id: '0x53b7c52090750c30a40babd50024588e527292c3' })
OPTIONAL MATCH (claim)-[:CONNECTED_WITH]->(returned)-[:RECEIVER]->(forwarder { id: '0xfcd0b4035f0d4f97d171a28d8256842fedfdcdeb' }), (returned)-[:SENDER]->(contract)
RETURN
    ad.id AS id,
    sent.amount AS sent_amount,
    returned.amount AS returned_amount
"""


@param("id", required=True)
def run(conn_mgr, input, **params):
    ads = conn_mgr.run_graph(ADS_QUERY, params)
    ads = {item["id"]: convert(item) for item in ads}
    boosts = conn_mgr.run_graph(BOOST_QUERY, params)
    for b in boosts:
        ads[b["id"]]["score"] += calc_score(b)
    ads = sorted(ads.values(), key=lambda item: item["score"], reverse=True)
    return {"items": ads}


def convert(item):
    return {
        "id": item["id"],
        "target": item["target"],
        "author": item["author"],
        "family": item["family"],
        "sequence": item["sequence"],
        "created_at": item["created_at"],
        "score": calc_score(item)
    }

def calc_score(item):
    score = int(float(item["sent_amount"]))
    if item["returned_amount"]:
        score -= int(float(item["returned_amount"]))
    return score
