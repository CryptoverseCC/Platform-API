"""
Author balance
==============

Version: 0.1.0

Adds balance of given author as score.

"""

from algorithms.utils import param, pipeable, filter_debug

VALUE_BALANCE = """
MATCH (t:Transfer { asset: {asset} })-[:RECEIVER]->(receiver)
WHERE receiver.id in {addresses}
RETURN t.amount as amount, receiver.id as address
UNION ALL
MATCH (t:Transfer { asset: {asset} })-[:SENDER]->(sender)
WHERE sender.id in {addresses}
RETURN '-' + t.amount as amount, sender.id as address
"""


@pipeable
@filter_debug
@param("asset", required=True)
def run(conn_mgr, input, **params):
    authors = list(set([item["author"] for item in input["items"]]))
    params["addresses"] = authors
    results = conn_mgr.run_graph(VALUE_BALANCE, params)
    scores = {}
    for r in results:
        if r["address"] not in scores:
            scores[r["address"]] = int(r["amount"])
        else:
            scores[r["address"]] += int(r["amount"])
    for i in input["items"]:
        if i["author"] in scores:
            i["score"] = scores[i["author"]]
    with_score_only = filter(lambda x: "score" in x, input["items"])
    sorted_by_score = sorted(with_score_only, key=lambda x: int(x["score"]), reverse=True)
    return {"items": sorted_by_score}
