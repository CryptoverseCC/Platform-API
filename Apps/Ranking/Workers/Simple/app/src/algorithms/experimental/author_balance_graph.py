"""
Author balance
==============

Version: 0.1.0

Adds balance of given author as score.

"""

from algorithms.utils import param, pipeable, filter_debug
from collections import defaultdict

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
    authors = find_all_authors(input["items"])
    params["addresses"] = list(authors)
    results = conn_mgr.run_graph(VALUE_BALANCE, params)
    scores = defaultdict(int)
    for r in results:
        scores[r["address"]] += int(float(r["amount"]))
    for i in input["items"]:
        if i["author"] in scores:
            i["score"] = scores[i["author"]]
    with_score_only = [x for x in input["items"] if x.get("score", 0) > 0]
    sorted_by_score = sorted(with_score_only, key=lambda x: x["score"], reverse=True)
    filter_likes_replies(sorted_by_score, scores)
    return {"items": sorted_by_score}


def find_all_authors(items):
    authors = set()
    authors.update([i["author"] for i in items])
    for i in items:
        if i.get("likes"):
            authors.update(find_all_authors(i["likes"]))
        if i.get("replies"):
            authors.update(find_all_authors(i["replies"]))
    return authors


def filter_likes_replies(items, scores):
    for i in items:
        if i.get("likes"):
            for like in i["likes"]:
                if like["author"] in scores:
                    like["score"] = scores[like["author"]]
            i["likes"] = [x for x in i["likes"] if x.get("score", 0) > 0]
        if i.get("replies"):
            for reply in i["replies"]:
                if reply["author"] in scores:
                    reply["score"] = scores[reply["author"]]
            i["replies"] = [x for x in i["replies"] if x.get("score", 0) > 0]
            filter_likes_replies(i["replies"], scores)
