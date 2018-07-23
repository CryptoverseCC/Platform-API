"""
Algorithm for magical scoring of feed
=====================================

Version: 0.1.0

"""

from time import time
from algorithms.utils import param, pipeable, filter_debug
from collections import defaultdict

AUTHOR_SCORE = """
UNWIND {addresses} AS address
MATCH (:Identity { id: address })<-[:RECEIVER]-(t)
WHERE t.asset IN {asset}
RETURN address, count(DISTINCT t.asset) as score
"""


@pipeable
@filter_debug
@param("asset", required=True)
def run(conn_mgr, input, **params):
    authors = find_all_authors(input["items"])
    params["addresses"] = list(authors)
    results = conn_mgr.run_graph(AUTHOR_SCORE, params)
    scores = defaultdict(int)
    for r in results:
        scores[r["address"]] = r["score"]
    for i in input["items"]:
        calc_score(i, scores)
    sorted_by_score = sorted(input["items"], key=lambda x: x["score"], reverse=True)
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


def calc_score(item, scores):
    authors = find_all_authors([item])
    item["score_abs"] = sum([scores[x] for x in authors])
    item["score"] = item["score_abs"] * 100000 / (1 + time() - item["created_at"] / 1000)
