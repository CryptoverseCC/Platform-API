"""
Author balance
==============

Version: 0.1.0

Adds balance of given author as score.

"""

from algorithms.utils import param, pipeable, filter_debug
from collections import defaultdict

VALUE_BALANCE = """
SELECT received.address as address, received.amount - COALESCE(send.amount, 0) as amount 
              FROM (SELECT receiver as address, sum(cast(amount as decimal)) as amount from persistent_transfer where asset = %(_asset)s and receiver in (SELECT * FROM UNNEST(%(addresses)s)) group by receiver) as received
    left outer join (SELECT senders as address, sum(cast(amount as decimal)) as amount from persistent_transfer where asset = %(_asset)s and senders in (SELECT * FROM UNNEST(%(addresses)s)) group by senders) as send
    on received.address = send.address;
"""

supported_assets = [
    "ethereum:0x0d8775f648430679a709e98d2b0cb6250d2887ef", #Basic Attention Token
    "ethereum:0xd26114cd6ee289accf82350c8d8487fedb8a0c07", #Omise Go
    "ethereum:0xa74476443119a942de498590fe1f2454d7d4ac0d", #Golem
    "ethereum:0x744d70fdbe2ba4cf95131626614a1763df805b9e", #Status
    "ethereum:0xe41d2489571d322189246dafa5ebde1f4699f498", #ZRX
]

@pipeable
@filter_debug
@param("asset", required=True)
def run(conn_mgr, input, **params):
    authors = find_all_authors(input["items"])
    if not authors:
        return {"items": [] }
    results = conn_mgr.run_rdb(VALUE_BALANCE, {"_asset": params["asset"], "addresses": list(authors)})
    scores = defaultdict(int)
    for r in results:
        scores[r["address"]] += int(r["amount"])
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
