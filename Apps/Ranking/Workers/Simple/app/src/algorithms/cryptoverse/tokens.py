"""
Tokens
======

Returns all erc721 tokens of given assets owned by given identity.

Version: 0.1.0

Example:

`ranking/cryptoverse_tokens;identity=0x157da...2bee3;asset=ethereum:0x06012...7a266d;asset=ethereum:0xf7a6e...abf643 <https://api.userfeeds.io/ranking/cryptoverse_tokens;identity=0x157da080cb7f3e091eadfa32bc7430d9f142bee3;asset=ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d;asset=ethereum:0xf7a6e15dfd5cdd9ef12711bd757a9b6021abf643>`_
"""

from algorithms.utils import normalize_to_list
from algorithms.utils import param

RECEIVED_TOKENS = """
MATCH (:Identity { id: {identity} })<-[:RECEIVER]-(t:Transfer)-[:IN]->(p)
WHERE t.asset IN {asset}
RETURN
    t.asset + ':' + t.amount AS context,
    max(p.sequence) AS sequence
ORDER BY sequence DESC
"""

SENT_TOKENS = """
MATCH (:Identity { id: {identity} })<-[:SENDER]-(t:Transfer)-[:IN]->(p)
WHERE t.asset IN {asset}
RETURN
    t.asset + ':' + t.amount AS context,
    max(p.sequence) AS sequence
"""


@param("identity", required=True)
@param("asset", required=True)
def run(conn_mgr, input, **params):
    normalized_params = {"identity": params["identity"], "asset": normalize_to_list(params["asset"])}
    received_tokens = conn_mgr.run_graph(RECEIVED_TOKENS, normalized_params)
    sent_tokens = conn_mgr.run_graph(SENT_TOKENS, normalized_params)
    results = filter_owned_tokens(received_tokens, sent_tokens)
    return {"items": results}


def filter_owned_tokens(received_tokens, sent_tokens):
    sent_tokens = {sent["context"]: sent["sequence"] for sent in sent_tokens}
    results = [{"context": recv["context"]} for recv in received_tokens if recv["sequence"] > sent_tokens.get(recv["context"], 0)]
    return results
