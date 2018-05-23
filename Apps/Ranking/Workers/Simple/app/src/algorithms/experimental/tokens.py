"""
Tokens
======

Returns all erc721 tokens of given asset owned by given identity.

Version: 0.1.0

Example:

`ranking/experimental_tokens;identity=0x157da...2bee3;asset=ethereum:0x06012...7a266d <https://api.userfeeds.io/ranking/experimental_tokens;identity=0x157da080cb7f3e091eadfa32bc7430d9f142bee3;asset=ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d>`_
"""

from collections import defaultdict

from algorithms.utils import normalize_to_list
from algorithms.utils import param
from algorithms.utils import group_by

SEND_TOKENS = """
MATCH
	(identity:Identity { id: {identity} })<-[:SENDER]-(transfer:Transfer)-[:IN]->(package:Package)
WHERE transfer.asset in { asset }
RETURN
    transfer.asset as asset,
    transfer.amount as token,
    package.sequence as sequence
"""

RECEIVED_TOKEN = """
MATCH
	(identity:Identity { id: {identity} })<-[:RECEIVER]-(transfer:Transfer)-[:IN]->(package:Package)
WHERE transfer.asset in { asset }
RETURN
    transfer.asset as asset,
    transfer.amount as token,
    package.sequence as sequence
"""


def sort_by_sequence(query_result):
    return sorted(query_result, key=lambda x: x["sequence"])


@param("identity", required=True)
@param("asset", required=True)
def run(conn_mgr, input, **params):
    normalized_params = {"identity": params.get("identity"), "asset": normalize_to_list(params.get("asset"))}
    send_tokens = conn_mgr.run_graph(SEND_TOKENS, normalized_params)
    received_tokens = conn_mgr.run_graph(RECEIVED_TOKEN, normalized_params)
    results = filter_owned_tokens(received_tokens, send_tokens)
    return {"items": sort_by_sequence(results)}


def filter_owned_tokens(received_tokens, send_tokens):
    tokens_to_send_tokens = group_by(send_tokens, 'asset')
    tokens_to_received_tokens = group_by(received_tokens, 'asset')
    results = []
    for token in tokens_to_received_tokens:
        received_token = tokens_to_received_tokens.get(token, [])
        send_token = tokens_to_send_tokens.get(token, [])
        tokens_owned = filter_owned_token(received_token, send_token, token)
        results.extend(tokens_owned)
    return results


def filter_owned_token(received_tokens, send_tokens, asset):
    token_map = defaultdict(int)
    for received_token in received_tokens:
        token = received_token["token"]
        sequence = received_token["sequence"]
        token_map[token] = max(token_map[token], sequence)
    for send_token in send_tokens:
        token = send_token["token"]
        sequence = send_token["sequence"]
        if sequence > token_map[token]:
            del token_map[token]
    return [{"asset": asset, "token": token, "sequence": sequence, } for token, sequence in token_map.items()]

