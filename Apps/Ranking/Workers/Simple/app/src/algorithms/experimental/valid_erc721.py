"""
Valid erc721
===================

It filters claims authored by an owner of context.
It parses claim context to get collectible id and erc721 contract address.
Then it verifies if claim author was an owner of this collectible at the time of creating claim.

Version: 0.1.0

Example:

`ranking/experimental_context_feed;id=ethereum:0x06012...a266d:341605/experimental_valid_erc721 <https://api.userfeeds.io/ranking/experimental_context_feed;id=ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d:341605/experimental_valid_erc721>`_
"""

from algorithms.utils import pipeable, filter_debug


VALIDATE_QUERY = """
MATCH (claim:Entity:Claim)-[:CONTEXT]->(context:Entity)
WHERE claim.id IN {ids} and io.userfeeds.erc721.isValidClaim(claim)
RETURN
    claim.id as id,
    context.id as context
"""


@pipeable
@filter_debug
def run(conn_mgr, input, **params):
    ids = collect_ids(input)
    result = conn_mgr.run_graph(VALIDATE_QUERY, {"ids": ids})
    id_context_map = parse_response(result)
    return {
        "items": filter_valid_claim(input, id_context_map)
    }


def collect_ids(input):
    return [item["id"] for item in input["items"]]


def parse_response(result):
    return {record["id"]: record["context"] for record in result}


def filter_valid_claim(input, id_context_map):
    result = []
    for item in input["items"]:
        context = id_context_map.get(item["id"])
        if context:
            item["context"] = context
            result.append(item)
    return result
