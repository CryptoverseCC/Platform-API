"""
Filter by labels
===================

It filters claims by those labeling target with given labels.
Also adds info about those (filtered) labels to each claim.

Version: 0.1.0

Examples:

`ranking/experimental_context_feed;id=ethereum:0x06012...a266d:341605/experimental_filter_labels;id=like <https://api.userfeeds.io/ranking/experimental_context_feed;id=ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d:341605/experimental_filter_labels;id=like>`_
`ranking/experimental_context_feed;id=ethereum:0x06012...a266d:341605/experimental_filter_labels;id=follow;id=favourite <https://api.userfeeds.io/ranking/experimental_context_feed;id=ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d:341605/experimental_filter_labels;id=follow;id=favourite>`_
"""

from algorithms.utils import pipeable, filter_debug


LABELS_FOR_CLAIMS = """
MATCH (claim:Entity:Claim)-[labels:LABELS]->()
WHERE claim.id IN {ids} AND labels.value IN {labels}
RETURN claim.id AS id, collect(labels.value) AS labels
"""


@pipeable
@filter_debug
def run(conn_mgr, input, id, **ignore):
    ids = collect_ids(input)
    if isinstance(id, list):
        labels = id
    else:
        labels = [id]
    result = conn_mgr.run_graph(LABELS_FOR_CLAIMS, {"ids": ids, "labels": labels})
    id_labels_map = parse_response(result)
    return {
        "items": filter_valid_claim(input, id_labels_map)
    }


def collect_ids(input):
    return [item["id"] for item in input["items"]]


def parse_response(result):
    return {record["id"]: record["labels"] for record in result}


def filter_valid_claim(input, id_labels_map):
    result = []
    for item in input["items"]:
        labels = id_labels_map.get(item["id"])
        if labels:
            item["labels"] = labels
            result.append(item)
    return result
