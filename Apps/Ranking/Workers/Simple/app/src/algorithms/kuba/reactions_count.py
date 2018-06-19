"""
Reactions count
===============

Version: 0.1.0

"""

from algorithms.utils import pipeable, filter_debug

REACTIONS_COUNT = """
UNWIND {ids} as id
MATCH (claim:Claim { id: id })
OPTIONAL MATCH (claim)<-[:TARGET]-(reaction:Claim)
RETURN id, count(reaction) as count
"""


@pipeable
@filter_debug
def run(conn_mgr, input, **ignore):
    root_messages = input["items"]
    ids = [message["id"] for message in root_messages] + [reply["id"] for message in root_messages for reply in message["replies"]]
    reactions = conn_mgr.run_graph(REACTIONS_COUNT, {"ids": ids})
    reactions = {reaction["id"]: reaction["count"] for reaction in reactions}
    for message in root_messages:
        message["likes_count"] = reactions[message["id"]]
        for reply in message["replies"]:
            reply["likes_count"] = reactions[reply["id"]]
    return input
