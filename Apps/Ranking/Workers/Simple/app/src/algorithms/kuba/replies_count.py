"""
Replies count
=============

Version: 0.1.0

"""

from algorithms.utils import pipeable, filter_debug

REPLIES_COUNT = """
UNWIND {ids} as id
MATCH (claim:Claim { id: id })
OPTIONAL MATCH (claim)<-[:ABOUT]-(reply:Claim)
RETURN id, count(reply) as count
"""


@pipeable
@filter_debug
def run(conn_mgr, input, **ignore):
    root_messages = input["items"]
    ids = [message["id"] for message in root_messages]
    replies = conn_mgr.run_graph(REPLIES_COUNT, {"ids": ids})
    replies = {reply["id"]: reply["count"] for reply in replies}
    for message in root_messages:
        message["replies_count"] = replies[message["id"]]
    return input
