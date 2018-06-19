"""
Reactions
=========

Version: 0.1.0

"""

from algorithms.utils import pipeable, filter_debug

REACTIONS = """
UNWIND {ids} as id
MATCH (claim:Claim { id: id })
OPTIONAL MATCH
    (claim)<-[:ABOUT]-(replyClaim:Claim),
    (replyClaim)<-[:AUTHORED]-(replyAuthor:Identity),
    (replyClaim)-[:IN]->(replyPackage:Package)
WITH id, replyClaim, replyAuthor, replyPackage,
    CASE replyClaim
        WHEN null THEN false
        ELSE io.userfeeds.erc721.isValidClaim(replyClaim) END AS erc721ValidClaim
OPTIONAL MATCH
    (replyClaim)-[:CONTEXT]->(replyContext:Entity)
WHERE erc721ValidClaim
RETURN
    id,
    collect(replyClaim.id) as reply_id,
    collect(replyAuthor.id) AS reply_author,
    collect(replyPackage.family) AS reply_family,
    collect(replyPackage.sequence) AS reply_sequence,
    collect(replyPackage.timestamp) AS reply_created_at,
    collect(erc721ValidClaim) AS reply_context_exists,
    collect(replyContext.id) AS reply_context
"""


@pipeable
@filter_debug
def run(conn_mgr, input, **ignore):
    root_messages = input["items"]
    ids = [message["id"] for message in root_messages]
    replies = conn_mgr.run_graph(REACTIONS, {"ids": ids})
    replies = {r["id"]: create_reply_list(r) for r in replies}
    add_replies(root_messages, replies)
    return input


def create_reply_list(r):
    reply_contexts = iter(r["reply_context"])
    return [create_reply(id, author, family, sequence, created_at, next(reply_contexts) if context_exists else None)
            for id, author, family, sequence, created_at, context_exists in zip_reply_info(r)]


def zip_reply_info(r):
    return zip(
        r["reply_id"],
        r["reply_author"],
        r["reply_family"],
        r["reply_sequence"],
        r["reply_created_at"],
        r["reply_context_exists"])


def create_reply(id, author, family, sequence, created_at, context):
    return {
        "id": id,
        "author": author,
        "family": family,
        "sequence": sequence,
        "created_at": created_at,
        "context": context
    }


def add_replies(root_messages, replies):
    for message in root_messages:
        message["replies"] = replies[message["id"]]
