"""
Reactions
=========

Version: 0.1.0

"""

from algorithms.utils import pipeable, filter_debug, param

REPLIES = """
UNWIND {ids} as id
MATCH (claim:Claim { id: id })
OPTIONAL MATCH
    (claim)<-[:ABOUT]-(replyClaim),
    (replyClaim)-[:TARGET]->(replyTarget),
    (replyClaim)<-[:AUTHORED]-(replyAuthor),
    (replyClaim)-[:IN]->(replyPackage)
WHERE NOT (replyClaim)-[:TYPE]->(:Type { id: 'ad' })
    AND (NOT ({nocoiners} AND (replyAuthor)<-[:RECEIVER]-()) OR NOT ({coiners} AND NOT (replyAuthor)<-[:RECEIVER]-()))
WITH id, replyClaim, replyTarget, replyAuthor, replyPackage,
    CASE replyClaim
        WHEN null THEN false
        ELSE io.userfeeds.erc721.isValidClaim(replyClaim) END AS erc721ValidClaim
OPTIONAL MATCH
    (replyClaim)-[:CONTEXT]->(replyContext)
WHERE erc721ValidClaim
RETURN
    id,
    collect(replyClaim.id) as reply_id,
    collect(replyTarget.id) as reply_target,
    collect(replyAuthor.id) AS reply_author,
    collect(replyPackage.family) AS reply_family,
    collect(replyPackage.sequence) AS reply_sequence,
    collect(replyPackage.timestamp) AS reply_created_at,
    collect(erc721ValidClaim) AS reply_context_exists,
    collect(replyContext.id) AS reply_context
"""


@pipeable
@filter_debug
@param("nocoiners", required=False)
@param("coiners", required=False)
def run(conn_mgr, input, **params):
    root_messages = input["items"]
    nocoiners = not not params.get("nocoiners")
    coiners = not not params.get("coiners")
    ids = [message["id"] for message in root_messages]
    replies = conn_mgr.run_graph(REPLIES, {"ids": ids, "nocoiners": nocoiners, "coiners": coiners})
    replies = {r["id"]: create_reply_list(r) for r in replies}
    add_replies(root_messages, replies)
    return input


def create_reply_list(r):
    reply_contexts = iter(r["reply_context"])
    return [create_reply(id, target, author, family, sequence, created_at, next(reply_contexts) if context_exists else None)
            for id, target, author, family, sequence, created_at, context_exists in zip_reply_info(r)]


def zip_reply_info(r):
    return zip(
        r["reply_id"],
        r["reply_target"],
        r["reply_author"],
        r["reply_family"],
        r["reply_sequence"],
        r["reply_created_at"],
        r["reply_context_exists"])


def create_reply(id, target, author, family, sequence, created_at, context):
    return {
        "id": id,
        "target": target,
        "author": author,
        "family": family,
        "sequence": sequence,
        "created_at": created_at,
        "context": context
    }


def add_replies(root_messages, replies):
    for message in root_messages:
        message["replies"] = sorted(replies[message["id"]], key=lambda x: x["created_at"])
