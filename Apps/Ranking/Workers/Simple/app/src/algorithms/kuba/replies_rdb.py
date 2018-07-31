"""
Reactions
=========

Version: 0.1.0

"""

from algorithms.utils import pipeable, filter_debug

REACTIONS = """
SELECT * FROM persistent_claim AS claim 
WHERE is_valid_erc721_context(claim.author, SPLIT_PART(claim.context, ':', 1) || ':' || SPLIT_PART(claim.context, ':', 2),SPLIT_PART(claim.context, ':', 3),  claim.timestamp)
AND claim.about IN (SELECT * FROM UNNEST(%(ids)s))
ORDER BY claim."timestamp" DESC
"""


@pipeable
@filter_debug
def run(conn_mgr, input, **ignore):
    root_messages = input["items"]
    ids = [message["id"] for message in root_messages]
    replies = conn_mgr.run_rdb(REACTIONS, {"ids": ids})
    # replies = {r["id"]: create_reply_list(r) for r in replies}
    # add_replies(root_messages, replies)
    return replies


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
