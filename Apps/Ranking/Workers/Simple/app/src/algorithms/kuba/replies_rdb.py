"""
Reactions
=========

Version: 0.1.0

"""

from algorithms.utils import pipeable, filter_debug, group_by

REACTIONS = """
SELECT claim.id, claim.target, claim.author, claim.family, claim.sequence, claim.timestamp AS created_at, claim.context, claim.about,
is_valid_erc721_context(claim.author, SPLIT_PART(claim.context, ':', 1) || ':' || SPLIT_PART(claim.context, ':', 2),SPLIT_PART(claim.context, ':', 3),  claim.timestamp)
FROM persistent_claim AS claim 
WHERE claim.about IN (SELECT * FROM UNNEST(%(ids)s))
ORDER BY claim."timestamp" DESC
"""

@pipeable
@filter_debug
def run(conn_mgr, input, **ignore):
    root_messages = input["items"]
    ids = [message["id"] for message in root_messages]
    replies = conn_mgr.run_rdb(REACTIONS, {"ids": ids})
    for x in replies:
        if not x["is_valid_erc721_context"]:
            x["context"] = None
        del x["is_valid_erc721_context"]
    replies = group_by(replies, "about")
    remove_about(replies)
    add_replies(root_messages, replies)
    return input


def remove_about(reactions):
    for key, value in reactions.items():
        for r in value:
            del r["about"]


def add_replies(root_messages, replies):
    for message in root_messages:
        message["replies"] = sorted_replies_or_empty(message, replies)


def sorted_replies_or_empty(message, replies):
    replay = replies.get(message["id"])
    if replay:
        return sorted(replay, key=lambda x: x["created_at"])
    else:
        return []
