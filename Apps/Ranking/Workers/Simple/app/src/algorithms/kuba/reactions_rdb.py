"""
Reactions
=========

Version: 0.1.0

"""

from algorithms.utils import pipeable, filter_debug, group_by

REACTIONS = """
SELECT claim.id, claim.target, claim.author, claim.family, claim.sequence, claim.timestamp AS created_at, claim.context,
is_valid_erc721_context(claim.author, SPLIT_PART(claim.context, ':', 1) || ':' || SPLIT_PART(claim.context, ':', 2),SPLIT_PART(claim.context, ':', 3),  claim.timestamp)
FROM persistent_claim AS claim 
WHERE claim.target IN (SELECT * FROM UNNEST(%(ids)s))
ORDER BY claim."timestamp" DESC
"""


@pipeable
@filter_debug
def run(conn_mgr, input, **ignore):
    root_messages = input["items"]
    ids = all_ids(root_messages)
    reactions = conn_mgr.run_rdb(REACTIONS, {"ids": ids})
    for x in reactions:
        if not x["is_valid_erc721_context"]:
            x["context"] = None
        del x["is_valid_erc721_context"]
    reactions = group_by(reactions, "target")
    add_likes(root_messages, reactions)
    return input


def all_ids(messages):
    return [message["id"] for message in messages] + [id for message in messages for id in all_ids(message.get("replies", [])) ]


def add_likes(messages, reactions):
    for message in messages:
        message["likes"] = sorted_reactions_or_none(message, reactions)
        add_likes(message.get("replies", []), reactions)

def sorted_reactions_or_none(message, reactions):
    reaction = reactions.get(message["id"])
    if reaction:
        return sorted(reaction, key=lambda x: x["created_at"])
    else:
        return []
