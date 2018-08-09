"""
Reactions
=========

Version: 0.1.0

"""

from algorithms.utils import pipeable, filter_debug, group_by

REACTIONS = """
SELECT claim.id, claim.target, claim.author, claim.family, claim.sequence, claim.timestamp AS created_at, claim.context,
case when valid.is_valid is null then is_valid_erc721_id(claim.id) else valid.is_valid end as is_valid_erc721_context
FROM persistent_claim AS claim
 LEFT OUTER JOIN persistent_claim_is_valid AS valid ON claim.id = valid.id
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
    remove_target(reactions)
    add_likes(root_messages, reactions)
    return input


def all_ids(messages):
    return [message["id"] for message in messages] + [id for message in messages for id in all_ids(message.get("replies", [])) ]


def remove_target(reactions):
    for key, value in reactions.items():
        for r in value:
            del r["target"]


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
