"""
Reactions
=========

Version: 0.1.0

"""

from algorithms.utils import pipeable, filter_debug, param

REACTIONS = """
UNWIND {ids} as id
MATCH (claim:Claim { id: id })
OPTIONAL MATCH
    (claim)<-[:TARGET]-(reactionClaim:Claim),
    (reactionClaim)<-[:AUTHORED]-(reactionAuthor:Identity),
    (reactionClaim)-[:IN]->(reactionPackage:Package)
WHERE NOT ({nocoiners} AND (reactionAuthor)<-[:RECEIVER]-()) OR NOT ({coiners} AND NOT (reactionAuthor)<-[:RECEIVER]-())
WITH id, reactionClaim, reactionAuthor, reactionPackage,
    CASE reactionClaim
        WHEN null THEN false
        ELSE io.userfeeds.erc721.isValidClaim(reactionClaim) END AS erc721ValidClaim
OPTIONAL MATCH
    (reactionClaim)-[:CONTEXT]->(reactionContext:Entity)
WHERE erc721ValidClaim
RETURN
    id,
    collect(reactionClaim.id) as reaction_id,
    collect(reactionAuthor.id) AS reaction_author,
    collect(reactionPackage.family) AS reaction_family,
    collect(reactionPackage.sequence) AS reaction_sequence,
    collect(reactionPackage.timestamp) AS reaction_created_at,
    collect(erc721ValidClaim) AS reaction_context_exists,
    collect(reactionContext.id) AS reaction_context
"""


@pipeable
@filter_debug
@param("nocoiners", required=False)
@param("coiners", required=False)
def run(conn_mgr, input, **params):
    root_messages = input["items"]
    nocoiners = not not params.get("nocoiners")
    coiners = not not params.get("coiners")
    ids = all_ids(root_messages)
    reactions = conn_mgr.run_graph(REACTIONS, {"ids": ids, "nocoiners": nocoiners, "coiners": coiners})
    reactions = {r["id"]: create_like_list(r) for r in reactions}
    add_likes(root_messages, reactions)
    return input


def all_ids(messages):
    return [message["id"] for message in messages] + [id for message in messages for id in all_ids(message.get("replies", [])) ]


def create_like_list(r):
    reaction_contexts = iter(r["reaction_context"])
    return [create_like(id, author, family, sequence, created_at, next(reaction_contexts) if context_exists else None)
            for id, author, family, sequence, created_at, context_exists in zip_reaction_info(r)]


def zip_reaction_info(r):
    return zip(
        r["reaction_id"],
        r["reaction_author"],
        r["reaction_family"],
        r["reaction_sequence"],
        r["reaction_created_at"],
        r["reaction_context_exists"])


def create_like(id, author, family, sequence, created_at, context):
    return {
        "id": id,
        "author": author,
        "family": family,
        "sequence": sequence,
        "created_at": created_at,
        "context": context
    }


def add_likes(messages, reactions):
    for message in messages:
        message["likes"] = sorted(reactions[message["id"]], key=lambda x: x["created_at"])
        add_likes(message.get("replies", []), reactions)
