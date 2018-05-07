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
    (claim)<-[:TARGET]-(reactionClaim:Claim),
    (reactionClaim)<-[:AUTHORED]-(reactionAuthor:Identity),
    (reactionClaim)-[:IN]->(reactionPackage:Package)
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
def run(conn_mgr, input, **ignore):
    root_messages = input["items"]
    ids = [message["id"] for message in root_messages] + [reply["id"] for message in root_messages for reply in
                                                          message["replies"]]
    reactions = conn_mgr.run_graph(REACTIONS, {"ids": ids})
    reactions = {r["id"]: create_like_list(r) for r in reactions}
    add_likes(root_messages, reactions)
    return input


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


def add_likes(root_messages, reactions):
    for message in root_messages:
        message["likes"] = reactions[message["id"]]
        for reply in message["replies"]:
            reply["likes"] = reactions[reply["id"]]
