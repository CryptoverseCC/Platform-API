"""
Profile
=======

Algorithm used by https://userfeeds.github.io/cryptopurr/

Returns frontend specific structure of purrs.

Version: 0.1.0

Example:

`ranking/cryptoverse_profile;id=ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d:608827 <https://api.userfeeds.io/ranking/cryptoverse_profile;id=ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d:608827>`_

Json claim example:

.. code-block:: json

    {
        "twitter": "https://twitter.com/Kitty_608827"
    }
"""

from algorithms.utils import addressPattern
from algorithms.experimental import context_feed, filter_labels, valid_erc721, sort
from algorithms.utils import param

ADDRESS_PROFILE_QUERY = """
MATCH (social:Entity)<-[l:LABELS]-(c:Claim)<-[:AUTHORED]-(:Identity { id: {identity} }), (c)-[:IN]->(p)
WHERE l.value IN ['facebook', 'instagram', 'twitter', 'github', 'discord', 'telegram'] AND NOT io.userfeeds.erc721.isValidClaim(c)
RETURN social.id as social, l.value as label
ORDER BY p.timestamp
"""


@param("id", required=True)
def run(conn_mgr, input, **params):
    id = params["id"]
    output = {}
    if addressPattern.match(id):
        input = conn_mgr.run_graph(ADDRESS_PROFILE_QUERY, {"identity": id})
        for item in input:
            output[item["label"]] = item["social"]
    else:
        input = context_feed.run(conn_mgr, input, id=id)
        input = filter_labels.run(conn_mgr, input, id=["facebook", "instagram", "twitter", "github", "discord", "telegram"])
        input = valid_erc721.run(conn_mgr, input)
        input = sort.run(conn_mgr, input, by="created_at")
        for claim in input["items"]:
            for label in claim["labels"]:
                output[label] = claim["target"]
    return output
