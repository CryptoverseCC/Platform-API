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

from algorithms.utils import param
from algorithms.experimental import context_feed, filter_labels, valid_erc721, sort


@param("id", required=True)
def run(conn_mgr, input, **params):
    input = context_feed.run(conn_mgr, input, id=params["id"])
    input = filter_labels.run(conn_mgr, input, id=["facebook", "instagram", "twitter", "github"])
    input = valid_erc721.run(conn_mgr, input)
    input = sort.run(conn_mgr, input, by="created_at")
    output = {}
    for claim in input["items"]:
        for label in claim["labels"]:
            output[label] = claim["target"]
    return output
