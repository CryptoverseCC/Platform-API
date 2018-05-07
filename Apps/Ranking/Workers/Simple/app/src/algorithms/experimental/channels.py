"""
Channels
========

Version: 0.1.0

Example:
::
    curl -X POST \\
        -d '{"flow":[{"algorithm":"experimental_channels","params":{"starts_with":"https://"}}]}' \\
        -H 'Content-Type: application/json' 'https://api.userfeeds.io/ranking/'

Json claim example:

.. code-block:: json

    {
        "type":["about"],
        "claim":{
            "target":"Cool website, bro!",
            "about":"https://www.google.com"
        }
    }

ERC721 example:

`ranking/experimental_channels;starts_with=ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d <https://api.userfeeds.io/ranking/experimental_channels;starts_with=ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d>`_

Json claim example:

.. code-block:: json

    {
        "type": ["about"],
        "claim": {
            "target": "New cool kitten on the block",
            "about": "ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d:593163"
        },
        "context": "ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d:608827"
    }

"""


from algorithms.utils import param

FIND_CHANNELS = """
MATCH
    (about:Entity)<-[:ABOUT]-()
WHERE
    about.id STARTS WITH {starts_with}
RETURN
    distinct about.id as channel
"""


@param("starts_with", required=True)
def run(conn_mgr, input, **params):
    query_result = conn_mgr.run_graph(FIND_CHANNELS, params)
    return {"items": retrieve_channels(query_result)}


def retrieve_channels(query_result):
    return [{"id": record["channel"]} for record in query_result]
