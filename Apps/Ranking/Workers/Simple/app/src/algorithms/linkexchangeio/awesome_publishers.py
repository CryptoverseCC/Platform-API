"""
Awesome publishers for linkexchange.io website
==============================================

Version: 1.0.0

"""

AWESOME_PUBLISHERS = """
MATCH (c:Claim)-[:GIVES_CREDITS { type: 'interface' }]->(credits:Entity),
  (c)-[:TYPE]->(:Entity { id: 'link' }),
  (c)-[:IN]->(:PackageEthereum)
RETURN
  count(*) AS score,
  credits.id AS publisher
ORDER BY score DESC
LIMIT 99
"""


def run(conn_mgr, input, **params):
    results = conn_mgr.run_graph(AWESOME_PUBLISHERS, params)

    publishers = [{
        "score": i["score"],
        "publisher": i["publisher"]
    } for i in results]

    return {"items": publishers}
