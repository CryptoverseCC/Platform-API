"""
Claims
======

Version: 0.1.0

- Filter by id

    `ranking/experimental_claims;id=claim:0x98a87...526e6:0 <https://api.userfeeds.io/ranking/experimental_claims;id=claim:0x98a873f7f2843a12fa76d3026ba30072ee21a70f34324e9ec7875c21cb8526e6:0>`_

- Filter by author

    `ranking/experimental_claims;author=0x7195ebc1bdbcff1d8557541a2b186c6dfd01aef8 <https://api.userfeeds.io/ranking/experimental_claims;author=0x7195ebc1bdbcff1d8557541a2b186c6dfd01aef8>`_

- Filter by target

    `ranking/experimental_claims;target=claim:0x1470e...fd0d1:0 <https://api.userfeeds.io/ranking/experimental_claims;target=claim:0x1470ee0b001370a4e84272a117c94182c092f8e0bfb22b60909c754ce9dfd0d1:0>`_

All of above filters support filtering by multiple arguments:

    `ranking/experimental_claims;author=0x7195e...1aef8;author=0xda9d64...8d18e1
    <https://api.userfeeds.io/ranking/experimental_claims;author=0x7195ebc1bdbcff1d8557541a2b186c6dfd01aef8;author=0xda9d643b264d969788adfc01e22c8b3e2e8d18e1>`_

Filters can be put in any combination:

 `ranking/experimental_claims;author=0x7195e...1aef8;target=claim:0x1470ee...fd0d1:0 <https://api.userfeeds.io/ranking/experimental_claims;author=0x7195ebc1bdbcff1d8557541a2b186c6dfd01aef8;target=claim:0x1470ee0b001370a4e84272a117c94182c092f8e0bfb22b60909c754ce9dfd0d1:0>`_

JSON claim example:

.. code-block:: json

    {
        "author": "0x7195ebc1bdbcff1d8557541a2b186c6dfd01aef8",
        "created_at": 1522921352000,
        "family": "kovan",
        "id": "claim:0x98a873f7f2843a12fa76d3026ba30072ee21a70f34324e9ec7875c21cb8526e6:0",
        "sequence": 6727044,
        "target": "claim:0x1470ee0b001370a4e84272a117c94182c092f8e0bfb22b60909c754ce9dfd0d1:0"
    }
"""

from algorithms.utils import map_result_to_claims
from algorithms.utils import param
from algorithms.utils import sort_by_created_at
from algorithms.utils import normalize_to_list

FIND_CLAIMS = """
MATCH (claim:Entity:Claim),
    (claim)-[:TARGET]->(target),
    (claim)-[:IN]->(package:Entity:Package),
    (claim)<-[:AUTHORED]-(author:Entity:Identity)
"""

RETURN = """RETURN
    claim.id as id,
    target.id as target,
    author.id as author,
    package.family as family,
    package.sequence as sequence,
    package.timestamp as created_at
"""

FILTER_BY_ID = """claim.id IN {ids}
"""

FILTER_BY_TARGETS = """target.id IN {targets}
"""

FILTER_BY_AUTHORS = """author.id IN {authors}
"""


@param("id", required=False)
@param("target", required=False)
@param("author", required=False)
def run(conn_mgr, input, **params):
    query = build_query(params)
    query_result = conn_mgr.run_graph(query, {
        "ids": normalize_to_list(params.get("id")),
        "targets": normalize_to_list(params.get("target")),
        "authors": normalize_to_list(params.get("author"))
    })
    mapped_items = map_result_to_claims(query_result)
    return {"items": sort_by_created_at(mapped_items)}


def build_query(params):
    return FIND_CLAIMS + create_where_clause(params) + RETURN


def create_where_clause(params):
    where = []
    if params.get("id"):
        where.append(FILTER_BY_ID)
    if params.get("target"):
        where.append(FILTER_BY_TARGETS)
    if params.get("author"):
        where.append(FILTER_BY_AUTHORS)
    if where:
        return "WHERE " + "AND ".join(where)
    else:
        return ""
