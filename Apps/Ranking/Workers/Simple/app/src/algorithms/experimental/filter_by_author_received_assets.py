"""
Filter by authors who received specific asset
=============================================

Version: 0.1.0

For now it filters claims authored by identities which received at latest one transfer of given asset.

Possible root assets: `ethereum`, `rinkeby`, `ropsten`, `kovan`

Any ERC20: `ethereum:0xe41d2489571d322189246dafa5ebde1f4699f498`

Example:

`ranking/experimental_claims;target=claim:0x49994...bf8e8:0/experimental_filter_by_author_received_asset;asset=rinkeby <https://api-staging.userfeeds.io/ranking/experimental_claims;target=claim:0x4999436ecf49984576651c7586dc95d4b59766e00c779cc2fdeade6ffc0bf8e8:0/experimental_filter_by_author_received_asset;asset=rinkeby>`_

"""

from algorithms.utils import param, pipeable, filter_debug


FILTER_AUTHORS = """
MATCH (author:Entity:Identity)
WHERE author.id IN {authors} and (author)<-[:RECEIVER]-(:Transfer {asset: {asset} })
RETURN author.id as author
"""


@pipeable
@filter_debug
@param("asset", required=True)
def run(conn_mgr, input, **params):
    authors = collect_authors(input)
    response = conn_mgr.run_graph(FILTER_AUTHORS, {"authors": authors, "asset": params["asset"]})
    valid_authors = parse_response(response)
    return {
        "items": filter_valid(input, valid_authors)
    }


def collect_authors(input):
    return [claim["author"] for claim in input["items"]]


def parse_response(response):
    return set(record["author"] for record in response)


def filter_valid(input, valid_authors):
    return [claim for claim in input["items"] if claim["author"] in valid_authors]


