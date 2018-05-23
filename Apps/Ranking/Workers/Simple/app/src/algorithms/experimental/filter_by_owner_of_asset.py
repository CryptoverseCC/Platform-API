"""
Filter by owners of specific asset
==================================

Version: 0.1.0

Possible root assets: `ethereum`, `rinkeby`, `ropsten`, `kovan`

Any ERC20: `ethereum:0xe41d2489571d322189246dafa5ebde1f4699f498`

Example:


`ranking/experimental_claims;target=claim:0x7299...f2bf:0/experimental_filter_by_owner_of_asset;asset=rinkeby:0x5301...2bb8d <https://api-dev.userfeeds.io/ranking/experimental_claims;target=claim:0x72990709f7d5392725dec42392cea98e8c3994dcae0820974b6368df2df4f2bf:0/experimental_filter_by_owner_of_asset;asset=rinkeby:0x5301f5b1af6f00a61e3a78a9609d1d143b22bb8d>`_

"""

from algorithms.experimental.balance_of import balance_of
from algorithms.utils import param, pipeable, filter_debug
from functional import seq

@pipeable
@filter_debug
@param("asset", required=True)
def run(conn_mgr, input, **params):
    authors = collect_authors(input)
    balance = balance_of(conn_mgr, [params["asset"]], authors)
    owners = seq(balance).filter(lambda x: x['balance'] > 0 ).map(lambda x: x['identity'])
    return {
        "items": filter_valid(input, owners)
    }


def collect_authors(input):
    return [claim["author"] for claim in input["items"]]


def filter_valid(input, owners):
    return [claim for claim in input["items"] if claim["author"] in owners]
