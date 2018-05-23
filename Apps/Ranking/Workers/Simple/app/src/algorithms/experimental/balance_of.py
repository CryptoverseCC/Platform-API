"""
Calculates balance of given tokens for given identities
=================

Version: 0.1.0
Possible root assets: `ethereum`, `rinkeby`, `ropsten`, `kovan`

Any ERC20: `ethereum:0xe41d2489571d322189246dafa5ebde1f4699f498`

Please note that it will give strange results when asked about ERC721

Example:
`ranking/experimental_balance_of;identity=0xa47ba...78e4b;asset='ethereum:0x57ad...52ec' <https://api-dev.userfeeds.io/ranking/experimental_balance_of;identity=0xa47ba5dc2bb0cdb4433ac4f09fb89b92a1078e4b;asset=ethereum:0x57ad67acf9bf015e4820fbd66ea1a21bed8852ec>`_

"""

from algorithms.utils import normalize_to_list
from algorithms.utils import param

MATCH_SEND_TRANSFERS = """
MATCH
	(identity:Identity)<-[:SENDER]-(transfer:Transfer)
WHERE identity.id in {identities}"""

MATCH_RECEIVED_TRANSFERS = """
MATCH
	(identity:Identity)<-[:RECEIVER]-(transfer:Transfer)
WHERE identity.id in {identities}"""

ASSET_CONDITION = """
AND transfer.asset in {assets}"""

RETURN_STATEMENT = """
RETURN
    identity.id as identity,
    transfer.amount as amount,
    transfer.asset as asset
"""

RECEIVED_TRANSFERS_WITH_ASSETS_QUERY = MATCH_RECEIVED_TRANSFERS + ASSET_CONDITION + RETURN_STATEMENT
SEND_TRANSFERS_WITH_ASSETS_QUERY = MATCH_SEND_TRANSFERS + ASSET_CONDITION + RETURN_STATEMENT


@param("identity", required=True)
@param("asset", required=False)
def run(conn_mgr, input, **params):
    identity = normalize_to_list(params.get('identity'))
    assets = normalize_to_list(params.get('asset'))

    if params.get('asset'):
        items = balance_of(conn_mgr, assets, identity)
    else:
        send_query = MATCH_SEND_TRANSFERS + RETURN_STATEMENT
        received_query = MATCH_RECEIVED_TRANSFERS + RETURN_STATEMENT
        items = balance_of(conn_mgr, assets, identity, send_query, received_query)

    return {"items": items}


def balance_of(conn_mgr,
               assets,
               identity,
               send_query=SEND_TRANSFERS_WITH_ASSETS_QUERY,
               received_query=RECEIVED_TRANSFERS_WITH_ASSETS_QUERY):
    query_param = {
        'identities': identity,
        'assets': assets
    }
    send_transfers = conn_mgr.run_graph(send_query, query_param)
    received_transfers = conn_mgr.run_graph(received_query, query_param)
    return balances_of(received_transfers, send_transfers)


def balances_of(received_transfers, sent_transfers):
    balances = {}
    for item in sent_transfers:
        identity, transfer_asset, transfer_amount = item["identity"], item["asset"], item["amount"]
        amount = balances.get((identity, transfer_asset), 0)
        balances[(identity, transfer_asset)] = amount - int(transfer_amount)
    for item in received_transfers:
        identity, transfer_asset, transfer_amount = item["identity"], item["asset"], item["amount"]
        amount = balances.get((identity, transfer_asset), 0)
        balances[(identity, transfer_asset)] = amount + int(transfer_amount)

    return [
        {
            "identity": identity,
            "asset": transfer_asset,
            "balance": balance
        }
        for (identity, transfer_asset), balance in balances.items()
    ]
