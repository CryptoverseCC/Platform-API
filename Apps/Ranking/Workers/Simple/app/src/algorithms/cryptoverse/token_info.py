"""
Token Info
==========

Algorithm used by Cryptoverse

Version: 0.1.0

Example:

`ranking/token_info;ids=ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d <https://api.userfeeds.io/ranking/ranking/token_info;ids=ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d>`_

"""

from algorithms.utils import param
from web3 import Web3
from cachetools.func import lfu_cache

ABI = [
    {
        "constant": True,
        "inputs": [],
        "name": "name",
        "outputs": [
            {
                "name": "",
                "type": "string"
            }
        ],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [
            {
                "name": "",
                "type": "uint256"
            }
        ],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "symbol",
        "outputs": [
            {
                "name": "",
                "type": "string"
            }
        ],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    }
]


@lfu_cache(maxsize=2048)
def getData(conn_mgr, asset):
    address = Web3.toChecksumAddress(asset.split(':')[1])
    try:
        data = {
            "asset": asset,
            "name": conn_mgr.run_eth(address, ABI, 'name'),
            "symbol": conn_mgr.run_eth(address, ABI, 'symbol'),
            "decimals": conn_mgr.run_eth(address, ABI, 'decimals'),
        }
    except Exception as e:
        print(e)
        data = {
            "asset": asset,
            "name": address,
            "symbol": "",
            "decimals": None
        }
    return data


@param("id", required=True)
def run(conn_mgr, input, **params):
    ids = type(params['id']) == list and params['id'] or [params['id']]
    return {asset: getData(conn_mgr, asset) for asset in ids}
