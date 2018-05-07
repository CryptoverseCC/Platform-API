"""
Filter airdrop receivers
========================

Version: 0.1.0

Filters input in the form of
```
{
  "items" [
    { "address": "0xabc...", "something": "else" },
    { "address": "0x123...", "moar": "things" }
  ]
}

to contain only elements connected with airdrop identified by given id,
where currently this id is in the form of claim:ethereum_transaction_hash:0.

"""

from algorithms.experimental import airdrop_receivers
from algorithms.utils import param, pipeable, filter_debug


@pipeable
@filter_debug
@param("id", required=True)
def run(conn_mgr, input, id, **ignore):
    receivers = airdrop_receivers.run(conn_mgr, None, id)
    addresses = set([item["address"] for item in receivers["items"]])
    return {
        "items": [item for item in input["items"] if item["address"] in addresses]
    }
