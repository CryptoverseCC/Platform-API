"""
Boost
=====

Claim targets grouped and sorted by token sent to certain address.

Version: 0.1.0

Example:

`ranking/experimental_boost;asset=ethereum;context=0x9816b4d0da6ae6204c0a8222ec47b0f33ff3f910 <https://api.userfeeds.io/ranking/experimental_boost;asset=ethereum;context=0x9816b4d0da6ae6204c0a8222ec47b0f33ff3f910>`_

"""

from algorithms.experimental import payments, sort
from algorithms.filter import group
from algorithms.utils import param

@param("context", required=True)
@param("asset", required=True)
def run(conn_mgr, input, **params):
    result = payments.run(conn_mgr, input, **params)
    result = group.run(conn_mgr, result)
    result = sort.run(conn_mgr, result, by="score", order="desc")
    return result
