"""
Sort
====

Sorts items by given key.
To sort in reversed order pass `order=desc`.

Version: 0.1.0

Example:

`ranking/experimental_tokens;identity=0x157da...2bee3;asset=ethereum:0x06012...a266d/experimental_sort;by=sequence <https://api.userfeeds.io/ranking/experimental_tokens;identity=0x157da080cb7f3e091eadfa32bc7430d9f142bee3;asset=ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d/experimental_sort;by=sequence>`_

`ranking/experimental_tokens;identity=0x157da...2bee3;asset=ethereum:0x06012...a266d/experimental_sort;by=sequence;order=DESC <https://api.userfeeds.io/ranking/experimental_tokens;identity=0x157da080cb7f3e091eadfa32bc7430d9f142bee3;asset=ethereum:0x06012c8cf97bead5deae237070f9587f8e7a266d/experimental_sort;by=sequence;order=desc>`_
"""

from algorithms.utils import pipeable, filter_debug


@pipeable
@filter_debug
def run(conn_mgr, input, by, **params):
    items = sorted(input["items"], key=lambda elem: int(elem[by]), reverse="order" in params)
    input["items"] = items
    return input
