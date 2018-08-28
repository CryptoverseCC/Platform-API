"""
Cryptoverse club feed with replies and likes (sorted by time in replies and likes)
==================================================================================

"""

from algorithms.cryptoverse import club_feed
from algorithms.utils import param


@param("id", required=True)
def run(conn_mgr, input, **params):
    result = club_feed.run(conn_mgr, input, **params)
    result = sorted(result["items"], key=lambda x: max_created_at(x), reverse=True)
    return {
        "items": result
    }


def max_created_at(item):
    ret = item["created_at"]
    for like in item["likes"]:
        if ret < like["created_at"]:
            ret = like["created_at"]
    for reply in item["replies"]:
        if ret < reply["created_at"]:
            ret = reply["created_at"]
        for like in reply["likes"]:
            if ret < like["created_at"]:
                ret = like["created_at"]
    return ret
