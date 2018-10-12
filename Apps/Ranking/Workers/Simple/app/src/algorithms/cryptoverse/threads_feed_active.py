"""
Cryptoverse feed with replies and likes (sorted by time in replies and likes)
=============================================================================

"""

from algorithms.cryptoverse import threads_feed as feed


def run(conn_mgr, input, **params):
    result = feed.run(conn_mgr, input, **params)
    result = sorted(result["items"], key=lambda x: max_created_at(x), reverse=True)
    return {
        "items": result
    }


def max_created_at(item):
    ret = item["created_at"]
    for reply in item["replies"]:
        if ret < reply["created_at"]:
            ret = reply["created_at"]
    return ret
