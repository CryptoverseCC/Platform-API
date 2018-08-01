"""
Filter feed to only contain claims with given context
=====================================================

Version: 0.1.0

"""

from algorithms.utils import param, pipeable, filter_debug


@pipeable
@filter_debug
@param("asset", required=True)
def run(conn_mgr, input, **params):
    asset = params["asset"]
    filtered = filter_context(input["items"], asset)
    return {"items": filtered}


def filter_context(items, asset):
    filtered = [x for x in items if x["context"] and x["context"].startswith(asset)]
    for x in filtered:
        if x.get("likes"):
            x["likes"] = filter_context(x["likes"], asset)
        if x.get("replies"):
            x["replies"] = filter_context(x["replies"], asset)
    return filtered
