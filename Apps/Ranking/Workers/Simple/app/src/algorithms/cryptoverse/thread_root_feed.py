"""
Cryptoverse thread root feed with likes
=======================================
"""

from algorithms.utils import claimPattern, tokenPattern, assetPattern, addressPattern
from algorithms.cryptoverse import thread_root
from algorithms.kuba import reactions
from algorithms.utils import param


@param("id", required=True)
def run(conn_mgr, input, **params):
    result = thread_root.run(conn_mgr, input, **params)
    result = reactions.run(conn_mgr, result)
    set_types(result["items"])
    return result


def set_types(items):
    for i in items:
        if not isinstance(i["target"], str):
            i["type"] = "like"
            set_type(i["target"])
        else:
            set_type(i)


def set_type(i):
    if tokenPattern.match(i["target"]) or addressPattern.match(i["target"]):
        i["type"] = "boost"
    elif i["about"]:
        if claimPattern.match(i["about"]):
            i["type"] = "response"
        elif tokenPattern.match(i["about"]):
            i["type"] = "post_to"
        elif addressPattern.match(i["about"]):
            i["type"] = "post_to_simple"
        elif assetPattern.match(i["about"]):
            i["type"] = "post_club"
        else:
            i["type"] = "post_about"
    else:
        i["type"] = "regular"
