"""
Cryptoverse single (extended) feed with replies and likes
=========================================================

"""

from algorithms.cryptoverse import single_extended, single_extended_address
from algorithms.kuba import replies, reactions
from algorithms.utils import param
from algorithms.utils import tokenPattern, assetPattern, addressPattern


@param("id", required=True)
def run(conn_mgr, input, **params):
    ids = params["id"]
    if isinstance(ids, str):
        ids = [ids]
    address_ids = [id for id in ids if addressPattern.match(id)]
    token_ids = [id for id in ids if not addressPattern.match(id)]
    address_result = single_extended_address.run(conn_mgr, input, id=address_ids)
    token_result = single_extended.run(conn_mgr, input, id=token_ids)
    result = {
        "items": sorted(address_result["items"] + token_result["items"], key=lambda x: x["created_at"], reverse=True)
    }
    result = replies.run(conn_mgr, result)
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
    if i.get("reply_to"):
        i["type"] = "response"
    elif tokenPattern.match(i["target"]) or addressPattern.match(i["target"]):
        i["type"] = "boost"
    elif i.get("label") in ["github", "twitter", "instagram", "facebook", "discord", "telegram"]:
        i["type"] = "social"
    elif i["about"]:
        if tokenPattern.match(i["about"]):
            i["type"] = "post_to"
        elif addressPattern.match(i["about"]):
            i["type"] = "post_to_simple"
        elif assetPattern.match(i["about"]):
            i["type"] = "post_club"
        else:
            i["type"] = "post_about"
    else:
        i["type"] = "regular"
