"""
Filter: Group
=============

Version: 1.0.0

"""

from algorithms.utils import pipeable, filter_debug


@pipeable
@filter_debug
def run(conn_mgr, response, sum_keys=["score"], **ignore):
    sum_keys = [sum_keys] if isinstance(sum_keys, str) else sum_keys
    result = {}
    for item in response['items']:
        if item["id"] not in result:
            result[item["id"]] = item
            result[item["id"]]["group_count"] = 1
        else:
            for key in sum_keys:
                result[item["id"]][key] += item[key]
            result[item["id"]]["group_count"] += 1

    items = list(result.values())
    # TODO: looks better, but still not good
    if "score" in sum_keys:
        items = sorted(items, key=lambda x: x['score'], reverse=True)
    response.update(items=items)

    return response
