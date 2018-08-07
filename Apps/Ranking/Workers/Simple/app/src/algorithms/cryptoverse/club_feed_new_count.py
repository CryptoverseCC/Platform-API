"""
Cryptoverse Club Feed New Messages Count
========================================
"""

from algorithms.utils import param

ROOT_QUERY = """
SELECT count(*) as count FROM persistent_claim where about = %(id)s and timestamp > %(version)s;
"""


@param("versions", required=True)
def run(conn_mgr, input, **params):
    return {
        "items": [count_new_for_id(conn_mgr, id, version) for id, version in params["versions"].items()],
    }


def count_new_for_id(conn_mgr, id, version):
    return {
        "club_id": id,
        "version": version,
        "count": conn_mgr.run_rdb(ROOT_QUERY, {"id": id, "version": version})[0]["count"],
    }
