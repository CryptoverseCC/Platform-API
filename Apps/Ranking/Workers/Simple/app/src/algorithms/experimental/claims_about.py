from algorithms.utils import normalize_to_list
from algorithms.utils import param

FIND_ABOUT = """
SELECT id, about FROM persistent_claim WHERE id IN (SELECT * FROM UNNEST(%(ids)s))
"""


@param("id", required=True)
def run(conn_mgr, input, **params):
    result = conn_mgr.run_rdb(FIND_ABOUT, {"ids": normalize_to_list(params.get("id"))})
    return {row["id"]: row["about"] for row in result}

