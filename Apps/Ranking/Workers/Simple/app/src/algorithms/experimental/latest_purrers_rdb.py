"""
Latest purrers
==============

Version: 0.1.0

"""

from algorithms.utils import sort_by_created_at

LATEST_PURRERS = """
SELECT a.author, a.context, max(a.created_at) as created_at from (
   SELECT a.author as author,
          case when b.is_valid is null and is_valid_erc721_id(a.id) then a.context when b.is_valid then a.context else null end as context,
          a.timestamp AS created_at
   FROM persistent_claim as a
               LEFT OUTER JOIN persistent_claim_is_valid as b on a.id = b.id
   WHERE TO_TIMESTAMP((a.timestamp + 604800000) / 1000) > now()
) as a GROUP BY a.author, a.context
"""


def run(conn_mgr, input, **params):
    query_result = conn_mgr.run_rdb(LATEST_PURRERS, {})
    return {"items": sort_by_created_at(query_result)}
