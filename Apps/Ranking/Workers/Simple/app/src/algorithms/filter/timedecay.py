"""
Filter: Timedecay
=================

Version: 1.0.0

"""

from algorithms.utils import pipeable, filter_debug


@pipeable
@filter_debug
def run(conn_mgr, response, period=7, **ignore):
    from datetime import timedelta, datetime

    def update(item, period):
        base = datetime.fromtimestamp(item["created_at"] / 1e3) + period
        now = datetime.now()

        if base < now:
            item["score"] = 0
        else:
            valid_for = (base - now).total_seconds()
            item["score"] = int(item["score"]) / period.total_seconds() * valid_for

        return item

    period = timedelta(days=int(period))

    response.update(items=[update(i, period) for i in response['items']])

    return response
