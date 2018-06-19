"""
Feed with replies and likes
===========================

Gives

Version: 0.1.0

Example:

`ranking/kuba_feed_replies_reactions;id=test <https://api.userfeeds.io/ranking/kuba_feed_replies_reactions;id=test>`_

"""

from algorithms.experimental import channel_feed
from algorithms.kuba import replies, reactions
from algorithms.utils import param


@param("id", required=True)
def run(conn_mgr, input, **params):
    result = channel_feed.run(conn_mgr, input, **params)
    result = replies.run(conn_mgr, result)
    result = reactions.run(conn_mgr, result)
    return result
