"""
Ads
===

"""

from algorithms.utils import pipeable, filter_debug, param

ADS_QUERY = """
UNWIND {ids} as id
MATCH (claim:Claim { id: id })
OPTIONAL MATCH
    (claim)<-[:ABOUT]-(adClaim),
    (adClaim)-[:TARGET]->(adTarget),
    (adClaim)<-[:AUTHORED]-(adAuthor),
    (adClaim)-[:IN]->(adPackage),
    (adClaim)-[:CONNECTED_WITH]->(sent)-[:RECEIVER]->(contract:Identity { id: '0x53b7c52090750c30a40babd50024588e527292c3' })
WHERE (adClaim)-[:TYPE]->(:Type { id: 'ad' })
OPTIONAL MATCH
    (adClaim)-[:CONNECTED_WITH]->(returned)-[:RECEIVER]->(forwarder { id: '0xfcd0b4035f0d4f97d171a28d8256842fedfdcdeb' }), (returned)-[:SENDER]->(contract)
RETURN
    id,
    collect(adClaim.id) as ad_ids,
    collect(adTarget.id) as ad_targets,
    collect(adAuthor.id) AS ad_authors,
    collect(adPackage.family) AS ad_families,
    collect(adPackage.sequence) AS ad_sequences,
    collect(adPackage.timestamp) AS ad_created_at,
    collect(sent.amount) AS sent_amounts,
    collect(returned.amount) AS returned_amounts
"""


@pipeable
@filter_debug
def run(conn_mgr, input, **params):
    root_messages = input["items"]
    ids = [message["id"] for message in root_messages]
    all_ads = conn_mgr.run_graph(ADS_QUERY, {"ids": ids})
    all_ads = {thread_ads["id"]: create_ad_list(thread_ads) for thread_ads in all_ads}
    add_ads(root_messages, all_ads)
    return input


def create_ad_list(ads):
    return [create_ad(id, target, author, family, sequence, created_at, sent, returned)
            for id, target, author, family, sequence, created_at, sent, returned in zip_ad_info(ads)]


def zip_ad_info(ads):
    return zip(
        ads["ad_ids"],
        ads["ad_targets"],
        ads["ad_authors"],
        ads["ad_families"],
        ads["ad_sequences"],
        ads["ad_created_at"],
        ads["sent_amounts"],
        ads["returned_amounts"])


def create_ad(id, target, author, family, sequence, created_at, sent, returned):
    return {
        "id": id,
        "target": target,
        "author": author,
        "family": family,
        "sequence": sequence,
        "created_at": created_at,
        "score": int(float(sent)) - int(float(returned))
    }


def add_ads(root_messages, ads):
    for m in root_messages:
        m["ads"] = ads[m["id"]]
