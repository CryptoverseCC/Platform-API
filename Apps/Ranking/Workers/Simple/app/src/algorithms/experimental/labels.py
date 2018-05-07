"""
Labels
======

Version: 0.1.0

"""

from algorithms.utils import param

LABELS_META_QUERY = """
MATCH (claim:Entity:Claim)-[:CONTEXT]->(:Entity { id: {context} }), (claim)-[l:LABELS]->()
RETURN l.value as label, count(*) as count
ORDER BY count DESC, label ASC
"""

QUERY = """
MATCH
    (target:Entity)<-[label:LABELS]-(claim:Entity:Claim)-[:CONTEXT]->(:Entity { id: {context} })
WHERE
    {labels} = [] OR label.value IN {labels}
RETURN
    target.id as target, collect(label.value) as labels, count(label) as score
ORDER BY
    score DESC
LIMIT 100
"""


@param("context", required=True)
@param("labels", required=False)
def run(conn_mgr, input, **params):
    labels_meta_query_result = conn_mgr.run_graph(LABELS_META_QUERY, {"context": params["context"]})
    labels_meta = [data["label"] for data in labels_meta_query_result]

    labels_raw = params.get("labels")
    if type(labels_raw) == list:
        labels = labels_raw
    elif type(labels_raw) == str:
        labels = [labels_raw]
    else:
        labels = []
    query_result = conn_mgr.run_graph(QUERY, {
        "context": params["context"],
        "labels": labels,
    })
    items = [
        {
            "target": item["target"],
            "score": int(item["score"]),
        } for item in query_result
    ]
    return {"meta": {"labels": labels_meta}, "items": items}
