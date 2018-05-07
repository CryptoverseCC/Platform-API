from algorithms import handlers


def run(conn_mgr, input, **params):
    algos = [{
        "name": algo["name"],
        "spec": algo["spec"],
        "docs": algo["docs"]
    } for name, algo in handlers.items()]
    return {
        "items": algos
    }
