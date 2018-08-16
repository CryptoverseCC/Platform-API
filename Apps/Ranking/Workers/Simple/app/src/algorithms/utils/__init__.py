import logging
import re
from collections import defaultdict

PARAMS = {}


tokenPattern = re.compile("[a-z]+:0x[0-9a-f]{40}:\d+$")
addressPattern = re.compile("0x[0-9a-f]{40}")

def param(name, required=False):
    # Decorator for validation purposes
    # dummy for now
    # TODO: add validation logic
    def decorator(func):
        def wrapper(*args, **kwargs):
            # if name in kwargs ...
            return func(*args, **kwargs)

        spec = getattr(func, 'spec', defaultdict(dict))
        spec["params"][name] = required
        wrapper.spec = spec
        return wrapper
    return decorator


def pipeable(func):
    """
    Usage:

    @pipeable
    def run(conn_manager, input, **params):
        ...doing something with input...

    """
    spec = getattr(func, 'spec', defaultdict(dict))
    spec["pipeable"] = True
    func.spec = spec
    return func


def filter_debug(fun):
    def inner(*args, **kwargs):
        logging.debug(args)
        logging.debug(kwargs)
        result = fun(*args, **kwargs)
        logging.debug(result)
        return result
    return inner


def materialize_records(query_result):
    return [materialize_record(record) for record in query_result]


def materialize_record(record):
    return {key: record[key] for key in record.keys()}


def sort_by_created_at(query_result):
    return sorted(query_result, key=lambda x: x["created_at"], reverse=True)


def normalize_to_list(raw_query_param):
    if isinstance(raw_query_param, list):
        return raw_query_param
    else:
        return [raw_query_param]


def group_by(items, key):
    ret = {}
    for i in items:
        ret[i[key]] = ret.get(i[key], []) + [i]
    return ret

# Exceptions

class NotPipeableAlgorithm(Exception):
    pass


def create_asset(contract_mapping):
    return contract_mapping.network + ":" + contract_mapping.address