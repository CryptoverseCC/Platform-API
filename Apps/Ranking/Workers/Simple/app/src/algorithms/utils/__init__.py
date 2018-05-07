import logging
from collections import defaultdict


PARAMS = {}


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


def map_result_to_claims(query_result):
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


# Exceptions

class NotPipeableAlgorithm(Exception):
    pass
