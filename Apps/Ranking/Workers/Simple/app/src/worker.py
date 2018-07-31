#!/usr/bin/env python
import json
import logging
import logging.config; logging.config.fileConfig('/app/logging.conf')

from algorithms import handlers as algorithms
from algorithms.utils import NotPipeableAlgorithm


def run(conn_mgr, request):
    steps = []
    result = {}
    step_debug = {}

    for step_number, step in enumerate(request["flow"]):
        try:
            algorithm = step["algorithm"]
            params = step.get("params", {})
            try:
                algorithm_function = algorithms[algorithm]["run"]
            except KeyError as e:
                logging.exception(e)
                result = "InvalidAlgorithmError: `{}` does not exist".format(algorithm)
                return result

            algorithm_specification = getattr(algorithm_function, 'spec', {})

            if step_number > 0 and not algorithm_specification.get("pipeable"):
                raise NotPipeableAlgorithm("Algorithm '{}' needs to be used as primary algorithm".format(algorithm))

            step_debug = {
                "algorithm": algorithm,
                "params": params
            }
            result = algorithm_function(conn_mgr, result, **params)
            logging.debug(result)
        except Exception as e:
            logging.exception(e)
            result = str(e)
            return result
        finally:
            step_debug["out"] = json.loads(json.dumps(result))
            steps.append(step_debug)
    if request['debug']:
        result['debug'] = steps
    return result
