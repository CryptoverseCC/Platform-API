import logging
import importlib.util
import os

CURRENT_DIR = os.path.dirname(__file__)

handlers = {}


def algo_files():
    for root, directories, filenames in os.walk(CURRENT_DIR):
        for filename in filenames:
            path = os.path.join(root, filename)
            if '__' in path or path.endswith('.pyc'):
                continue

            yield path


def import_algo(module_name, path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if hasattr(module, 'run'):
        run_func = getattr(module, "run")
        return {
            "run": run_func,
            "name": module_name,
            "path": path,
            "docs": module.__doc__,
            "spec": getattr(run_func, "spec", {})
        }


for path in algo_files():
    module_name = os.path.relpath(path, CURRENT_DIR)[:-3].replace(os.path.sep, '_')
    try:
        handlers[module_name] = import_algo(module_name, path)
    except Exception as e:
        logging.exception(e)
