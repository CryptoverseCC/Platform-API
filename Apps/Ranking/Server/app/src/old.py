from copy import deepcopy


class InvalidAlgorithm(Exception):
    pass


COMPATIBILITY_FLOWS = {
    'links': [
        {
            'algorithm': 'links',
            'params': {
                'asset': 'rinkeby'
            }
        },
        {
            'algorithm': 'filter_timedecay',
            'params': {}
        },
        {
            'algorithm': 'filter_group',
            'params': {
                'sum_keys': ['score', 'total']
            }
        }
    ],
    'links_whitelist': [
        {
            'algorithm': 'links',
            'params': {
                'asset': 'rinkeby'
            }
        },
        {
            'algorithm': 'filter_whitelist',
            'params': {}
        },
        {
            'algorithm': 'filter_timedecay',
            'params': {}
        },
        {
            'algorithm': 'filter_group',
            'params': {
                'sum_keys': ['score', 'total']
            }
        }
    ]
}


def convert(context, backend, params):

    context = context.lower()

    try:
        asset, context = context.rsplit(':', 1)
    except Exception:
        pass
    else:
        params['asset'] = asset

    if 'whitelist' in params:
        # Remove `transport` mechanism (eg. rinkeby:) from whitelist param
        # We dropped it as it had no benefit to distinguish whitelists by
        # how whitelisting claims were transported into the system
        params['whitelist'] = params['whitelist'].rsplit(':', 1)[-1]
        params['whitelist'] = params['whitelist'].lower()

    params.update(context=context)

    try:
        flow = deepcopy(COMPATIBILITY_FLOWS[backend.lower() + ('_whitelist' if 'whitelist' in params else '')])
    except KeyError:
        raise InvalidAlgorithm()

    for part in flow:
        part['params'].update(params)

    return {
        "flow": flow
    }
