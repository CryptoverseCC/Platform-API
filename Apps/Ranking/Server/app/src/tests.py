import unittest
from old import convert
from flows import path_to_flow


class TestBackwardCompatibility(unittest.TestCase):

    maxDiff = None

    def test_convert_links(self):
        context = 'abc'
        backend = 'links'
        params = {}
        self.assertEqual(convert(context, backend, params), {
            'flow': [
                {
                    'algorithm': 'links',
                    'params': {
                        'context': 'abc',
                        'asset': 'rinkeby',
                    }
                },
                {
                    'algorithm': 'filter_timedecay',
                    'params': {
                        'context': 'abc'
                    }
                },
                {
                    'algorithm': 'filter_group',
                    'params': {
                        'context': 'abc',
                        'sum_keys': ['score', 'total'],
                    }
                }
            ]
        })

    def test_convert_links_whitelist(self):
        context = 'abc'
        backend = 'links'
        params = {'whitelist': 'whl'}
        self.assertEqual(convert(context, backend, params), {
            'flow': [
                {
                    'algorithm': 'links',
                    'params': {
                        'context': 'abc',
                        'asset': 'rinkeby',
                        'whitelist': 'whl',
                    }
                },
                {
                    'algorithm': 'filter_whitelist',
                    'params': {
                        'context': 'abc',
                        'whitelist': 'whl',
                    }
                },
                {
                    'algorithm': 'filter_timedecay',
                    'params': {
                        'context': 'abc',
                        'whitelist': 'whl',
                    }
                },
                {
                    'algorithm': 'filter_group',
                    'params': {
                        'context': 'abc',
                        'sum_keys': ['score', 'total'],
                        'whitelist': 'whl',
                    }
                }
            ]
        })

    def test_convert_links_whitelist_lowercase(self):
        context = 'ABC'
        backend = 'links'
        params = {'whitelist': 'WHL'}
        self.assertEqual(convert(context, backend, params), {
            'flow': [
                {
                    'algorithm': 'links',
                    'params': {
                        'context': 'abc',
                        'asset': 'rinkeby',
                        'whitelist': 'whl',
                    }
                },
                {
                    'algorithm': 'filter_whitelist',
                    'params': {
                        'context': 'abc',
                        'whitelist': 'whl',
                    }
                },
                {
                    'algorithm': 'filter_timedecay',
                    'params': {
                        'context': 'abc',
                        'whitelist': 'whl',
                    }
                },
                {
                    'algorithm': 'filter_group',
                    'params': {
                        'context': 'abc',
                        'sum_keys': ['score', 'total'],
                        'whitelist': 'whl',
                    }
                }
            ]
        })

    def test_convert_links_asset(self):
        context = 'abc'
        backend = 'links'
        params = {'asset': 'ethereum'}
        self.assertEqual(convert(context, backend, params), {
            'flow': [
                {
                    'algorithm': 'links',
                    'params': {
                        'context': 'abc',
                        'asset': 'ethereum',
                    }
                },
                {
                    'algorithm': 'filter_timedecay',
                    'params': {
                        'context': 'abc',
                        'asset': 'ethereum',
                    }
                },
                {
                    'algorithm': 'filter_group',
                    'params': {
                        'context': 'abc',
                        'sum_keys': ['score', 'total'],
                        'asset': 'ethereum',
                    }
                }
            ]
        })

    def test_convert_links_whitelist_asset(self):
        context = 'abc'
        backend = 'links'
        params = {'whitelist': 'whl', 'asset': 'ethereum'}
        self.assertEqual(convert(context, backend, params), {
            'flow': [
                {
                    'algorithm': 'links',
                    'params': {
                        'context': 'abc',
                        'asset': 'ethereum',
                        'whitelist': 'whl',
                    }
                },
                {
                    'algorithm': 'filter_whitelist',
                    'params': {
                        'context': 'abc',
                        'asset': 'ethereum',
                        'whitelist': 'whl',
                    }
                },
                {
                    'algorithm': 'filter_timedecay',
                    'params': {
                        'context': 'abc',
                        'asset': 'ethereum',
                        'whitelist': 'whl',
                    }
                },
                {
                    'algorithm': 'filter_group',
                    'params': {
                        'context': 'abc',
                        'sum_keys': ['score', 'total'],
                        'asset': 'ethereum',
                        'whitelist': 'whl',
                    }
                }
            ]
        })

    def test_convert_links_whitelist_drop_transport_part(self):
        context = 'abc'
        backend = 'links'
        params = {'whitelist': 'rinekby:whl'}
        self.assertEqual(convert(context, backend, params), {
            'flow': [
                {
                    'algorithm': 'links',
                    'params': {
                        'context': 'abc',
                        'asset': 'rinkeby',
                        'whitelist': 'whl',
                    }
                },
                {
                    'algorithm': 'filter_whitelist',
                    'params': {
                        'context': 'abc',
                        'whitelist': 'whl',
                    }
                },
                {
                    'algorithm': 'filter_timedecay',
                    'params': {
                        'context': 'abc',
                        'whitelist': 'whl',
                    }
                },
                {
                    'algorithm': 'filter_group',
                    'params': {
                        'context': 'abc',
                        'sum_keys': ['score', 'total'],
                        'whitelist': 'whl',
                    }
                }
            ]
        })


class TestPathToFlow(unittest.TestCase):

    maxDiff = None

    def test_path_to_flow(self):
        path = '/links;context=abc&asset=rinkeby/filter_timedecay/filter_group;sum_keys=score&sum_keys=total/'
        flow = path_to_flow(path)
        self.assertEqual(flow, {
            'flow': [
                {
                    'algorithm': 'links',
                    'params': {
                        'context': 'abc',
                        'asset': 'rinkeby',
                    }
                },
                {
                    'algorithm': 'filter_timedecay',
                    'params': {}
                },
                {
                    'algorithm': 'filter_group',
                    'params': {
                        'sum_keys': ['score', 'total'],
                    }
                }
            ]
        })


if __name__ == '__main__':
    unittest.main()
