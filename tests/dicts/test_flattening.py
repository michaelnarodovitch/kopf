import pytest

from kopf.structs import dicts


@pytest.mark.parametrize('obj,expected', [
    pytest.param({'k1': {'k2': {'k3': 'leaf'}}},
                 [(['k1', 'k2', 'k3'], 'leaf')]),
    pytest.param({'k1': {'k2': {'k3': 1, 'l3': 1.2}}},
                 [(['k1', 'k2', 'k3'], 1), (['k1', 'k2', 'l3'], 1.2)]),
    pytest.param({'k1': 'leaf', 'l3': {'k3': None}},
                 [(['k1'], 'leaf'), (['l3', 'k3'], None)]),
    pytest.param(None, []),
])
def test_flattening(obj, expected):
    assert sorted(list(dicts.flatten(obj))) == sorted(expected)
