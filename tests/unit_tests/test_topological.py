from carry.utils import topological_find, topological_remove


def test_topological_find_simple():
    graph1 = {
        'a': ['b', 'c', 'd'],
        'b': [],
        'c': ['d'],
        'd': []
    }
    assert topological_find(graph1, True) == {'b', 'd'}
    assert topological_find(graph1, True) == {'c'}
    assert topological_find(graph1, True) == {'a'}
    assert topological_find(graph1, True) == set()


def test_topological_find_2_components():
    graph2 = {
        'a': ['b', 'c', 'd'],
        'b': [],
        'c': ['d'],
        'd': [],
        'e': ['g', 'f', 'q'],
        'g': [],
        'f': [],
        'q': []
    }
    assert topological_find(graph2, True) == {'b', 'd', 'g', 'f', 'q'}
    assert topological_find(graph2, True) == {'c', 'e'}
    assert topological_find(graph2, True) == {'a'}
    assert topological_find(graph2, True) == set()


def test_topological_remove():
    graph2 = {
        'a': ['b'],
        'b': ['c', 'e'],
        'c': [],
        'e': ['f']
    }
    assert topological_remove(graph2, 'c') == {
        'e': ['f']
    }


def test_topological_remove2():
    graph2 = {
        'a': ['b', 'c'],
        'c': ['d'],
    }
    assert topological_remove(graph2, 'c') == {}
