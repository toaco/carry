from __future__ import unicode_literals

from collections import defaultdict


def topological(graph):
    order, enter, state = [], set(graph), {}
    GRAY, BLACK = 0, 1

    def dfs(node):
        state[node] = GRAY
        for k in graph.get(node, ()):
            sk = state.get(k, None)
            if sk == GRAY:
                raise ValueError("cycle")
            if sk == BLACK:
                continue
            enter.discard(k)
            dfs(k)
        order.append(node)
        state[node] = BLACK

    while enter: dfs(enter.pop())
    return order


def topological_for_edge_set_array(relations):
    """
    :param relations: [(main,ref)]
    """
    graph = defaultdict(list)
    for main, ref in relations:
        graph[main].append(ref)
    order = topological(graph)
    return order


class DefaultDict(object):
    """
    >>> class RDBPutConfig(DefaultDict):
            default = {
                'if_exists': 'append',
                'index': False,
                'chunksize': 5000
            }
    >>> RDBPutConfig({12: 2})
    {'if_exists': 'append', 12: 2, 'chunksize': 5000, 'index': False}
    >>> RDBPutConfig({'index': 3})
    {'if_exists': 'append', 'chunksize': 5000, 'index': 3}
    >>> RDBPutConfig({})
    {'if_exists': 'append', 'chunksize': 5000, 'index': False}
    >>> RDBPutConfig()
    {'if_exists': 'append', 'chunksize': 5000, 'index': False}
    """

    def __new__(cls, dict_=None, default=None):
        if dict_ is None:
            dict_ = {}
        else:
            assert isinstance(dict_, dict)

        if default is not None:
            assert isinstance(default, dict)
        else:
            if hasattr(cls, 'default'):
                if isinstance(cls.default, dict):
                    default = cls.default
                else:
                    raise TypeError
            else:
                default = {}
        extended = default.copy()
        extended.update(dict_)
        return extended


def topological_find(graph, auto_delete=False):
    result, enter, state = set(), set(graph), {}
    GRAY, BLACK = 0, 1

    def dfs(node):
        state[node] = GRAY
        dependency = graph.get(node, ())
        if not dependency:
            result.add(node)
        else:
            for k in dependency:
                sk = state.get(k, None)
                if sk == GRAY:
                    raise ValueError("cycle")
                if sk == BLACK:
                    continue
                enter.discard(k)
                dfs(k)
        state[node] = BLACK

    while enter: dfs(enter.pop())

    if auto_delete:
        for r in result:
            if r in graph:
                del graph[r]
            for gv in graph.values():
                if gv and r in gv:
                    gv.remove(r)

    return result


def topological_remove(graph, node):
    del graph[node]

    def dfs(node):
        removed = []
        for g in graph:
            dependency = graph.get(g, ())
            if node in dependency:
                removed.append(g)
        for r in removed:
            del graph[r]
        for r in removed:
            dfs(r)

    dfs(node)

    return graph
