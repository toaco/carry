from collections import defaultdict

GRAY, BLACK = 0, 1


def topological(graph):
    order, enter, state = [], set(graph), {}

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
