import re
from functools import reduce
from itertools import islice
from typing import Union, Generator, Iterable, List, Optional, Any


def filter_dict(d: dict, keys: set) -> dict:
    if keys:
        return {k: v for k, v in d.items() if k in keys}
    return d


class HashableDict(dict):
    def __hash__(self):
        return hash(frozenset(self.items()))


def hashable(item: Union[dict, list, str, float, int, type(None)]):
    """Makes hashable from the given item
    >>> d = {'q': [1,3,5, {'h': 34, 'c': ['1', '2']}], 'v': {1: [1,2,3]}}
    >>> d1 = {'v': {1: [1,2,3]}, 'q': [1,3,5, {'h': 34, 'c': ['1', '2']}]}
    >>> hash(hashable(d)) == hash(hashable(d1))
    True
    """
    if isinstance(item, dict):
        h_dict = HashableDict()
        for k, v in item.items():
            h_dict[k] = hashable(v)
        return h_dict
    elif isinstance(item, list):
        h_list = []
        for i in item:
            h_list.append(hashable(i))
        return tuple(h_list)
    else:  # str, int, bool, None (all hashable)
        return item


def deep_get(dct, path):
    return reduce(
        lambda d, key: d.get(key, None) if isinstance(d, dict) else None,
        path, dct)


def batches(iterable: Iterable, n: int) -> Generator[List, None, None]:
    """
    Batch data into lists of length n. The last batch may be shorter.
    """
    if n < 1:
        raise ValueError('n must be >= 1')
    it = iter(iterable)
    batch = list(islice(it, n))
    while batch:
        yield batch
        batch = list(islice(it, n))


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


JSON_PATH_LIST_INDEXES = re.compile(r'\w*\[(-?\d+)\]')


def json_path_get(d: Union[dict, list], path: Optional[str] = None) -> Any:
    """
    Simple json paths with only basic operations supported
    >>> json_path_get({'a': 'b', 'c': [1,2,3, [{'b': 'c'}]]}, 'c[-1][0].b')
    'c'
    >>> json_path_get([-1, {'one': 'two'}], 'c[-1][0].b') is None
    True
    >>> json_path_get([-1, {'one': 'two'}], '[-1].one')
    'two'
    """
    if path.startswith('$'):
        path = path[1:]
    if path.startswith('.'):
        path = path[1:]
    parts = path.split('.')

    item = d
    for part in parts:
        try:
            _key = part.split('[')[0]
            _indexes = re.findall(JSON_PATH_LIST_INDEXES, part)
            if _key:
                item = item.get(_key)
            for i in _indexes:
                item = item[int(i)]
        except (IndexError, TypeError, AttributeError):
            item = None
            break
    return item
