import io
import json
from itertools import islice

import pytest

from helpers import (
    deep_get,
    deep_set,
    title_keys,
    setdefault,
    filter_dict,
    hashable,
    urljoin,
    skip_indexes,
    peek,
    without_duplicates,
    MultipleCursorsWithOneLimitIterator,
    catchdefault,
    batches,
    dereference_json,
    NextToken,
    iter_values,
    flip_dict,
    Version,
    comparable,
    iter_key_values,
    group_by,
)


@pytest.fixture
def dictionary() -> dict:
    """
    Generates some more or less complex dictionary to test related method on
    :return:
    """
    return {
        'one': 'two',
        3: {'four': 'five'},
        'six': {'seven': {'eight': 'nine'}},
        'ten': [{'eleven': 'twelve'}],
    }


def test_deep_get(dictionary):
    assert deep_get(dictionary, ('one',)) == 'two'
    assert deep_get(dictionary, (3, 'four')) == 'five'
    assert deep_get(dictionary, ('six', 'seven', 'eight')) == 'nine'
    assert deep_get(dictionary, ('six', 'seven', 'ten')) is None


def test_deep_set():
    a = {}
    deep_set(a, ('one', 'two', 'three'), 'candy')
    deep_set(a, (1, 2, 3), 'candy')
    assert a['one']['two']['three'] == 'candy'
    assert a[1][2][3] == 'candy'


def test_batches():
    a = [1, 2, 3, 4, 5]
    it = batches(a, 2)
    assert next(it) == [1, 2]
    assert next(it) == [3, 4]
    assert next(it) == [5]
    with pytest.raises(StopIteration):
        next(it)


def test_title_keys(dictionary):
    assert title_keys(dictionary) == {
        'One': 'two',
        3: {'Four': 'five'},
        'Six': {'Seven': {'Eight': 'nine'}},
        'Ten': [{'Eleven': 'twelve'}],
    }


def test_setdefault():
    class Test:
        pass

    instance = Test()
    assert not hasattr(instance, 'attr')
    setdefault(instance, 'attr', 1)
    assert instance.attr == 1
    setdefault(instance, 'attr', 2)
    assert instance.attr == 1


def test_filter_dict(dictionary):
    assert filter_dict(dictionary, ()) == dictionary
    assert filter_dict(dictionary, ('one', 3)) == {
        'one': 'two',
        3: {'four': 'five'},
    }


def test_hashable(dictionary):
    assert hashable(dictionary)
    d = {'q': [1, 3, 5, {'h': 34, 'c': ['1', '2']}], 'v': {1: [1, 2, 3]}}
    d1 = {'v': {1: [1, 2, 3]}, 'q': [1, 3, 5, {'h': 34, 'c': ['1', '2']}]}
    assert hash(hashable(d)) == hash(hashable(d1))
    d = {'q': [1, 2, 3]}
    d1 = {'Q': [1, 2, 3]}
    assert hash(hashable(d)) != hash(hashable(d1))
    d = {'q': [1, 2, 3]}
    d1 = {'q': [3, 2, 1]}
    assert hash(hashable(d)) != hash(hashable(d1))


def test_hashable_json_serializable(dictionary):
    json.dumps(dictionary)


def test_comparable():
    d = {'key1': [1, 2, 3, {'k': 'v'}], 'key2': {'k': 'v'}}
    d1 = {'key2': {'k': 'v'}, 'key1': [1, {'k': 'v'}, 2, 3]}
    assert comparable(d) == comparable(d1)
    assert hash(comparable(d)) == hash(comparable(d1))

    d = [1, {'key1': [1, {'k': '2024-11-06T12:14:02.881343'}]}]
    d1 = [{'key1': [{'k': '2021-10-02T11:30:02.881343'}, 1]}, 1]
    assert comparable(d) != comparable(d1)
    assert comparable(d, replace_dates_with=None) == comparable(
        d1, replace_dates_with=None
    )


def test_urljoin():
    assert urljoin('one', 'two', 'three') == 'one/two/three'
    assert urljoin('/one/', '/two/', '/three/') == 'one/two/three'
    assert urljoin('/one', 'two', 'three/') == 'one/two/three'
    assert urljoin('/one/') == 'one'


def test_skip_indexes():
    a = [0, 1, 2, 3, 4, 5, 6, 7]
    assert list(skip_indexes(a, {0, 2, 4})) == [1, 3, 5, 6, 7]
    assert list(skip_indexes(a, {-1, 0, 7, 8})) == [1, 2, 3, 4, 5, 6]


def test_peek():
    def make_it(data, chunk_size):
        while True:
            block = data.read(chunk_size)
            if not block:
                break
            yield block

    it = make_it(io.BytesIO(b'some data'), 4)

    start, it = peek(it)
    assert start == b'some'
    assert list(it) == [b'some', b' dat', b'a']


def test_without_duplicates():
    assert [1, 2, 3, 4] == list(without_duplicates([1, 1, 2, 1, 1, 3, 4, 4]))
    assert [1] == list(without_duplicates([1, 1, 1, 1, 1, 1, 1]))
    assert [] == list(without_duplicates([]))


def test_catchdefault():
    def method():
        raise Exception

    def method1():
        return 'value'

    assert catchdefault(method) is None
    assert catchdefault(method, 'default') == 'default'
    assert catchdefault(method1) == 'value'
    assert catchdefault(method1, 'default') == 'value'


def test_multiple_cursors_with_one_limit_it():
    def build_f(it):
        """Builds a factory that builds a cursor"""

        def f(lim):
            return islice(it, lim)

        return f

    it1 = iter(range(3))
    it2 = iter(range(2))
    it3 = iter(range(3))
    f1 = build_f(it1)
    f2 = build_f(it2)
    f3 = build_f(it3)
    it = MultipleCursorsWithOneLimitIterator(2, f1, f2, f3)
    assert list(it) == [0, 1]
    assert list(it1) == [2]
    assert list(it2) == [0, 1]
    assert list(it3) == [0, 1, 2]

    it1 = iter(range(3))
    it2 = iter(range(2))
    it3 = iter(range(3))
    f1 = build_f(it1)
    f2 = build_f(it2)
    f3 = build_f(it3)
    it = MultipleCursorsWithOneLimitIterator(0, f1, f2, f3)
    assert list(it) == []
    assert list(it1) == [0, 1, 2]
    assert list(it2) == [0, 1]
    assert list(it3) == [0, 1, 2]

    it1 = iter(range(3))
    it2 = iter(range(2))
    it3 = iter(range(3))
    f1 = build_f(it1)
    f2 = build_f(it2)
    f3 = build_f(it3)
    it = MultipleCursorsWithOneLimitIterator(None, f1, f2, f3)
    assert list(it) == [0, 1, 2, 0, 1, 0, 1, 2]
    assert list(it1) == []
    assert list(it2) == []
    assert list(it3) == []

    it1 = iter(range(3))
    it2 = iter(range(2))
    it3 = iter(range(3))
    f1 = build_f(it1)
    f2 = build_f(it2)
    f3 = build_f(it3)
    it = MultipleCursorsWithOneLimitIterator(5, f1, f2, f3)
    assert list(it) == [0, 1, 2, 0, 1]
    assert list(it1) == []
    assert list(it2) == []
    assert list(it3) == [0, 1, 2]


def test_dereference_json():
    obj = {
        'key1': {'a': [1, 2, 3], 'b': {'$ref': '#/key3'}},
        'key2': {'$ref': '#/key1'},
        'key3': 10,
    }
    dereference_json(obj)
    # obj = jsonref.replace_refs(obj, lazy_load=False)
    assert obj == {
        'key1': {'a': [1, 2, 3], 'b': 10},
        'key2': {'a': [1, 2, 3], 'b': 10},
        'key3': 10,
    }

    obj = {
        'key0': {'$ref': '#/key1/value'},
        'key1': {'value': 1, 'test': {'$ref': '#/key2'}},
        'key2': [1, 2, 3],
        'key4': [1, 2, {'test': {'$ref': '#/key0'}}],
    }
    dereference_json(obj)
    # obj = jsonref.replace_refs(obj, lazy_load=False)
    assert obj == {
        'key0': 1,
        'key1': {'value': 1, 'test': [1, 2, 3]},
        'key2': [1, 2, 3],
        'key4': [1, 2, {'test': 1}],
    }


class TestNextToken:
    def test_serialize_deserialize(self):
        lak = {'key': 'value'}
        assert NextToken.deserialize(NextToken(lak).serialize()).value == lak
        lak = 5
        assert NextToken.deserialize(NextToken(lak).serialize()).value == lak

    def test_bool(self):
        assert NextToken({'key': 'value'})
        assert NextToken(5)
        assert not NextToken({})
        assert not NextToken(None)
        assert not NextToken(0)

    def test_json_dumps(self):
        def default(obj):
            if hasattr(obj, '__json__'):
                return obj.__json__()
            raise TypeError

        nt = NextToken({'key': 'value'})
        assert json.dumps({'next_token': nt}, default=default)


def test_iter_values():
    item = {'key1': [1, 2, 3], 'key2': {'key3': 4}}
    gen = iter_values(item)
    try:
        real = next(gen)
        while True:
            real = gen.send(real**2)
    except StopIteration:
        pass
    assert item == {'key1': [1, 4, 9], 'key2': {'key3': 16}}


def test_iter_key_values(dictionary):
    gen = iter_key_values(dictionary)
    try:
        _, real = next(gen)
        while True:
            _, real = gen.send(real + real)
    except StopIteration:
        pass
    assert dictionary == {
        'one': 'twotwo',
        3: {'four': 'fivefive'},
        'six': {'seven': {'eight': 'ninenine'}},
        'ten': [{'eleven': 'twelve'}],
    }
    gen = iter_key_values(dictionary)
    assert next(gen) == (('one',), 'twotwo')
    assert gen.send(0) == ((3, 'four'), 'fivefive')
    assert gen.send(0) == (('six', 'seven', 'eight'), 'ninenine')
    with pytest.raises(StopIteration):
        _, _ = gen.send(0)
    assert dictionary == {
        'one': 0,
        3: {'four': 0},
        'six': {'seven': {'eight': 0}},
        'ten': [{'eleven': 'twelve'}],
    }


def test_flip_dict():
    def gen_mirrored_dicts() -> tuple[dict, dict]:
        return (
            {f'{i}': i for i in range(1000)},
            {i: f'{i}' for i in range(1000)},
        )

    d1, d2 = gen_mirrored_dicts()
    assert d1 != d2
    flip_dict(d1)
    assert d1 == d2


def test_version():
    assert Version().to_str() == '0.0.0'

    ver = Version('2.4.7')
    assert ver.major == 2
    assert ver.minor == 4
    assert ver.patch == 7

    assert Version.first_version().to_str() == '1.0.0'

    ver = Version('1.2.3')
    assert ver.next_major().to_str() == '2.0.0'
    assert ver.next_minor().to_str() == '1.3.0'
    assert ver.next_patch().to_str() == '1.2.4'

    with pytest.raises(ValueError):
        Version('not a version')

    assert Version('ffsd3.55.1fsd').to_str() == '3.55.1'

    ver = Version('1.2.3')
    assert Version(ver) is ver


def test_group_by():
    assert group_by(range(10), key=lambda n: n % 2 == 0) == {
        True: [0, 2, 4, 6, 8],
        False: [1, 3, 5, 7, 9],
    }

    o1, o2, o3, o4 = (
        {'k': 1, 'd': 1},
        {'k': 1, 'd': 2},
        {'k': 2, 'd': 3},
        {'k': 3, 'd': 4},
    )
    groupped = group_by((o1, o2, o3, o4), key=lambda o: o['k'])
    assert o1 in groupped[1]
    assert o2 in groupped[1]
    assert o3 in groupped[2]
    assert o4 in groupped[3]
