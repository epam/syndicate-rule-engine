import base64
import binascii
from contextlib import contextmanager
from enum import Enum as _Enum
import json
from dateutil.parser import isoparse
from functools import reduce
import io
from itertools import chain, islice
import math
import re
import msgspec
import time
from types import NoneType
from typing import (
    Any,
    BinaryIO,
    Callable,
    Generator,
    Hashable,
    Iterable,
    Iterator,
    Optional,
    TypeVar,
)
from typing_extensions import Self
import uuid

import requests

from helpers.constants import RuleDomain, Cloud
from helpers.log_helper import get_logger


T = TypeVar('T')

_LOG = get_logger(__name__)


class RequestContext:
    __slots__ = ('aws_request_id', 'invoked_function_arn')

    def __init__(self, request_id: str | None = None):
        self.aws_request_id: str = request_id or str(uuid.uuid4())
        self.invoked_function_arn = None

    @staticmethod
    def get_remaining_time_in_millis():
        return math.inf

    @staticmethod
    def extract_account_id(invoked_function_arn: str) -> str:
        return invoked_function_arn.split(':')[4]


def deep_get(dct: dict, path: list | tuple) -> Any:
    """
    >>> d = {'a': {'b': 1}}
    >>> deep_get(d, ('a', 'b'))
    1
    >>> deep_get(d, (1, 'two'))
    None
    """
    return reduce(
        lambda d, key: d.get(key, None) if isinstance(d, dict) else None,
        path,
        dct,
    )


def deep_set(dct: dict, path: tuple, item: Any):
    if len(path) == 1:
        dct[path[0]] = item
    else:
        subdict = dct.get(path[0], None)
        if not isinstance(subdict, dict):
            dct[path[0]] = {}
        deep_set(dct[path[0]], path[1:], item)


def title_keys(item: dict | list) -> dict | list:
    if isinstance(item, dict):
        titled = {}
        for k, v in item.items():
            titled[k[0].upper() + k[1:] if isinstance(k, str) else k] = (
                title_keys(v)
            )
        return titled
    elif isinstance(item, list):
        return [title_keys(i) for i in item]
    return item


def setdefault(obj: object, name: str, value: T) -> T | Any:
    """
    Works like dict.setdefault
    """
    if not hasattr(obj, name):
        setattr(obj, name, value)
    return getattr(obj, name)


def batches(iterable: Iterable, n: int) -> Generator[list, None, None]:
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


def filter_dict(d: dict, keys: set | list | tuple) -> dict:
    if keys:
        return {k: v for k, v in d.items() if k in keys}
    return d


class HashableDict(dict):
    def __hash__(self) -> int:
        if hasattr(self, '__calculated_hash'):
            return getattr(self, '__calculated_hash')
        h = hash(frozenset(self.items()))
        setattr(self, '__calculated_hash', h)
        return h


def hashable(item: dict | list | tuple | set | str | float | int | None):
    """
    Makes hashable from the given item
    >>> d = {'q': [1,3,5, {'h': 34, 'c': ['1', '2']}], 'v': {1: [1,2,3]}}
    >>> d1 = {'v': {1: [1,2,3]}, 'q': [1,3,5, {'h': 34, 'c': ['1', '2']}]}
    >>> hash(hashable(d)) == hash(hashable(d1))
    True
    """
    if isinstance(item, dict):
        return HashableDict(zip(item.keys(), map(hashable, item.values())))
    elif isinstance(item, (tuple, list, set)):
        return tuple(map(hashable, item))
    else:  # str, int, bool, None (all hashable)
        return item


_SENTINEL = object()


def comparable(
    item: dict | list | tuple | set | str | float | int | None,
    *,
    replace_dates_with=_SENTINEL,
):
    """
    >>> d = [{'key1': [1,2,3]}, {'key2': [4,5,6]}]
    >>> d1 = [{'key2': [6,5,4]}, {'key1': [3,2,1]}]
    >>> comparable(d) == comparable(d1)
    Order of items inside inner collections is not important.
    The result of this function cannot be dumped to json without default=list.
    Currently is used primarily for tests.
    """
    if isinstance(item, dict):
        return HashableDict(
            [
                (k, comparable(v, replace_dates_with=replace_dates_with))
                for k, v in item.items()
            ]
        )
    elif isinstance(item, (tuple, list, set)):
        return frozenset(
            [
                comparable(i, replace_dates_with=replace_dates_with)
                for i in item
            ]
        )
    else:
        if replace_dates_with is _SENTINEL:
            return item
        try:
            isoparse(str(item))
            return replace_dates_with
        except ValueError:
            return item


class KeepValueGenerator:
    """
    Wraps the given generator and provides an ability to keep its value
    after the iteration has stopped.
    """

    def __init__(self, generator: Generator):
        self._generator = generator
        self.value = None

    def __iter__(self):
        self.value = yield from self._generator


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class Enum(str, _Enum):
    """
    Should be used as 'choice' parameter in Pydantic models
    """

    @classmethod
    def has(cls, value: str) -> bool:
        """
        Does not work with Standard enum (not inherited from str)
        :param value:
        :return:
        """
        # return value in cls._value2member_map_
        return value in iter(cls)

    @classmethod
    def build(cls, name: str, items: Iterable) -> type['Enum']:
        """
        Values can contain spaces and even "+" or "-"
        :param name:
        :param items:
        :return:
        """
        return cls(name, {v: v for v in items})

    @classmethod
    def iter(cls) -> Iterable:
        """
        Iterates over values, not enum items
        """
        return map(lambda x: x.value, cls)

    @classmethod
    def list(cls) -> list:
        return list(cls.iter())


def adjust_cloud(cloud: str) -> str:
    """
    Backward compatibility. We use GCP everywhere, but Maestro
    Tenants use GOOGLE
    """
    u = cloud.upper()
    if u == Cloud.GOOGLE.value:
        return RuleDomain.GCP.value
    return u


def peek(iterable) -> Optional[tuple[Any, chain]]:
    try:
        first = next(iterable)
    except StopIteration:
        return
    return first, chain([first], iterable)


def urljoin(*args: str | int) -> str:
    """
    Joins all the parts with one "/"
    :param args:
    :return:
    """
    return '/'.join(map(lambda x: str(x).strip('/'), args))


def skip_indexes(
    iterable: Iterable[T], skip: set[int]
) -> Generator[T, None, None]:
    """
    Iterates over the collection skipping specific indexes
    :param iterable:
    :param skip:
    :return:
    """
    it = iter(iterable)
    for i, item in enumerate(it):
        if i in skip:
            continue
        yield item


def catchdefault(method: Callable, default: Any = None):
    """
    Returns method's result. In case it fails -> returns default
    :param method:
    :param default:
    :return:
    """
    try:
        return method()
    except Exception:  # noqa
        return default


class NotHereDescriptor:
    def __get__(self, obj, type=None):
        raise AttributeError


JSON_PATH_LIST_INDEXES = re.compile(r'\w*\[(-?\d+)\]')


def json_path_get(d: dict | list, path: str) -> Any:
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


FT = TypeVar('FT', bound=BinaryIO)  # file type


def download_url(url: str, out: FT | None = None) -> FT | io.BytesIO | None:
    """
    Downloads the content by the url handling compression and other encoding
    in case those are specified in headers
    :param url:
    :param out: temp file opened in binary mode
    :return:
    """
    if not out:
        out = io.BytesIO()
    try:
        with requests.get(url, stream=True) as resp:
            for chunk in resp.raw.stream(decode_content=True):
                out.write(chunk)
        out.seek(0)
        return out
    except Exception:
        _LOG.exception(f'Could not download file from url: {url}')
        return


def sifted(request: dict) -> dict:
    return {
        k: v for k, v in request.items() if isinstance(v, (bool, int)) or v
    }


HT = TypeVar('HT', bound=Hashable)


def without_duplicates(iterable: Iterable[HT]) -> Generator[HT, None, None]:
    """
    Iterates over the collection skipping already yielded items
    :param iterable:
    :return:
    """
    it = iter(iterable)
    buffer = set()
    for i in it:
        if i in buffer:
            continue
        buffer.add(i)
        yield i


CT = TypeVar('CT')


class MultipleCursorsWithOneLimitIterator(Iterable[CT]):
    def __init__(
        self,
        limit: int | None,
        *factories: Callable[[int | None], Iterator[CT]],
    ):
        """
        This class will consider the number of yielded items and won't query
        more. See tests for this thing to understand better
        :param limit: common limit for all the cursors
        :param factories: accepts any number of functions with one argument
        `limit` assuming that if limit is None, - there is no limit. Each
        function must build a cursor (supposedly a DB cursor) with that limit.
        """
        self._limit = limit
        if not factories:
            raise ValueError('Invalid usage: at least one factory should be')
        self._factories = factories

    def __iter__(self) -> Iterator[CT]:
        self._current_limit = self._limit
        self._chain = iter(self._factories)
        factory = next(self._chain)
        self._it = factory(self._current_limit)
        return self

    def __next__(self) -> CT:
        if self._current_limit == 0:
            raise StopIteration
        while True:
            try:
                item = next(self._it)
                if isinstance(self._current_limit, int):
                    self._current_limit -= 1
                return item
            except StopIteration:
                # current cursor came to its end. Making the next one
                factory = next(self._chain)
                self._it = factory(self._current_limit)


JT = TypeVar('JT')  # json type


def _default_hook(x: Any) -> bool:
    return isinstance(x, (str, int, bool, NoneType))


def iter_values(
    finding: JT, hook: Callable[[Any], bool] = _default_hook
) -> Generator[Any, Any, JT]:
    """
    Yields values from the given finding with an ability to send back
    the desired values. I proudly think this is cool, because we can put
    values replacement login outside of this generator
    >>> gen = iter_values({'1':'q', '2': ['w', 'e'], '3': {'4': 'r'}})
    >>> next(gen)
    q
    >>> gen.send('instead of q')
    w
    >>> gen.send('instead of w')
    e
    >>> gen.send('instead of e')
    r
    >>> gen.send('instead of r')
    After the last command StopIteration will be raised, and it
    will contain the changed finding.
    Changes the given finding in-place for performance purposes so be careful
    :param finding:
    :param hook:
    :return:
    """
    if hook(finding):
        new = yield finding
        return new
    elif _default_hook(finding):  # anyway we need this default one
        return finding
    if isinstance(finding, dict):
        for k, v in finding.items():
            finding[k] = yield from iter_values(v, hook)
        return finding
    if isinstance(finding, list):
        for i, v in enumerate(finding):
            finding[i] = yield from iter_values(v, hook)
        return finding


def iter_key_values(
    finding: JT,
    hook: Callable[[Any], bool] = _default_hook,
    keys: tuple[str, ...] = (),
) -> Generator[tuple[tuple[str, ...], Any], Any, JT]:
    """
    Slightly changed version of the function above. Iterates over dict
    and yields tuples where the first element is tuple of keys and the second
    is a value. Skips lists entirely for now.
    """
    if hook(finding):
        new = yield keys, finding
        return new
    elif _default_hook(finding):
        return finding
    if isinstance(finding, dict):
        for k, v in finding.items():
            finding[k] = yield from iter_key_values(v, hook, keys + (k,))
        return finding
    if isinstance(finding, list):
        # TODO: maybe yield list indexes here, but they can be confused
        # with dict keys of type int. Currently, we don't need this block
        return finding


def dereference_json(obj: dict) -> None:
    """
    Changes the given dict in place de-referencing all $ref. Does not support
    files and http references. If you need them, better use jsonref
    lib. Works only for dict as root object.
    Note that it does not create new objects but only replaces {'$ref': ''}
    with objects that ref(s) are referring to, so:
    - works really fast, 20x faster than jsonref, at least relying on my
      benchmarks;
    - changes your existing object;
    - can reference the same object multiple times so changing some arbitrary
      values afterward can change object in multiple places.
    Though, it's perfectly fine in case you need to dereference obj, dump it
    to file and forget
    :param obj:
    :return:
    """

    def _inner(o):
        if isinstance(o, (str, int, float, bool, NoneType)):
            return
        # dict or list
        if isinstance(o, dict):
            for k, v in o.items():
                if isinstance(v, dict) and isinstance(v.get('$ref'), str):
                    _path = v['$ref'].strip('#/').split('/')
                    o[k] = deep_get(obj, _path)
                else:
                    _inner(v)
        else:  # isinstance(o, list)
            for i, v in enumerate(o):
                if isinstance(v, dict) and isinstance(v.get('$ref'), str):
                    _path = v['$ref'].strip('#/').split('/')
                    o[i] = deep_get(obj, _path)
                else:
                    _inner(v)

    _inner(obj)


@contextmanager
def measure_time():
    holder = [time.perf_counter_ns(), None]
    try:
        yield holder
    finally:
        holder[1] = time.perf_counter_ns()


class NextToken:
    __slots__ = ('_lak',)

    def __init__(self, lak: dict | int | str | None = None):
        """
        Wrapper over dynamodb last_evaluated_key and pymongo offset
        :param lak:
        """
        self._lak = lak

    def __json__(self) -> str | None:
        """
        Handled only inside commons.lambda_response
        :return:
        """
        return self.serialize()

    def serialize(self) -> str | None:
        if not self:
            return
        return base64.urlsafe_b64encode(
            msgspec.json.encode(self._lak)
        ).decode()

    @property
    def value(self) -> dict | int | str | None:
        return self._lak

    @classmethod
    def deserialize(cls, s: str | None = None) -> Self:
        if not s or not isinstance(s, str):
            return cls()
        decoded = None
        try:
            decoded = msgspec.json.decode(base64.urlsafe_b64decode(s))
        except (binascii.Error, msgspec.DecodeError):
            pass
        except Exception:  # noqa
            pass
        return cls(decoded)

    def __bool__(self) -> bool:
        return not not self._lak  # 0 and empty dict are None


def flip_dict(d: dict):
    """
    In place
    :param d:
    :return:
    """
    for k in tuple(d.keys()):
        d[d.pop(k)] = k


class Version(tuple):
    """
    Limited version. Additional labels, pre-release labels and build metadata
    are not supported.
    Tuple with three elements (integers): (Major, Minor, Patch).
    Minor and Patch can be missing. It that case they are 0. This class is
    supposed to be used primarily by rulesets versioning
    """

    _not_allowed = re.compile(r'[^.0-9]')

    def __new__(cls, seq: str | tuple[int, int, int] = (0, 0, 0)) -> 'Version':
        if isinstance(seq, Version):
            return seq
        if isinstance(seq, str):
            seq = cls._parse(seq)
        return tuple.__new__(Version, seq)

    @classmethod
    def _parse(cls, version: str) -> tuple[int, int, int]:
        """
        Raises ValueError
        """
        prepared = re.sub(cls._not_allowed, '', version).strip('.')
        items = tuple(map(int, prepared.split('.')))
        match len(items):
            case 3:
                return items
            case 2:
                return items[0], items[1], 0
            case 1:
                return items[0], 0, 0
            case _:
                raise ValueError(
                    f'Cannot parse. Version must have one '
                    f'of formats: 1, 2.3, 4.5.6'
                )

    @property
    def major(self) -> int:
        return self[0]

    @property
    def minor(self) -> int:
        return self[1]

    @property
    def patch(self) -> int | None:
        return self[2]

    @classmethod
    def first_version(cls) -> 'Version':
        return cls((1, 0, 0))

    def to_str(self) -> str:
        return '.'.join(map(str, self))

    def __str__(self) -> str:
        return self.to_str()

    def next_major(self) -> 'Version':
        return Version((self.major + 1, 0, 0))

    def next_minor(self) -> 'Version':
        return Version((self.major, self.minor + 1, 0))

    def next_patch(self) -> 'Version':
        return Version((self.major, self.minor, self.patch + 1))


class JWTToken:
    """
    A simple wrapper over jwt token
    """

    EXP_THRESHOLD = 300  # in seconds
    __slots__ = '_token', '_exp_threshold'

    def __init__(self, token: str, exp_threshold: int = EXP_THRESHOLD):
        self._token = token
        self._exp_threshold = exp_threshold

    @property
    def raw(self) -> str:
        return self._token

    @property
    def payload(self) -> dict | None:
        try:
            return json.loads(
                base64.b64decode(self._token.split('.')[1] + '==').decode()
            )
        except Exception:
            return

    def is_expired(self) -> bool:
        p = self.payload
        if not p:
            return True
        exp = p.get('exp')
        if not exp:
            return False
        return exp < time.time() + self._exp_threshold


def group_by(
    it: Iterable[T], key: Callable[[T], Hashable]
) -> dict[Hashable, list[T]]:
    res = {}
    for item in it:
        res.setdefault(key(item), []).append(item)
    return res


def map_by(
    it: Iterable[T], key: Callable[[T], Hashable]
) -> dict[Hashable, T]:
    res = {}
    for item in it:
        res.setdefault(key(item), item)
    return res
