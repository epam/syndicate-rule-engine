import base64
import binascii
from contextlib import contextmanager
from enum import Enum as _Enum
import json
import functools
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
    TYPE_CHECKING
)
from typing_extensions import Self
import uuid

import requests

from helpers.constants import GCP_CLOUD_ATTR, GOOGLE_CLOUD_ATTR
from helpers.log_helper import get_logger
if TYPE_CHECKING:
    from services.abs_lambda import ProcessedEvent

T = TypeVar('T')

_LOG = get_logger(__name__)

PARAM_USER_ID = 'user_id'


BAD_REQUEST_MISSING_PARAMETERS = 'Bad Request. The following parameters ' \
                                 'are missing: {0}'


class RequestContext:
    __slots__ = ('aws_request_id', 'invoked_function_arn')

    def __init__(self, request_id: str | None = None):
        self.aws_request_id: str = request_id or str(uuid.uuid4())
        self.invoked_function_arn = None

    @staticmethod
    def get_remaining_time_in_millis():
        return math.inf


def get_missing_parameters(event, required_params_list):
    missing_params_list = []
    for param in required_params_list:
        if not event.get(param):
            missing_params_list.append(param)
    return missing_params_list


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
        path, dct
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
            titled[k[0].upper() + k[1:] if isinstance(k, str) else k] = \
                title_keys(v)
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
        return hash(frozenset(self.items()))


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
    return GCP_CLOUD_ATTR if cloud.upper() == GOOGLE_CLOUD_ATTR else cloud.upper()


def coroutine(func):
    @functools.wraps(func)
    def start(*args, **kwargs):
        gen = func(*args, **kwargs)
        next(gen)
        return gen

    return start


def nested_items(item: dict | list | str | float | int | None
                 ) -> Generator[tuple[str, Any], None, bool]:
    """
    Recursively iterates over nested key-values
    >>> d = {'key': 'value', 'key2': [{1: 2, 3: 4, 'k': 'v'}, {5: 6}, 1, 2, 3]}
    >>> print(list(nested_items(d)))
    [('key', 'value'), (1, 2), (3, 4), ('k', 'v'), (5, 6)]
    :param item:
    :return:
    """
    if isinstance(item, (str, float, int, NoneType)):
        return False  # means not iterated, because it's a leaf
    # list or dict -> can be iterated over
    if isinstance(item, dict):
        for k, v in item.items():
            # if iterated over nested, we don't need to yield it
            if not (yield from nested_items(v)):
                yield k, v
    else:  # isinstance(item, list)
        for i in item:
            yield from nested_items(i)
    return True


def peek(iterable) -> Optional[tuple[Any, chain]]:
    try:
        first = next(iterable)
    except StopIteration:
        return
    return first, chain([first], iterable)


def urljoin(*args: str) -> str:
    """
    Joins all the parts with one "/"
    :param args:
    :return:
    """
    return '/'.join(map(lambda x: str(x).strip('/'), args))


def skip_indexes(iterable: Iterable[T], skip: set[int]
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


def get_last_element(string: str, delimiter: str) -> str:
    return string.split(delimiter)[-1]


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
        k: v for k, v in request.items()
        if isinstance(v, (bool, int)) or v
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
    def __init__(self, limit: int | None,
                 *factories: Callable[[int | None], Iterator[CT]]):
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


# todo test
def to_api_gateway_event(processed_event: 'ProcessedEvent') -> dict:
    """
    Converts our ProcessedEvent back to api gateway event. It does not
    contain all the fields only some that are necessary for high level reports
    endpoints
    :param processed_event:
    :return:
    """
    assert processed_event['resource'], \
        'Only event with existing resource supported'

    # values can be just strings in case we get this event from a database
    resource = processed_event['resource']
    if isinstance(resource, Enum):
        resource = resource.value
    method = processed_event['method']
    if isinstance(method, Enum):
        method = method.value
    return {
        'resource': resource,
        'path': resource,
        'httpMethod': method,
        'headers': {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip,deflate',
        },
        'multiValueHeaders': {
            'Accept': ['application/json'],
            'Accept-Encoding': ['gzip,deflate'],
            'Content-Type': ['application/json'],
        },
        'queryStringParameters': processed_event['query'],
        'pathParameters': processed_event['path_params'],
        'requestContext': {
            'path': processed_event['fullpath'],
            'resourcePath': resource,
            'httpMethod': method,
            'requestTimeEpoch': time.time() * 1e3,
            'protocol': 'HTTP/1.1',
            'authorizer': {
                'claims': {
                    'sub': processed_event['cognito_user_id'],
                    'custom:customer': processed_event['cognito_customer'],
                    'cognito:username': processed_event['cognito_username'],
                    'custom:role': processed_event['cognito_user_role']
                }
            }
        },
        'body': json.dumps(processed_event['body'], separators=(',', ':')),
        'isBase64Encoded': False
    }


JT = TypeVar('JT')  # json type
IT = TypeVar('IT')  # item type


def _default_hook(x):
    return isinstance(x, (str, int, bool, NoneType))


def iter_values(finding: JT,
                hook: Callable[[IT], bool] = _default_hook
                ) -> Generator[IT, Any, JT]:
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
        return base64.urlsafe_b64encode(msgspec.json.encode(self._lak)).decode()

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


class TermColor:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    DEBUG = '\033[90m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

    @classmethod
    def blue(cls, st: str) -> str:
        return f'{cls.OKBLUE}{st}{cls.ENDC}'

    @classmethod
    def cyan(cls, st: str) -> str:
        return f'{cls.OKCYAN}{st}{cls.ENDC}'

    @classmethod
    def green(cls, st: str) -> str:
        return f'{cls.OKGREEN}{st}{cls.ENDC}'

    @classmethod
    def yellow(cls, st: str) -> str:
        return f'{cls.WARNING}{st}{cls.ENDC}'

    @classmethod
    def red(cls, st: str) -> str:
        return f'{cls.FAIL}{st}{cls.ENDC}'

    @classmethod
    def gray(cls, st: str) -> str:
        return f'{cls.DEBUG}{st}{cls.DEBUG}'


def flip_dict(d: dict):
    """
    In place
    :param d:
    :return:
    """
    for k in tuple(d.keys()):
        d[d.pop(k)] = k
