import collections.abc
import functools
import json
import uuid
from enum import Enum as _Enum
from functools import reduce
from http import HTTPStatus
from itertools import islice, chain
from typing import Dict, Type, Any, Union, TypeVar, Optional, List, Tuple, \
    Iterable, Generator, Callable, Hashable
from uuid import uuid4

from helpers.constants import PARAM_MESSAGE, PARAM_ITEMS, PARAM_TRACE_ID, \
    GOOGLE_CLOUD_ATTR, GCP_CLOUD_ATTR
from helpers.exception import CustodianException
from helpers.log_helper import get_logger

T = TypeVar('T')

_LOG = get_logger(__name__)

PARAM_USER_ID = 'user_id'

LINE_SEP = '/'
REPO_DYNAMODB_ROOT = 'dynamodb'
REPO_S3_ROOT = 's3'

REPO_ROLES_FOLDER = 'Roles'
REPO_POLICIES_FOLDER = 'Policies'
REPO_SETTINGS_FOLDER = 'Settings'
REPO_LICENSES_FOLDER = 'Licenses'
REPO_SIEM_FOLDER = 'SIEMManager'

REPO_SETTINGS_PATH = LINE_SEP.join((REPO_DYNAMODB_ROOT, REPO_SETTINGS_FOLDER))

STATUS_READY_TO_SCAN = 'READY_TO_SCAN'

BAD_REQUEST_IMPROPER_TYPES = 'Bad Request. The following parameters ' \
                             'don\'t adhere to the respective types: {0}'
BAD_REQUEST_MISSING_PARAMETERS = 'Bad Request. The following parameters ' \
                                 'are missing: {0}'


def build_response(content: Optional[Union[str, dict, list, Iterable]] = None,
                   code: int = HTTPStatus.OK.value,
                   meta: Optional[dict] = None):
    context = _import_request_context()
    meta = meta or {}
    _body = {
        PARAM_TRACE_ID: context.aws_request_id,
        **meta
    }
    if isinstance(content, str):
        _body.update({PARAM_MESSAGE: content})
    elif isinstance(content, dict) and content:
        _body.update({PARAM_ITEMS: [content, ]})
    elif isinstance(content, list):
        _body.update({PARAM_ITEMS: content})
    elif isinstance(content, Iterable):
        _body.update({PARAM_ITEMS: list(content)})
    else:
        _body.update({PARAM_ITEMS: []})

    if 200 <= code <= 207:
        return {
            'statusCode': code,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': '*'
            },
            'isBase64Encoded': False,
            'multiValueHeaders': {},
            'body': json.dumps(_body, sort_keys=True, separators=(',', ':'))
        }
    raise CustodianException(
        code=code,
        content=content
    )


class RequestContext:
    def __init__(self, request_id: str = None):
        self.aws_request_id: str = request_id or str(uuid.uuid4())


def _import_request_context():
    """Imports request_context global variable from abstract_api_handler_lambda
    and abstract_lambda. Only one of them will be initialized, but here we
    cannot know which will. So just try"""
    from services.abstract_api_handler_lambda import REQUEST_CONTEXT as first
    from services.abstract_lambda import REQUEST_CONTEXT as second
    if not first and not second:
        _LOG.warning('NO REQUEST CONTEXT WAS FOUND.')
        return RequestContext('Custom trace_id')
    return first if first else second


def raise_error_response(code, content):
    raise CustodianException(code=code, content=content)


def get_invalid_parameter_types(
        event: Dict, required_param_types: Dict[str, Type[Any]]
) -> Dict:
    _missing = {}
    for param, _type in required_param_types.items():
        data = event.get(param, None)
        if data is None or not isinstance(data, _type):
            _missing[param] = _type
    return _missing


def retrieve_invalid_parameter_types(
        event: Dict, required_param_types: Dict[str, Type[Any]]
) -> Union[str, Type[None]]:
    """
    Checks if all required parameters are given in lambda payload,
    and follow each respective type.
    :param event: the lambda payload
    :param required_param_types: list of the lambda required parameters
    :return: Union[str, Type[None]]
    """
    _missing = get_invalid_parameter_types(event, required_param_types)
    if _missing:
        _items = _missing.items()
        _missing = ', '.join(f'{k}:{v.__name__}' for k, v in _items)
    return BAD_REQUEST_IMPROPER_TYPES.format(_missing) if _missing else None


def get_missing_parameters(event, required_params_list):
    missing_params_list = []
    for param in required_params_list:
        if not event.get(param):
            missing_params_list.append(param)
    return missing_params_list


def validate_params(event, required_params_list):
    """
    Checks if all required parameters present in lambda payload.
    :param event: the lambda payload
    :param required_params_list: list of the lambda required parameters
    :return: bad request response if some parameter[s] is/are missing,
        otherwise - none
    """
    missing_params_list = get_missing_parameters(event, required_params_list)

    if missing_params_list:
        raise_error_response(
            HTTPStatus.BAD_REQUEST.value,
            BAD_REQUEST_MISSING_PARAMETERS.format(missing_params_list)
        )


def deep_update(source, overrides):
    """
    Update a nested dictionary or similar mapping, with extending
    inner list by unique items only.
    """
    source = source.copy()
    for key, value in overrides.items():
        if isinstance(value, collections.abc.Mapping) and value:
            returned = deep_update(source.get(key, {}), value)
            source[key] = returned
        elif isinstance(value, list) and \
                isinstance(source.get(key), list) and value:
            for item in value:
                if item not in source[key]:
                    source[key].append(item)
        else:
            source[key] = overrides[key]
    return source


def generate_id():
    return str(uuid4())


def trim_milliseconds_from_iso_string(iso_string):
    try:
        index = iso_string.index('.')
        return iso_string[:index]
    except:
        return iso_string


def deep_get(dct: dict, path: Union[list, tuple]) -> Any:
    """
    >>> d = {'a': {'b': 1}}
    >>> deep_get(d, ('a', 'b'))
    1
    >>> deep_get(d, (1, 'two'))
    None
    """
    return reduce(
        lambda d, key: d.get(key, None) if isinstance(d, dict) else None,
        path, dct)


def deep_set(dct: dict, path: tuple, item: Any):
    if len(path) == 1:
        dct[path[0]] = item
    else:
        subdict = dct.get(path[0], None)
        if not isinstance(subdict, dict):
            dct[path[0]] = {}
        deep_set(dct[path[0]], path[1:], item)


def remove_duplicated_dicts(lst: List[Dict]) -> List[Dict]:
    return list(
        {json.dumps(dct, sort_keys=True): dct for dct in lst}.values()
    )


def title_keys(item: Union[dict, list]) -> Union[dict, list]:
    if isinstance(item, dict):
        titled = {}
        for k, v in item.items():
            titled[k[0].upper() + k[1:] if isinstance(k, str) else k] = \
                title_keys(v)
        return titled
    elif isinstance(item, list):
        titled = []
        for i in item:
            titled.append(title_keys(i))
        return titled
    return item


def setdefault(obj: object, name: str, value: T) -> Union[T, Any]:
    """
    Works like dict.setdefault
    """
    if not hasattr(obj, name):
        setattr(obj, name, value)
    return getattr(obj, name)


def list_update(target_list: List[T], source_list: List[T],
                update_by: Tuple[str, ...]) -> List[T]:
    """
    Updates objects in target_list from source_list by specified attributes.
    """
    _make_dict = lambda _list, _attrs: {
        tuple(getattr(obj, attr, None) for attr in _attrs): obj
        for obj in _list
    }
    target = _make_dict(target_list, update_by)
    source = _make_dict(source_list, update_by)
    target.update(source)
    return list(target.values())


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


def filter_dict(d: dict, keys: Union[set, tuple, list]) -> dict:
    if keys:
        return {k: v for k, v in d.items() if k in keys}
    return d


class HashableDict(dict):
    def __hash__(self):
        return hash(frozenset(self.items()))


def hashable(item: Union[dict, list, str, float, int, type(None)]
             ) -> Hashable:
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


def keep_value(func: Callable):
    """
    Decorator that allows to keep generator's value in `value` attribute
    :param func: function thar returns generator obj
    :return: function that returns the wrapped generator
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return KeepValueGenerator(func(*args, **kwargs))

    return wrapper


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
    def build(cls, name: str, items: Iterable) -> Type['Enum']:
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


def nested_items(item: Union[dict, list, str, float, int, type(None)]
                 ) -> Generator[Tuple[str, Any], None, bool]:
    """
    Recursively iterates over nested key-values
    >>> d = {'key': 'value', 'key2': [{1: 2, 3: 4, 'k': 'v'}, {5: 6}, 1, 2, 3]}
    >>> print(list(nested_items(d)))
    [('key', 'value'), (1, 2), (3, 4), ('k', 'v'), (5, 6)]
    :param item:
    :return:
    """
    if isinstance(item, (str, float, int, type(None))):
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


def peek(iterable) -> Optional[Tuple[Any, chain]]:
    try:
        first = next(iterable)
    except StopIteration:
        return
    return first, chain([first], iterable)
