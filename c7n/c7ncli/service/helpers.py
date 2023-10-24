import secrets
import string
from copy import deepcopy
from datetime import datetime, timezone
from re import compile
from typing import Callable, Dict, Union, List, Iterable
from typing import Optional
from uuid import uuid4
import requests

from dateutil.parser import isoparse


def gen_password(digits: int = 20) -> str:
    allowed_punctuation = ''.join(set(string.punctuation) - {'"', "'", "!"})
    chars = string.ascii_letters + string.digits + allowed_punctuation
    while True:
        password = ''.join(secrets.choice(chars) for _ in range(digits)) + '='
        if (any(c.islower() for c in password)
                and any(c.isupper() for c in password)
                and sum(c.isdigit() for c in password) >= 3):
            break
    return password


def cast_to_list(input):
    if type(input) == tuple:
        list_item = list(input)
    elif type(input) == str:
        list_item = [input]
    else:
        list_item = input
    return list_item


def cast_to_dict(payload: str, null_key: str = None, deep: bool = False) -> \
        Dict[str, Union[Dict, List[str]]]:
    pattern, delimiter = compile(r'([^:]*):([^\s]*)(?=,|$)'), ','
    casted: dict = dict()
    for match in pattern.finditer(payload):
        key, value = match.groups()
        key = key or null_key
        if deep and value:
            value = cast_to_dict(payload=value, deep=deep) or value
        value = value.split(delimiter) if isinstance(value, str) else value

        value = [*filter(bool, value)] if isinstance(value, list) else value

        default = dict() if deep else list()
        reference: Union[List, Dict] = casted.setdefault(key, default)

        if isinstance(value, dict):
            reference.update(value)
        elif not deep and value:
            reference.extend(value)

    return casted


def merge_dict(resolver: Callable, default: Callable, *subjects):
    merged = dict()
    for each in subjects:
        copied: dict = deepcopy(each)
        for key, value in copied.items():
            installed = merged.setdefault(key, default())
            conflict = isinstance(value, dict) & isinstance(installed, dict)

            if conflict and installed is not value:
                merged[key] = merge_dict(installed, value)
            else:
                resolver(installed, value)

    return merged


def invert_dict(
        subject: dict, default: Callable = None, resolver: Callable = None
):
    _output = {}
    for key, value in subject.items():
        values = value if isinstance(value, Iterable) else (value,)
        for each in values:
            if default and resolver:
                store = _output.setdefault(each, default())
                resolver(store, key)
            else:
                _output[each] = key
    return _output


def utc_datetime(_from: Optional[str] = None) -> datetime:
    """
    Returns time-zone aware datetime object in UTC. You can optionally pass
    an existing ISO string. The function will parse it to object and make
    it UTC if it's not
    :params _from: Optional[str]
    :returns: datetime
    """
    obj = datetime.now(timezone.utc) if not _from else isoparse(_from)
    return obj.astimezone(timezone.utc)


def utc_iso(_from: Optional[datetime] = None) -> str:
    """
    Returns time-zone aware datetime ISO string in UTC with military suffix.
    You can optionally pass datetime object. The function will make it
    UTC if it's not and serialize to string
    :param _from: Optional[datetime]
    :returns: str
    """
    obj = _from or utc_datetime()
    return obj.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')


def build_cloudtrail_record(cloud_identifier: str, region: str,
                            event_source: str, event_name: str) -> dict:
    return {
        "eventTime": utc_iso(),
        "awsRegion": region,
        "userIdentity": {
            "accountId": cloud_identifier
        },
        "eventSource": event_source,
        "eventName": event_name
    }


def normalize_lists(lists: List[List[str]]):
    """
    Changes the given lists in place making them all equal length by
    repeating the last attr the necessary number of times.
    :param lists:
    :return:
    """
    lens = [len(l) for l in lists]
    if not all(lens):
        raise ValueError('Each list must have at least one value')
    max_len = max(lens)
    for l in lists:
        l_len = len(l)
        if l_len < max_len:
            l += [l[-1] for _ in range(max_len - l_len)]
    assert len(set(len(l) for l in lists)) == 1  # equal lens


def build_cloudtrail_records(cloud_identifier: list, region: list,
                             event_source: list, event_name: list) -> list:
    """
    Builds CloudTrail log records based on given params. If you still
    don't get it just execute the function with some random parameters
    (no validation of parameters content provided) and see the result.
    """
    records = []
    lists = [cloud_identifier, region, event_source, event_name]
    normalize_lists(lists)

    for i in range(len(lists[0])):
        records.append(
            build_cloudtrail_record(cloud_identifier[i], region[i],
                                    event_source[i], event_name[i]))
    return records


def build_eventbridge_record(detail_type: str, source: str,
                             account: str, region: str, detail: dict) -> dict:
    return {
        "version": "0",
        "id": str(uuid4()),
        "detail-type": detail_type,
        "source": source,
        "account": account,
        "time": utc_iso(),
        "region": region,
        "resources": [],
        "detail": detail
    }


def build_maestro_record(event_action: str, group: str, sub_group: str,
                         tenant_name: str, cloud: str):
    """
    Only necessary attributes are kept
    :param event_action:
    :param group:
    :param sub_group:
    :param tenant_name:
    :param cloud:
    :return:
    """
    return {
        "_id": str(uuid4()),
        "eventAction": event_action,
        "group": group,
        "subGroup": sub_group,
        "tenantName": tenant_name,
        "eventMetadata": {
            "request": {'cloud': cloud},  # todo native maestro events have string here
            "cloud": cloud
        }
    }


class Color:
    """
    Terminal out put color
    """
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

    _current = ''

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
    def init(cls, color: str):
        """
        Color.init(Color.OKGREEN)
        Color._('hello')  # will be green
        Color.reset()
        :param color:
        :return:
        """
        cls._current = color

    @classmethod
    def reset(cls):
        cls._current = ''

    @classmethod
    def _(cls, st: str) -> str:
        return f'{cls._current}{st}{cls.ENDC}'


def validate_api_link(api_link: str) -> Optional[str]:
    message = None
    try:
        requests.get(api_link)
    except (requests.exceptions.MissingSchema,
            requests.exceptions.ConnectionError):
        message = f'Invalid API link: {api_link}'
    except requests.exceptions.InvalidURL:
        message = f'Invalid URL \'{api_link}\': No host specified.'
    except requests.exceptions.InvalidSchema:
        message = f'Invalid URL \'{api_link}\': No network protocol specified ' \
                  f'(http/https).'
    return message
