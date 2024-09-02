import base64
import sys
import json
import secrets
import string
import time
import uuid
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Callable, TypeVar

from dateutil.parser import isoparse
from urllib3.exceptions import LocationParseError
from urllib3.util import parse_url
from distutils.version import LooseVersion


def urljoin(*args: str) -> str:
    """
    This method somehow differs from urllib.parse.urljoin. See:
    >>> urljoin('one', 'two', 'three')
    'one/two/three'
    >>> urljoin('one/', '/two/', '/three/')
    'one/two/three'
    >>> urljoin('https://example.com/', '/prefix', 'path/to/service')
    'https://example.com/prefix/path/to/service'
    :param args: list of string
    :return:
    """
    return '/'.join(map(lambda x: str(x).strip('/'), args))


def sifted(data: dict) -> dict:
    """
    >>> sifted({'k': 'value', 'k1': None, 'k2': '', 'k3': 0, 'k4': False})
    {'k': 'value', 'k3': 0, 'k4': False}
    :param data:
    :return:
    """
    return {k: v for k, v in data.items() if isinstance(v, (bool, int)) or v}


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


def utc_datetime(_from: str | None = None) -> datetime:
    """
    Returns time-zone aware datetime object in UTC. You can optionally pass
    an existing ISO string. The function will parse it to object and make
    it UTC if it's not
    :params _from: Optional[str]
    :returns: datetime
    """
    obj = datetime.now(timezone.utc) if not _from else isoparse(_from)
    return obj.astimezone(timezone.utc)


def utc_iso(_from: datetime | None = None) -> str:
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


def normalize_lists(lists: list[list[str]]):
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
        "id": str(uuid.uuid4()),
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
        "_id": str(uuid.uuid4()),
        "eventAction": event_action,
        "group": group,
        "subGroup": sub_group,
        "tenantName": tenant_name,
        "eventMetadata": {
            "request": {'cloud': cloud},
            # todo native maestro events have string here
            "cloud": cloud
        }
    }


def validate_api_link(url: str) -> str | None:
    url = url.lstrip()

    if "://" in url and not url.lower().startswith("http"):
        return 'Invalid API link: not supported scheme'
    try:
        scheme, auth, host, port, path, query, fragment = parse_url(url)
    except LocationParseError as e:
        return 'Invalid API link'
    if not scheme:
        return 'Invalid API link: missing scheme'
    if not host:
        return 'Invalid API link: missing host'
    try:
        req = urllib.request.Request(url)
        urllib.request.urlopen(req)
    except urllib.error.HTTPError as e:
        pass
    except urllib.error.URLError as e:
        return 'Invalid API link: cannot make a request'


RT = TypeVar('RT')  # return type
ET = TypeVar('ET', bound=Exception)  # exception type


def catch(func: Callable[[], RT], exception: type[ET] = Exception
          ) -> tuple[RT | None, ET | None]:
    """
    Calls the provided function and catches the desired exception.
    Seems useful to me :) ?
    :param func:
    :param exception:
    :return:
    """
    try:
        return func(), None
    except exception as e:
        return None, e


def check_version_compatibility(api, cli, /):
    # todo check without distutils
    if not api:
        print('Custodian API did not return the version number!',
              file=sys.stderr)
        return
    cli_version = LooseVersion(cli)
    api_version = LooseVersion(api)
    if cli_version > api_version:
        print(f'Consider that you SRE CLI version {cli_version} is '
              f'higher than the API version {api_version}',
              file=sys.stderr)
        return
    if cli_version.version[0] < api_version.version[0]:  # Major
        print(f'CLI Major version {cli_version} is lower than '
              f'the API version {api_version}. Please, update the CLI',
              file=sys.stderr)
        sys.exit(1)
    if cli_version.version[1] < api_version.version[1]:  # Minor
        print(f'CLI Minor version {cli_version} is lower than the '
              f'API version {api_version}. Some features may not '
              f'work. Consider updating the SRE CLI',
              file=sys.stderr)
