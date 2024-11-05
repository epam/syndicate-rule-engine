import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from dateutil.parser import isoparse
from webtest import TestApp, TestResponse
from helpers import HashableDict

SOURCE = Path(__file__).parent.parent / 'src'

DATA = Path(__file__).parent / 'data'

AWS_ACCOUNT_ID = '123456789012'
AZURE_ACCOUNT_ID = '3d615fa8-05c6-47ea-990d-9d162testing'  # subscription id
GOOGLE_ACCOUNT_ID = 'testing-project-123'


class InMemoryHvacClient:
    """
    This is in-memory mock for hvac.Client. This implementation should be
    enough for our purposes because i cannot find a lib that can do that
    """
    __data = {}

    def __init__(self, url=None, token=None, *args, **kwargs):
        class Container: pass

        self.secrets = Container()
        self.secrets.kv = Container()
        self.secrets.kv.v2 = Container()
        self.secrets.kv.v2.read_secret_version = self._read_secret_version
        self.secrets.kv.v2.create_or_update_secret = self._create_or_update_secret
        self.secrets.kv.v2.update_metadata = self._update_metadata
        self.secrets.kv.v2.delete_metadata_and_all_versions = self._delete_metadata_and_all_versions
        self.sys = Container()
        self.sys.enable_secrets_engine = self._enable_secret_engine
        self.sys.list_mounted_secrets_engines = self._list_mounted_secrets_engines

    @classmethod
    def reset(cls):
        cls.__data.clear()

    def is_authenticated(self):
        return True

    @staticmethod
    def _dt():
        return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')

    def _create_or_update_secret(self, path, secret, cas=None,
                                 mount_point='secret'):
        dt = self._dt()
        self.__class__.__data[(path, mount_point)] = (secret, dt)
        return {
            'request_id': str(uuid.uuid4()),
            'lease_id': '', 'renewable': False, 'lease_duration': 0,
            'data': {
                'data': secret,
                'metadata': {
                    'created_time': dt,
                    'custom_metadata': None, 'deletion_time': '',
                    'destroyed': False, 'version': 1
                }
            },
            'wrap_info': None,
            'warnings': None,
            'auth': None,
            'mount_type': mount_point
        }

    def _update_metadata(self, path, *args, **kwargs):
        pass

    def _read_secret_version(self, path, mount_point='secret'):
        item = self.__class__.__data.get((path, mount_point))
        if not item:
            from hvac.exceptions import InvalidPath
            raise InvalidPath
        return {
            'request_id': str(uuid.uuid4()),
            'lease_id': '', 'renewable': False, 'lease_duration': 0,
            'data': {
                'data': item[0],
                'metadata': {
                    'created_time': item[1],
                    'custom_metadata': None,
                    'deletion_time': '',
                    'destroyed': False, 'version': 1
                }
            },
            'wrap_info': None,
            'warnings': None, 'auth': None, 'mount_type': mount_point
        }

    def _delete_metadata_and_all_versions(self, path, mount_point='secret'):
        self.__class__.__data.pop((path, mount_point), None)
        return True

    def _enable_secret_engine(self, *args, **kwargs):
        pass

    def _list_mounted_secrets_engines(self):
        return ['secrets', 'kv']


def valid_isoformat(d) -> bool:
    if not d: return False
    try:
        isoparse(d)
        return True
    except ValueError:
        return False


def valid_uuid(item) -> bool:
    if not item: return False
    try:
        uuid.UUID(str(item))
        return True
    except ValueError:
        return False


def comparable(item: dict | list | tuple | set | str | float | int | None,
               ignore_dates: bool = False):
    """
    >>> d = [{'key1': [1,2,3]}, {'key2': [4,5,6]}]
    >>> d1 = [{'key2': [6,5,4]}, {'key1': [3,2,1]}]
    >>> comparable(d) == comparable(d1)
    Order of items inside inner collections is not important.
    The result of this function cannot be dumped to json without default=list,
    """
    if isinstance(item, dict):
        return HashableDict(zip(item.keys(), map(comparable, item.values())))
    elif isinstance(item, (tuple, list, set)):
        return frozenset(map(comparable, item))
    else:
        if ignore_dates and valid_isoformat(item):
            return None
        return item


def dicts_equal(one, two, ignore_dates: bool = True) -> bool:
    """
    Ignores the order of dicts inside lists
    """
    return comparable(one, ignore_dates) == comparable(two, ignore_dates)


class SREClient:
    """
    This class just to abstract away from TestApp
    """
    __slots__ = '_app', '_default_stage'

    def __init__(self, test_wsgi_app: TestApp, default_stage='caas'):
        self._app = test_wsgi_app
        self._default_stage = default_stage.strip('/')

    def request(self, url: str, method: str = 'GET', *,
                auth: str | None = None,
                data: dict | None = None) -> TestResponse:
        method = method.lower()
        assert method in ('get', 'post', 'put', 'delete', 'patch')
        data = data or {}

        path = urlparse(url).path.lstrip('/')
        if not path.startswith(self._default_stage):
            path = f'{self._default_stage}/{path}'
        path = f'/{path}'

        headers = None
        if auth: headers = {'Authorization': auth}

        if method == 'get':
            return self._app.get(path, params=data, headers=headers,
                                 expect_errors=True)
        # post, put, patch, delete
        return getattr(self._app, f'{method}_json')(
            path, data, headers=headers, expect_errors=True
        )
