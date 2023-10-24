import os
from datetime import timedelta, datetime, date
from json import JSONDecodeError, dumps
from pathlib import PurePosixPath
from re import compile, escape, error, Pattern
from typing import Dict, Union, Callable, Iterator, Type, List, Optional, \
    Generator

from botocore.exceptions import ClientError

from helpers.constants import RULE_ID_ATTR, DESCRIPTION_ATTR
from helpers.log_helper import get_logger
from helpers.time_helper import utc_datetime
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService

_LOG = get_logger(__name__)

FINDINGS_KEY = 'findings'
RESOURCE_TYPE_ATTR = 'resourceType'
SEVERITY_ATTR = 'severity'
REGION_ATTR = 'region'
RESOURCES_ATTR = 'resources'

LIST_TYPE_ATTR = 'list_type'
MAP_TYPE_ATTR = 'map_type'
MAP_KEY_ATTR = 'map_key'


class FindingsService:
    RULE_ITEM_KEYS: set = {DESCRIPTION_ATTR, RESOURCE_TYPE_ATTR, SEVERITY_ATTR}
    FILTER_KEYS: tuple = (RULE_ID_ATTR, REGION_ATTR,
                          RESOURCE_TYPE_ATTR, SEVERITY_ATTR)

    def __init__(self, environment_service: EnvironmentService,
                 s3_client: S3Client):
        self._environment_service = environment_service
        self._s3_client = s3_client

    @property
    def _bucket_name(self) -> str:
        """
        Retrieves name of Findings bucket, from the settings service.
        :return:str
        """
        return self._environment_service.get_statistics_bucket_name()

    def get_findings_keys(self, from_: Optional[date] = None,
                          until: Optional[date] = None,
                          days: Optional[int] = 365
                          ) -> Generator[str, None, None]:
        """
        Return an iterator over all the finding's keys:
        from <= [keys] < until
        If not parameters are specified, a year back is returned
        :param from_:
        :param until:
        :param days:
        :return:
        """
        if from_ and until:
            start, end = from_.isoformat(), until.isoformat()
        elif from_:
            start = from_.isoformat()
            end = (from_ + timedelta(days=days)).isoformat()
        elif until:
            start = (until - timedelta(days=days)).isoformat()
            end = until.isoformat()
        else:  # nothing
            start = (date.today() - timedelta(days=days)).isoformat()
            end = date.today().isoformat()
        _LOG.debug(f'Retrieving findings keys from {start} until {end}')
        start_after = str(PurePosixPath(FINDINGS_KEY, start))
        end_when = str(PurePosixPath(FINDINGS_KEY, end))

        prefix = str(PurePosixPath(FINDINGS_KEY,
                                   os.path.commonprefix([start, end])))
        keys = self._s3_client.list_dir(
            bucket_name=self._bucket_name,
            key=prefix,
            start_after=start_after,
        )  # list_dir performs pagination

        def _check(key: str) -> bool:
            """Checks whether the folder is vaid"""
            try:
                datetime.fromisoformat(key.strip('/').split('/')[1])
                return True
            except (ValueError, IndexError):
                return False

        for k in filter(_check, keys):
            if k >= end_when:  # we've reached the latest
                break
            yield k

    def get_findings_for_period(self, until_date, days, account_id) -> \
            Optional[dict]:
        """
        Retrieves last available findings for a specific period
        """
        it = self.get_findings_keys(
            until=until_date,  # not including
            days=days
        )
        key = self.get_latest_key(it, account_id)
        if not key:
            return
        return self._s3_client.get_json_file_content(
            bucket_name=self._environment_service.get_statistics_bucket_name(),
            full_file_name=key
        )

    @staticmethod
    def get_latest_key(it: Iterator[str], identifier: str) -> Optional[str]:
        for key in reversed(list(it)):
            _json = key.endswith(f'{identifier}.json.gz')
            _gz = key.endswith(f'{identifier}.json')
            if _json or _gz:
                return key

    def get_latest_findings_key(self, identifier: str) -> Optional[str]:
        """
        Findings are updated daily. The structure is:
            finding/2023-08-17/<identifier>.json.gz
            finding/2023-08-18/<identifier>.json.gz
        In case today's findings have not been created yet, we must return
        the nearest old finding. And here we have a problem: AWS Api does
        not allow to list keys in descending order
        (https://github.com/boto/boto3/issues/2248) (we could've requested a
        list in descending order and limit 1), but we cannot.
        Considering that s3 listing api can return up to 1000 keys per request
        and since we save folders daily, we can return up to 1000 previous
        days using only one request. After that we look for the latest day,
        where identifier exists
        Also, findings folder can contain some other junk except data
        folders. We should skip them
        :param identifier:
        :return:
        """
        return self.get_latest_key(self.get_findings_keys(), identifier)

    def get_findings_content(
            self, identifier: str, path: str = '',
            findings_date: str = utc_datetime().date().isoformat()) -> Dict:
        """
        Retrieves the content of a Findings file, which maintains
        the latest, respective vulnerability state, bound to the account.
        Given that no Findings state has been previously assigned, could not
        be sourced out or simply does not exist, returns an empty Dict.
        :parameter identifier: str
        :parameter path: str
        :parameter findings_date: str
        :returns: Dict
        """
        _findings: Dict = {}
        _name, _key = self._bucket_name, self._get_key(
            identifier, FINDINGS_KEY, findings_date, path
        )
        if not self._s3_client.file_exists(bucket_name=_name, key=_key):
            _LOG.info(f'Findings for the account {identifier} for '
                      f'{findings_date} were not found. Looking for older')
            _key = self.get_latest_findings_key(identifier)
            if not _key:
                _LOG.warning(f'Old findings for {identifier} not found')
                return _findings
        try:
            _LOG.debug(f'Pulling Findings state, from \'{_name}\' bucket '
                       f'sourced by \'{_key}\' key.')
            _findings = self._s3_client.get_json_file_content(
                bucket_name=_name, full_file_name=_key
            )
            _LOG.info('Findings state has been pulled '
                      f'from the \'{_name}/{_key}\' bucket source.')
        except JSONDecodeError as _je:
            _LOG.warning(f'Content of a Findings file from a \'{_name}\' '
                         f'bucket and sourced by a \'{_key}\' key could'
                         f' not be decoded.')
        except (ClientError, Exception) as _e:
            _LOG.warning(f'No Findings bound to an account:\'{identifier}\','
                         f' have been found. An exception has occurred: {_e}.')
        return _findings

    def get_findings_url(self, identifier: str, path: str = '') -> \
            Union[str, Type[None]]:
        """
        Retrieves the access URL of a Findings file, which maintains
        the latest, respective vulnerability state, bound to the account.
        Relative persistence path is derived by a given `path` parameter and
        the respective `identifier`, stored inside a delegated storage.
        :parameter identifier: str
        :parameter path: str
        :returns: Union[str, Type[None]]
        """
        _name, _key = self._bucket_name, self._get_key(
            identifier, FINDINGS_KEY, utc_datetime().date().isoformat(), path)
        _url = None
        if not self._s3_client.file_exists(bucket_name=_name, key=_key):
            _key = self._get_key(identifier, FINDINGS_KEY, path)

        if not self._s3_client.file_exists(bucket_name=_name, key=_key):
            _LOG.warning(f'Presigned Findings state URL'
                         f' bound to an account:\'{identifier}\','
                         f' could not be generated for the \'{_name}/{_key}\' '
                         f'bucket source.')
            return None

        _LOG.debug(f'Generating presigned Findings state URL, for a '
                   f'\'{_name}\' bucket driven by \'{_key}\' key.')
        _url = self._s3_client.generate_presigned_url(
            bucket_name=_name, full_file_name=_key, expires_in_sec=3600)
        _LOG.info(f'Presigned Findings state URL has been generated for a '
                  f'\'{_name}\' bucket driven by \'{_key}\' key.')
        return _url

    def delete_findings(self, identifier: str, path: str = '') -> bool:
        """
        Removes a Findings state file, bound to an account, driven by a
        provided identifier. Relative persistence path is derived by a given
        `path` parameter and the respective `identifier`, which is stored
        inside a delegated storage.
        :parameter identifier: str
        :parameter path:str
        :returns bool:
        """
        _name, _key = self._bucket_name, self._get_key(
            identifier, FINDINGS_KEY, utc_datetime().date().isoformat(), path)
        _response = False
        try:
            _LOG.debug(f'Removing Findings state, from \'{_name}\' bucket '
                       f'sourced by \'{_key}\' key.')
            _findings = self._s3_client.delete_file(bucket_name=_name,
                                                    file_key=_key)
            _LOG.info('Findings state has been removed '
                      f'from the \'{_name}/{_key}\' bucket source.')
            _response = True

        except (ClientError, Exception) as _e:
            _LOG.warning(f'No Findings bound to an account:\'{identifier}\','
                         f' have been found. An exception has occurred: {_e}.')
        return _response

    def put_findings(self, content: Union[Dict, List],
                     identifier: str, path: str = ''):
        """
        Puts a Findings state content, bound to an account, driven by a
        provided identifier. Relative persistence path is derived by a given
        `path` parameter and the respective `identifier`, which is stored
        inside a delegated storage.
        :parameter identifier: str
        :parameter path:str
        :returns bool:
        """
        _name = self._bucket_name
        _key = self._get_key(identifier, FINDINGS_KEY,
                             utc_datetime().date().isoformat(), path)
        _response = False
        try:
            _LOG.debug(f'Putting Findings state, inside of \'{_name}\' bucket '
                       f'driven by \'{_key}\' key.')
            self._s3_client.put_object(
                bucket_name=_name, object_name=_key, body=dumps(content)
            )
            _LOG.info('Findings state has been put '
                      f'into \'{_name}/{_key}\' bucket source.')
            _response = True
        except (ClientError, BaseException) as _e:
            _LOG.warning(f'Findings bound to an account:\'{identifier}\','
                         f' could not be put into \'{_name}/{_key}\' '
                         f'bucket source. An exception has occurred: {_e}.')
        return _response

    @classmethod
    def expand_content(cls, content: Dict, key: Union[RESOURCES_ATTR]):
        """
        Inverts Findings collection content into a sequence of vulnerability
        items, production of which is delegated to a respective method,
        derived from a map, by a given key.
        :parameter content:Dict
        :parameter key:Union[RESOURCES_ATTR]
        :return:Iterator
        """
        _reference_map: Dict = cls._get_expansion_map()
        _expansion: Callable[[Dict], Iterator] = _reference_map.get(
            key, cls._i_default_expansion
        )
        return _expansion(content)

    @classmethod
    def _i_resource_expansion(cls, content: Dict) -> Iterator:
        """
        Inverts the Findings content into a sequence of vulnerability
        states of each resource, providing an iterator to filter on.
        :parameter content: Dict
        :return: Iterator
        """
        for rule_id, rule_content in content.items():
            _output_resource_dict = {}
            _rule_dict = cls._default_instance(rule_content, dict)
            _resource_region_dict = cls._default_instance(
                _rule_dict.get(RESOURCES_ATTR, None), dict
            )
            for _region, _resource_list in _resource_region_dict.items():
                for _resource_dict in _resource_list:
                    _rule_outsourced = {
                        each: _rule_dict[each] for each in cls.RULE_ITEM_KEYS
                        if each in _rule_dict
                    }
                    _item = {RULE_ID_ATTR: rule_id, REGION_ATTR: _region}
                    _item.update(_rule_outsourced)
                    _item.update(_resource_dict)
                    yield _item

    @classmethod
    def _i_default_expansion(cls, content: Dict) -> Iterator:
        """
        Inverts the Findings content into a default sequence of vulnerability
        states nesting the rule id, providing an iterator to filter on.
        :parameter content: Dict
        :return Iterator:
        """
        for rule_id, rule_content in content.items():
            _output_resource_dict = {}
            _rule_dict = cls._default_instance(rule_content, dict)
            _rule_dict.update({RULE_ID_ATTR: rule_id})
            yield _rule_dict

    @classmethod
    def extractive_iterator(cls, iterator: Iterator, key: str) -> Iterator:
        """
        Yields each item out of the iterator, by extracting the provided `key`
        from aforementioned items and mapping other content to it. Such head
        key is afterwards mapped to a label, which defaults to 'map_key'.
        :parameter iterator:Iterator
        :parameter key:str
        :return:Iterator
        """
        label = MAP_KEY_ATTR
        for item in iterator:
            _item = cls._default_instance(item, dict)
            _value = cls._default_instance(_item.get(key), str)
            if not _value:
                continue
            yield {
                label: _value, _value: {
                    k: v for k, v in _item.items() if k != key
                }
            }

    @classmethod
    def format_iterator(cls, iterator: Iterator,
                        key: Union[LIST_TYPE_ATTR, MAP_TYPE_ATTR]):
        """
        Inverts Findings collection content into a sequence of vulnerability
        items, production of which is delegated to a respective method,
        derived from a map, by a given key.
        :parameter iterator:Iterator
        :parameter key:Union[LIST_TYPE_ATTR, MAP_TYPE_ATTR]
        :return:Union[List, Dict]
        """
        _reference_map: Dict = cls._get_format_map()
        _format: Callable[[Iterator], Union[Dict, List]] = _reference_map.get(
            key, cls._i_list_formatter
        )
        return _format(iterator)

    @staticmethod
    def _i_list_formatter(iterator: Iterator):
        return list(iterator)

    @classmethod
    def _i_map_formatter(cls, iterator: Iterator) -> Dict:
        """
        Returns a mapped collection, which
        """
        collection: Dict[str, List[Dict]] = {}
        for item in iterator:
            _item = cls._default_instance(item, dict)
            _key: str = cls._default_instance(_item.get(MAP_KEY_ATTR), str)
            _value: Dict = cls._default_instance(_item.get(_key), dict)
            if not all((_key, _value)):
                continue
            collection.setdefault(_key, [])
            collection[_key].append(_value)
        return collection

    @classmethod
    def filter_iterator(cls, iterator: Iterator, dependent: bool,
                        **filters) -> Iterator:
        """
        Yields each item out of the iterator, given that provided filters
        predicate so, by referencing the `_apply_filters` method.
        :parameter iterator:Iterator
        :parameter dependent:bool
        :parameter filters:Dict
        :return:Iterator
        """
        for _item in iterator:
            _item: Dict = cls._default_instance(_item, dict)
            _apply = cls._apply_filters
            if not filters or filters and _apply(_item, dependent, **filters):
                yield _item

    @classmethod
    def _apply_filters(cls, _item: Dict, dependent: bool, **filters) -> bool:
        """
        Predicates whether a given `_item` contains key-value pairs, pattern
        requirement of which are respectively denoted in provided filters.
        Given that action is meant to depend the predicates upon each other,
        one may provide `dependent` value.
        :parameter _item:Dict
        :parameter dependent:bool
        :parameter filters:Dict
        :return:bool
        """
        _adheres = 0
        for key, each in filters.items():
            _each = cls._default_instance(each, str)
            pattern = cls._get_include_pattern(_each)
            _subject = cls._default_instance(_item.get(key, ''), str)
            _encountered = cls._encounter_pattern(pattern, _subject)
            if _encountered is not None and not dependent:
                break
            elif dependent:
                _adheres += int(bool(_encountered))
        else:
            return _adheres == len(filters)

        return True

    @classmethod
    def _get_expansion_map(cls):
        return {
            RESOURCES_ATTR: cls._i_resource_expansion
        }

    @classmethod
    def _get_format_map(cls):
        return {
            LIST_TYPE_ATTR: cls._i_list_formatter,
            MAP_TYPE_ATTR: cls._i_map_formatter
        }

    @staticmethod
    def _get_include_pattern(string: str) -> Union[Pattern, Type[None]]:
        try:
            return compile(f'^.*{escape(string)}.*$')
        except error:
            return None

    @staticmethod
    def _encounter_pattern(pattern: Pattern, subject: str):
        return next(pattern.finditer(subject), None)

    @classmethod
    def _get_key(cls, identifier: str, *keys) -> str:
        """
        Retrieves bucket key path bound to an account, by concatenating
        a reference path, derived from given keys.
        I.e. : "findings/$identifier.json"
        :parameter identifier:str
        :parameter keys:Tuple[str]
        :return: str
        """
        return str(PurePosixPath(*keys, f'{identifier}.json'))

    @staticmethod
    def get_demand_folder():
        return 'on-demand'

    @staticmethod
    def _default_instance(value, _type: type, *args, **kwargs):
        return value if isinstance(value, _type) else _type(*args, **kwargs)

    @staticmethod
    def unique_resources_from_raw_findings(content: dict) -> set:
        result = set()
        for _, policy in content.items():
            for region, resources in policy.get('resources', {}).items():
                if not resources:
                    continue
                for i in resources:
                    name = ''
                    for k, v in i.items():
                        name += f'{k}:{v}:'
                    if name:
                        result.add(name)
        return result
