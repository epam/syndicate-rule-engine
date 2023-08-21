import json
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional
from typing import Union, Set

from helpers import filter_dict, hashable
from helpers.log_helper import get_logger

_LOG = get_logger(__name__)

DETAILED_REPORT_FILE = 'detailed_report.json'
USER_REPORT_FILE = 'user_detailed_report.json'
DIGEST_REPORT_FILE = 'report.json'

STATISTICS_FILE = 'statistics.json'
API_CALLS_FILE = 'api_calls.json'

KEYS_TO_EXCLUDE_FOR_USER = {'standard_points',}

Coverage = Dict[str, Dict[str, float]]

# Failed rule types.
ACCESS_TYPE = 'access'
CORE_TYPE = 'core'


@dataclass
class ChartsData:
    """Charts images in bytes and decoded to UTF-8"""
    checks_performed: Optional[str]
    severity: Optional[str]
    resource_type: Optional[str]


@dataclass
class AccountsData:
    """Data about specific account"""
    account_name: str
    regions: list
    total_checks: int
    success: int
    fail: int
    last_sync: datetime
    vulnerabilities: dict
    charts: ChartsData


@dataclass
class CloudData:
    """Data about all accounts within specific cloud provider"""
    all_regions: set
    total_checks: int
    total_successful: int
    total_failed: int
    total_vulnerabilities: int
    total_critical: int
    total_high: int
    total_medium: int
    total_low: int
    total_info: int
    total_unknown: int
    accounts: List[AccountsData]


class FindingsCollection:
    keys_to_keep: set = {
        'description', 'resourceType'
    }

    def __init__(self, data: dict = None, rules_data: dict = None):
        self._data: Dict[tuple, set] = data or {}
        self._rules_data: Dict[str, Dict] = rules_data or {}

    @property
    def rules_data(self) -> dict:
        return self._rules_data

    @classmethod
    def from_detailed_report(
            cls, report: dict,
            only_report_fields: bool = True,
            retain_all_keys: bool = False,
            keys_to_exclude: List[str] = None
    ) -> 'FindingsCollection':

        """Imports data from detailed_report's format. In addition, collects
        the descriptions and other info about a rule."""

        rak = retain_all_keys
        kte = keys_to_exclude or []

        result, rules_data = {}, {}
        for region, region_policies in report.items():
            for policy in region_policies:
                p_data = policy['policy']
                if p_data['name'] not in rules_data:  # retrieve rules info

                    keys = tuple(p_data) if rak else tuple(cls.keys_to_keep)
                    if kte:
                        keys = set(keys) - set(kte)

                    if 0 < len(keys) < len(p_data):
                        kept = filter_dict(p_data, keys)
                    elif len(keys) == len(p_data):
                        kept = p_data
                    else:
                        continue

                    rules_data[p_data['name']] = kept

                report_fields = set()
                if only_report_fields:
                    report_fields = set(p_data.get('report_fields') or [])

                result.setdefault((p_data['name'], region), set())
                for resource in policy.get('resources', []):
                    result[(p_data['name'], region)].add(
                        hashable(filter_dict(resource, report_fields))
                    )
        return cls(result, rules_data)

    @classmethod
    def deserialize(cls, report: dict) -> 'FindingsCollection':
        """Deserializes from a standard dict to the inner fancy one"""
        result, rules_data = {}, {}
        for rule, data in report.items():
            rules_data[rule] = filter_dict(data, cls.keys_to_keep)
            for region, resources in data.get('resources', {}).items():
                result.setdefault((rule, region), set())
                for resource in resources:
                    result[(rule, region)].add(hashable(resource))
        return cls(result, rules_data)

    def serialize(self) -> dict:
        """Serializes to a dict acceptable for json.dump"""
        result = {}
        for k, v in self._data.items():
            rule, region = k
            if rule not in result:
                result[rule] = filter_dict(self._rules_data.get(rule, {}),
                                           self.keys_to_keep)
            result[rule].setdefault('resources', {})[region] = list(v)
        return result

    @property
    def region_report(self):
        result = {}
        for k, resources in self._data.items():
            rule, region = k
            scope = result.setdefault(region, [])
            scope.append(
                {
                    'policy': {
                        'name': rule, **self._rules_data.get(rule, {})
                    },
                    'resources': list(resources)
                }
            )
        return result

    def json(self) -> str:
        return json.dumps(self.serialize())

    def update(self, other: 'FindingsCollection') -> None:
        self._data.update(other._data)
        self._rules_data.update(other._rules_data)

    def __sub__(self, other: 'FindingsCollection') -> 'FindingsCollection':
        result = {}
        for k, v in self._data.items():
            found_resource = v - other._data.get(k, set())
            if found_resource:
                result[k] = found_resource
        return FindingsCollection(result,
                                  {**other._rules_data, **self._rules_data})

    def __len__(self) -> int:
        length = 0
        for v in self._data.values():
            length += len(v)
        return length

    def __bool__(self) -> bool:
        return bool(self._data)


class Standard:
    """Basic representation of rule's standard with version"""
    NONE_VERSION = 'null'

    def __init__(self, name: str, version: str = None, points: set = None):
        self._name = name
        self._version = version or self.NONE_VERSION
        self._points = points or set()

    def __hash__(self):
        return hash((self._name, self._version))

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return (self._name, self._version) == (other._name, other._version)
        elif isinstance(other, tuple) and len(other) == 2:
            return (self._name, self._version) == (other[0], other[1])
        raise NotImplementedError()

    def __repr__(self):
        return f'({self._name}, {self._version})'

    @property
    def name(self):
        return self._name

    @property
    def version(self):
        return self._version

    @property
    def points(self):
        return self._points

    @property
    def full_name(self):
        return f'{self._name} {self._version}' \
            if self._version != self.NONE_VERSION else self._name

    @classmethod
    def deserialize(cls, standards: Union[Dict[str, List], Dict[str, Dict]],
                    return_strings=False) -> Union[Set['Standard'], Set[str]]:
        """Currently rules' standards look like it's showed below
        {
            'Standard_1': [
                'v1 (point1,sub-point1,point2)',
                'v2'
            ],
            'Standard_2': [
                '(sub-point2)'
            ],
        }
        The method will transform it to this:
        {('Standard_1', 'v1'), ('Standard_1', 'v2'), ('Standard_2', 'null')}
        Each standard will contain a set of its points inside
        """
        result = set()
        for standard, versions in standards.items():
            for version in versions:
                params = dict(name=standard)
                version_points = version.rsplit(maxsplit=1)
                if len(version_points) == 2:  # version and points
                    v, points = version_points
                    params['version'] = v
                    params['points'] = set(points.strip('()').split(','))
                elif len(version_points) == 1 and version_points[0].startswith(
                        '('):  # only points
                    params['points'] = set(version_points[0].strip(
                        '()').split(','))
                elif len(version_points) == 1:  # only version
                    params['version'] = version_points[0]
                else:
                    raise ValueError(f'Wrong rule standard format: '
                                     f'{standard}, {version}')
                result.add(cls(**params))

        if return_strings:
            return {standard.full_name for standard in result}
        return result
