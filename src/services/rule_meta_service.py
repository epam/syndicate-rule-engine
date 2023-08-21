import base64
import gzip
import json
from datetime import datetime
from itertools import chain
from typing import Optional, List, Dict, Union, Iterator, Generator, \
    Iterable, Tuple, Any, Set, Callable, TypedDict

from modular_sdk.models.pynamodb_extension.pynamodb_to_pymongo_adapter import \
    Result
from pydantic import BaseModel, Field, validator, root_validator
from pynamodb.expressions.condition import Condition

from helpers import RESPONSE_SERVICE_UNAVAILABLE_CODE, build_response, \
    adjust_cloud, Enum
from helpers.constants import COMPOUND_KEYS_SEPARATOR, AWS_CLOUD_ATTR, \
    AZURE_CLOUD_ATTR, GCP_CLOUD_ATTR, KUBERNETES_CLOUD_ATTR, ID_ATTR, \
    NAME_ATTR, VERSION_ATTR, FILTERS_ATTR, LOCATION_ATTR, CLOUD_ATTR
from helpers.log_helper import get_logger
from helpers.time_helper import utc_iso
from models.rule import Rule
from models.rule_meta import RuleMeta
from services import SERVICE_PROVIDER
from services.base_data_service import BaseDataService
from services.s3_settings_service import S3SettingsService
from services.setting_service import SettingsService

_LOG = get_logger(__name__)

RuleMetaCloud = Enum.build('RuleMetaCloud', (
    AWS_CLOUD_ATTR, AZURE_CLOUD_ATTR, GCP_CLOUD_ATTR, KUBERNETES_CLOUD_ATTR
))


class RuleMetaModel(BaseModel):
    class Config:
        use_enum_values = True
        extra = 'ignore'
        anystr_strip_whitespace = True

    name: str
    version: str
    cloud: RuleMetaCloud
    source: str  # "EPAM"
    service: Optional[str]
    category: Optional[str]
    article: Optional[str]
    service_section: str
    impact: str
    severity: str  # make choice?
    min_core_version: str
    report_fields: List[str] = Field(default_factory=list)
    multiregional: bool = False  # false by default only for AWS
    events: dict = Field(default_factory=dict)
    standard: dict = Field(default_factory=dict)
    mitre: dict = Field(alias='MITRE', default_factory=dict)
    remediation: str

    @validator('events', pre=False)
    def process_events(cls, value: dict) -> dict:
        processed = {}
        for source, names in value.items():
            processed[source] = list(chain.from_iterable(
                name.split(',') for name in names
            ))
        return processed

    @root_validator(pre=False)
    def validate_multiregional(cls, values: dict) -> dict:
        if values['cloud'] != AWS_CLOUD_ATTR:
            values['multiregional'] = True
        return values


class RuleModel(BaseModel):
    class Config:
        extra = 'ignore'
        anystr_strip_whitespace = True

    name: str
    resource: str
    description: str
    filters: List[Dict]


class RuleName:
    """
    Represents rule name scheme used by security team.
    """
    known_clouds = {'aws', 'azure', 'gcp', 'k8s'}  # inside rule name
    Resolved = Tuple[
        Optional[str], Optional[str], Optional[str], Optional[str]
    ]

    def __init__(self, raw: str):
        """
        Wraps our rule name. In general, it complies to such a format:
        [vendor]-[cloud]-[number]-[some human name].
        This wrapper can parse the name.
        Name part which contains only vendor and cloud can be given (ecc-aws)
        :param raw:
        """
        self._raw = raw
        self._resolved = self._resolve(self._raw)

    def _resolve(self, raw: str) -> Resolved:
        """
        Tries to resolve all the key parts from raw rule. Cloud returned only
        it's one of the known clouds
        :param raw:
        :return: (vendor, cloud, number, human name)
        """
        # _LOG.debug(f'Trying to resolve rule name: {raw}')
        items = raw.split('-', maxsplit=3)
        if len(items) == 1:
            return items[0], None, None, None
        # len(items) > 1
        _cl = items[1]
        if _cl not in self.known_clouds:
            _cl = None
        if len(items) == 2:
            return items[0], _cl, None, None
        if len(items) == 3:
            return items[0], _cl, items[2], None
        if len(items) == 4:
            return items[0], _cl, items[2], items[3]

    @property
    def vendor(self) -> Optional[str]:
        return self._resolved[0]

    @property
    def cloud_raw(self) -> Optional[str]:
        """
        Returns raw cloud resolved from rule name
        :return:
        """
        return self._resolved[1]

    @property
    def cloud(self) -> Optional[str]:
        """
        Returns cloud name which will be set to Rule's ID. The same cloud
        which is present in rule's metadata.
        For example: when self.cloud_raw returns 'aws', this method returns
        'AWS', because cloud_raw is just a lowercase value from rule name
        whereas 'AWS' is cloud name from rule's metadata.
        This method is probably a temp solution.
        :return:
        """
        _raw = self.cloud_raw
        if _raw == 'aws':
            return AWS_CLOUD_ATTR
        if _raw == 'azure':
            return AZURE_CLOUD_ATTR
        if _raw == 'gcp':
            return GCP_CLOUD_ATTR
        if _raw == 'k8s':
            return KUBERNETES_CLOUD_ATTR

    @property
    def number(self) -> Optional[str]:
        return self._resolved[2]

    @property
    def human_name(self) -> Optional[str]:
        return self._resolved[3]

    @property
    def raw(self) -> str:
        return self._raw


class RuleService(BaseDataService[Rule]):
    FilterValue = Union[str, Set[str]]

    def __init__(self, mappings_collector: 'LazyLoadedMappingsCollector'):
        super().__init__()
        self._mappings_collector = mappings_collector

    @staticmethod
    def gen_rule_id(customer: str, cloud: Optional[str] = None,
                    name: Optional[str] = None,
                    version: Optional[str] = None) -> str:
        """
        Make sure to supply this method with ALL the attributes in case
        you create a new rule
        :param customer:
        :param cloud:
        :param name:
        :param version:
        :return:
        """
        if name and not cloud:
            _LOG.warning('Cloud was not provided but name was. '
                         'Trying to resolve cloud from name')
            cloud = RuleName(name).cloud
        if name and not cloud or version and not name:
            raise AssertionError('Invalid usage')

        _id = f'{customer}{COMPOUND_KEYS_SEPARATOR}'
        if cloud:
            _id += f'{cloud}{COMPOUND_KEYS_SEPARATOR}'
        if name:
            _id += f'{name}{COMPOUND_KEYS_SEPARATOR}'
        if version:  # or Rule.latest_version(), or any str, or None
            _id += f'{version}'
        return _id

    @staticmethod
    def gen_location(git_project: Optional[str] = None,
                     ref: Optional[str] = None,
                     path: Optional[str] = None) -> str:
        """
        Make sure to supply this method with ALL the attribute in case
        you create a new rule
        :param git_project:
        :param ref:
        :param path:
        :return:
        """
        if ref and not git_project or path and not ref:
            raise AssertionError('Invalid usage')
        loc = ''
        if git_project:
            loc += f'{git_project}{COMPOUND_KEYS_SEPARATOR}'
        if ref:
            loc += f'{ref}{COMPOUND_KEYS_SEPARATOR}'
        if path:
            loc += f'{path}'
        return loc

    def create(self, customer: str, name: str, resource: str, description: str,
               cloud: Optional[str] = None,
               filters: Optional[List[Dict]] = None,
               version: Optional[str] = None,
               path: Optional[str] = None,
               ref: Optional[str] = None,
               commit_hash: Optional[str] = None,
               updated_date: Optional[Union[datetime, str]] = None,
               git_project: Optional[str] = None) -> Rule:
        if isinstance(updated_date, datetime):
            updated_date = utc_iso(updated_date)
        version = version or self.model_class.latest_version_tag()
        params = dict(
            id=self.gen_rule_id(customer, cloud, name, version),
            customer=customer,
            resource=resource,
            description=description,
            filters=filters,
            location=self.gen_location(git_project, ref, path),
            commit_hash=commit_hash,
            updated_date=updated_date,
        )
        return super().create(**params)

    def get_by_id_index(
            self, customer: str, cloud: Optional[str] = None,
            name: Optional[str] = None, version: Optional[str] = None,
            ascending: Optional[bool] = False, limit: Optional[int] = None,
            last_evaluated_key: Optional[dict] = None,
            filter_condition: Optional[Condition] = None,
            attributes_to_get: Optional[list] = None) -> Result:
        """
        Performs query by rules with full match of provided parameters.
        This query uses Customer id index (c-id-index)
        This is a low-level implementation when filter can be provided
        from outside.
        :param customer:
        :param cloud:
        :param name:
        :param version:
        :param ascending:
        :param limit:
        :param last_evaluated_key:
        :param filter_condition:
        :param attributes_to_get:
        :return:
        """
        _LOG.info('Going to query rules')
        if name and not cloud:
            # custom behaviour for this method if cloud not given
            _LOG.warning('Cloud was not provided but name was. '
                         'Trying to resolve cloud from name')
            cloud = RuleName(name).cloud
            if not cloud:
                return Result(iter([]))
        sort_key = self.gen_rule_id(customer, cloud, name, version)
        return self.model_class.customer_id_index.query(
            hash_key=customer,
            range_key_condition=self.model_class.id.startswith(sort_key),
            scan_index_forward=ascending,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
            filter_condition=filter_condition,
            attributes_to_get=attributes_to_get
        )

    def get_latest_rule(self, customer: str, name: str,
                        cloud: Optional[str] = None,
                        ) -> Optional[Rule]:
        return next(self.get_by_id_index(
            customer=customer,
            cloud=cloud,
            name=name,
            ascending=False,
            limit=1
        ), None)

    def get_fuzzy_by(self, customer: str, name_prefix: str,
                     cloud: Optional[str] = None, ascending: bool = False,
                     limit: Optional[int] = None,
                     last_evaluated_key: Optional[dict] = None
                     ) -> Iterator[Rule]:
        """
        Looks for rules by the given rule name prefix (ecc, ecc-aws,
        ecc-aws-022).
        :param customer:
        :param name_prefix:
        :param cloud:
        :param ascending:
        :param limit:
        :param last_evaluated_key:
        :return:
        """
        if not cloud:
            _LOG.warning('Cloud was not given to get_fuzzy_by. '
                         'Trying to resolve from name')
            cloud = RuleName(name_prefix).cloud
            if not cloud:
                return Result(iter([]))
        sort_key = f'{customer}{COMPOUND_KEYS_SEPARATOR}{cloud}' \
                   f'{COMPOUND_KEYS_SEPARATOR}{name_prefix}'
        return self.model_class.customer_id_index.query(
            hash_key=customer,
            range_key_condition=self.model_class.id.startswith(sort_key),
            scan_index_forward=ascending,
            limit=limit,
            last_evaluated_key=last_evaluated_key
        )

    def resolve_rule(self, customer: str, name_prefix: str,
                     cloud: Optional[str] = None) -> Optional[Rule]:
        """
        Looks for a concrete rule by given name prefix. If found, the latest
        version of the rule is returned.
        The method should be used to allow to resolve rule from partial names.
        Cloud, if not provided, will be resolved from rule name if possible
        :param customer:
        :param name_prefix:
        :param cloud:
        :return:
        """
        return next(self.get_fuzzy_by(
            customer=customer,
            name_prefix=name_prefix,
            cloud=cloud,
            ascending=False,
            limit=1
        ), None)

    @staticmethod
    def without_duplicates(rules: Iterable[Rule],
                           rules_version: Optional[str] = None
                           ) -> Generator[Rule, None, None]:
        """
        If ruleset contains different version of the same rules,
        the latest version will be kept. If rules_version is specified,
        it will be preferred in such described cases
        """
        name_rule: Dict[str, Rule] = {}
        for rule in rules:
            _name = rule.name
            if _name not in name_rule:
                name_rule[_name] = rule
                continue
            # duplicate
            if isinstance(rules_version,
                          str) and rule.version == rules_version:
                name_rule[_name] = rule
                continue
            # duplicate and either no rules_version or
            # rule.version != rules_version
            if (name_rule[_name].version != rules_version and
                    name_rule[_name].version < rule.version):
                name_rule[_name] = rule  # override with the largest version
        yield from name_rule.values()

    def dto(self, item: Rule) -> Dict[str, Any]:
        dct = super().dto(item)
        dct.pop(ID_ATTR, None)
        dct.pop(FILTERS_ATTR, None)
        dct.pop(LOCATION_ATTR, None)
        dct[NAME_ATTR] = item.name
        dct[CLOUD_ATTR] = item.cloud
        if item.version:
            dct[VERSION_ATTR] = item.version
        dct['project'] = item.git_project
        dct['branch'] = item.ref
        return dct

    def get_by_location_index(
            self, customer: str, project: Optional[str] = None,
            ref: Optional[str] = None, path: Optional[str] = None,
            ascending: bool = False, limit: Optional[int] = None,
            last_evaluated_key: Optional[dict] = None,
            filter_condition: Optional[Condition] = None) -> Result:
        """
        This query uses Customer location index (c-l-index).
        This is a low-level implementation when filter can be provided
        from outside
        :param customer:
        :param project:
        :param ref:
        :param path:
        :param ascending:
        :param limit:
        :param last_evaluated_key:
        :param filter_condition:
        :return:
        """
        sk = self.gen_location(project, ref, path)
        return self.model_class.customer_location_index.query(
            hash_key=customer,
            range_key_condition=(self.model_class.location.startswith(sk)),
            scan_index_forward=ascending,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
            filter_condition=filter_condition
        )

    def get_by(self, customer: str, project: Optional[str] = None,
               ref: Optional[str] = None, path: Optional[str] = None,
               cloud: Optional[str] = None, name: Optional[str] = None,
               version: Optional[str] = None,
               ascending: bool = False, limit: Optional[int] = None,
               last_evaluated_key: Optional[dict] = None,
               index: Optional[str] = 'c-l-index') -> Result:
        """
        A hybrid between get_by_id_index and get_by_location_index.
        This method can use either index. Which one will perform more
        efficiently depends on the data we query.
        Use this method only if you understand why it exists and what
        caveats it has
        :param customer:
        :param project:
        :param ref:
        :param path:
        :param cloud:
        :param name:
        :param version:
        :param ascending:
        :param limit:
        :param last_evaluated_key:
        :param index:
        :return:
        """
        assert index in {'c-l-index', 'c-id-index'}
        if index == 'c-l-index':
            _LOG.debug('Querying using c-l-index')
            _id = self.gen_rule_id(customer, cloud, name, version)
            return self.get_by_location_index(
                customer=customer,
                project=project,
                ref=ref,
                path=path,
                ascending=ascending,
                limit=limit,
                last_evaluated_key=last_evaluated_key,
                filter_condition=self.model_class.id.startswith(_id)
            )
        # index == 'c-id-index'
        condition = None
        location = self.gen_location(project, ref, path)
        if location:
            condition = self.model_class.location.startswith(location)
        return self.get_by_id_index(
            customer=customer,
            cloud=cloud,
            name=name,
            version=version,
            ascending=ascending,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
            filter_condition=condition
        )

    def resolve_names_from_map(self, names: Union[List[str], Set[str]],
                               clouds: Set[str] = None,
                               allow_multiple: Optional[bool] = False,
                               allow_ambiguous: Optional[bool] = False
                               ) -> Generator[Tuple[str, bool], None, None]:
        """
        Yields tuples there the first element is either resolved or not
        resolved rule name. If it's not resolved, the same value as came
        will be returned (no exceptions or etc.). The second element of the
        tuple if boolean pointing whether the rule name was resolved or not:
            003 -> (vendor-aws-003-valid, True)
            vendor-aws-qwert-invalid -> (vendor-aws-qwert-invalid, False)

        Some info:
        names of rules we work with adhere to such a format (fully lowercase):
        "[vendor]-[cloud]-[number]-[human name]". These names are used in
        rule repositories.
        :param names: list or names to resolve
        :param clouds: list of clouds to resolve the rules from.
        If not provided, all the available clouds are used
        :param allow_multiple: whether to allow to resolve multiple rules
        from one provided name (in case the name is ambiguous)
        :param allow_ambiguous: whether to allow to yield an ambiguous rule
        in case allow_multiple is False. See description below
        :return:
        """
        if allow_ambiguous and allow_multiple:
            raise AssertionError('If allow_multiple is True, '
                                 'allow_ambiguous must not be provided')
        clouds = clouds or {AWS_CLOUD_ATTR, AZURE_CLOUD_ATTR, GCP_CLOUD_ATTR}
        # + KUBERNETES_CLOUD_ATTR
        col = self._mappings_collector
        if not col.cloud_rules:
            _LOG.warning('Cloud to rules mapping is not available. '
                         'Proxying not resolved rules')
            for name in names:
                yield name, False
            return
        available_ids = lambda: chain.from_iterable(
            col.cloud_rules.get(cloud) or [] for cloud in clouds
        )
        for name in names:
            resolved = set()
            for sample in available_ids():
                if name not in sample:
                    continue
                resolved.add(sample)
                if allow_ambiguous and not allow_multiple:
                    # allow ambiguous means that even if the provided name
                    # to resolve is too fuzzy and can be interpreted as
                    # multiple different rules, we anyway resolve the first
                    # similar. If allow_ambiguous is False, we resolve the
                    # name only if it represents only one rule without doubt.
                    break
            if not resolved:
                _LOG.warning(f'Could not resolve any rule from: {name}')
                yield name, False
                continue
            # resolved rules exist
            if allow_multiple:
                _LOG.debug(f'Multiple rules resolved from {name}')
                for rule in resolved:
                    yield rule, True
                continue
            # multiple not allowed. But something is resolved.
            # Either one ambiguous or just one certain. Anyway yielding
            if len(resolved) == 1:
                _LOG.debug(f'One rule resolved from {name}')
                yield resolved.pop(), True
                continue
            # multiple not allowed
            _LOG.warning(f'Cannot certainly resolve name: {name}')
            yield name, False

    def resolved_names(self, *args, **kwargs) -> Generator[str, None, None]:
        """
        Ignores whether the rule was resolved or not. Just tries to do it
        :param args:
        :param kwargs:
        :return:
        """
        yield from (
            name for name, _ in self.resolve_names_from_map(*args, **kwargs)
        )

    @staticmethod
    def filter_by(rules: Iterable[Rule],
                  customer: Optional[FilterValue] = None,
                  cloud: Optional[FilterValue] = None,
                  name_prefix: Optional[FilterValue] = None,
                  version: Optional[FilterValue] = None,
                  git_project: Optional[FilterValue] = None,
                  ref: Optional[FilterValue] = None,
                  resource: Optional[FilterValue] = None
                  ) -> Iterator[Rule]:
        """
        God-like filter. Filter just using python. No queries
        :param rules:
        :param customer:
        :param cloud:
        :param name_prefix:
        :param version:
        :param git_project:
        :param ref:
        :param resource:
        :return:
        """
        if isinstance(customer, str):
            customer = {customer}
        if isinstance(cloud, str):
            cloud = {cloud}
        if isinstance(name_prefix, str):
            name_prefix = {name_prefix}
        if isinstance(version, str):
            name_prefix = {version}
        if isinstance(git_project, str):
            git_project = {git_project}
        if isinstance(ref, str):
            ref = {ref}
        if isinstance(resource, str):
            resource = {resource}

        def _check(rule: Rule) -> bool:
            if customer and rule.customer not in customer:
                return False
            if cloud and rule.cloud not in map(adjust_cloud, cloud):
                return False
            if name_prefix and not any(
                    map(lambda n: rule.name.startswith(n), name_prefix)):
                return False
            if git_project and rule.git_project not in git_project:
                return False
            if ref and rule.ref not in ref:
                return False
            if resource and rule.resource not in resource:
                return False
            return True

        return filter(_check, rules)


class HumanData(TypedDict):
    """
    Human-targeted info about rule
    """
    article: Optional[str]
    impact: str
    report_fields: List[str]
    remediation: str


class MappingsCollector:
    """
    This class helps to retrieve specific projections from rule's meta and
    keep them as mappings to allow more cost-effective access
    """
    SeverityType = Dict[str, str]  # rule to severity
    StandardType = Dict[str, Dict]  # rule to standards map
    MitreType = Dict[str, Dict]  # rule to mitre map
    ServiceSectionType = Dict[str, str]  # rule to service section
    CloudRulesType = Dict[str, Set[str]]  # cloud to rules
    Events = Dict[str, Dict[str, List[str]]]
    HumanDataType = Dict[str, HumanData]

    def __init__(self, compressor=gzip):
        """
        Compressor must implement compress and decompress. Use something from
        standard library
        :param compressor:
        """
        self._severity = {}
        self._standard = {}
        self._mitre = {}
        self._service_section = {}
        self._cloud_rules = {}
        self._human_data = {}

        self._aws_standards_coverage = {}
        self._azure_standards_coverage = {}
        self._google_standards_coverage = {}

        self._aws_events = {}
        self._azure_events = {}
        self._google_events = {}

        self._compressor = compressor

    def event_map(self, cloud: str) -> Optional[dict]:
        if cloud == AWS_CLOUD_ATTR:
            return self._aws_events
        if cloud == AZURE_CLOUD_ATTR:
            return self._azure_events
        if cloud == GCP_CLOUD_ATTR:
            return self._google_events

    def add_meta(self, meta: RuleMetaModel):
        self._severity[meta.name] = meta.severity
        self._mitre[meta.name] = meta.mitre
        self._service_section[meta.name] = meta.service_section
        self._standard[meta.name] = meta.standard
        self._cloud_rules.setdefault(meta.cloud, []).append(meta.name)
        self._human_data[meta.name] = {
            'article': meta.article,
            'impact': meta.impact,
            'report_fields': meta.report_fields,
            'remediation': meta.remediation
        }

        _map = self.event_map(meta.cloud)
        if isinstance(_map, dict):
            for source, names in meta.events.items():
                _map.setdefault(source, {})
                for name in names:  # here already parsed, without ','
                    _map[source].setdefault(name, []).append(meta.name)

    def dumps_json(self, data: dict) -> str:
        _LOG.debug(f'Dumping to JSON and compressing some data')
        return base64.b64encode(
            self._compressor.compress(
                json.dumps(data, separators=(',', ':')).encode()
            )
        ).decode()

    def loads_json(self, data: Union[str, bytes]) -> dict:
        _LOG.debug(f'Un-compressing and loading JSON data')
        return json.loads(self._compressor.decompress(base64.b64decode(data)))

    @property
    def severity(self) -> SeverityType:
        return self._severity

    @severity.setter
    def severity(self, value: SeverityType):
        self._severity = value

    @property
    def standard(self) -> StandardType:
        return self._standard

    @standard.setter
    def standard(self, value: StandardType):
        self._standard = value

    @property
    def mitre(self) -> MitreType:
        return self._mitre

    @mitre.setter
    def mitre(self, value: MitreType):
        self._mitre = value

    @property
    def service_section(self) -> ServiceSectionType:
        return self._service_section

    @service_section.setter
    def service_section(self, value: ServiceSectionType):
        self._service_section = value

    @property
    def cloud_rules(self) -> CloudRulesType:
        return self._cloud_rules

    @cloud_rules.setter
    def cloud_rules(self, value: CloudRulesType):
        self._cloud_rules = value

    @property
    def human_data(self) -> HumanDataType:
        return self._human_data

    @human_data.setter
    def human_data(self, value: HumanDataType):
        self._human_data = value

    @property
    def aws_standards_coverage(self) -> dict:
        return self._aws_standards_coverage

    @aws_standards_coverage.setter
    def aws_standards_coverage(self, value: dict):
        self._aws_standards_coverage = value

    @property
    def azure_standards_coverage(self) -> dict:
        return self._azure_standards_coverage

    @azure_standards_coverage.setter
    def azure_standards_coverage(self, value: dict):
        self._azure_standards_coverage = value

    @property
    def google_standards_coverage(self) -> dict:
        return self._google_standards_coverage

    @google_standards_coverage.setter
    def google_standards_coverage(self, value: dict):
        self._google_standards_coverage = value

    @property
    def aws_events(self) -> Events:
        return self._aws_events

    @aws_events.setter
    def aws_events(self, value: Events):
        self._aws_events = value

    @property
    def azure_events(self) -> Events:
        return self._azure_events

    @azure_events.setter
    def azure_events(self, value: Events):
        self._azure_events = value

    @property
    def google_events(self) -> Events:
        return self._google_events

    @google_events.setter
    def google_events(self, value: Events):
        self._google_events = value

    def compressed(self, value: dict) -> str:
        return self.dumps_json(value)

    def decompressed(self, value: str) -> dict:
        return self.loads_json(value)


class LazyLoadedMappingsCollector:
    """
    Read only class which allows to load mappings lazily. Currently, it
    loads them from S3
    """

    def __init__(self, collector: MappingsCollector,
                 settings_service: SettingsService,
                 s3_settings_service: S3SettingsService,
                 abort_if_not_found: bool = True):
        self._collector = collector
        self._settings_service = settings_service
        self._s3_settings_service = s3_settings_service
        self._abort_if_not_found = abort_if_not_found

    @classmethod
    def build(cls) -> 'LazyLoadedMappingsCollector':
        return cls(
            collector=MappingsCollector(),
            settings_service=SERVICE_PROVIDER.settings_service(),
            s3_settings_service=SERVICE_PROVIDER.s3_settings_service(),
            abort_if_not_found=True
        )

    @staticmethod
    def abort(domain: str = 'mapping'):
        return build_response(
            code=RESPONSE_SERVICE_UNAVAILABLE_CODE,
            content=f'Cannot access {domain} data'
        )

    def _load_setting(self, name: str, get: Callable,
                      abort: Optional[bool] = None):
        """
        :param name: MappingsCollector property name.
        :param get: method to get value
        :return:
        """
        if abort is None:
            abort = self._abort_if_not_found
        if not getattr(self._collector, name, {}):
            _LOG.debug(f'Loading setting: {name}')
            _raw = get()
            if not _raw and abort:
                return self.abort(name)
            if _raw:
                setattr(self._collector, name,
                        self._collector.decompressed(_raw))
        return getattr(self._collector, name, {}) or {}

    def _load_s3(self, name: str, get: Callable,
                 abort: Optional[bool] = None):
        """
        From s3 setting services files already decompressed
        :param name:
        :param get:
        :return:
        """
        if abort is None:
            abort = self._abort_if_not_found
        if not getattr(self._collector, name, {}):
            _LOG.debug(f'Loading s3: {name}')
            _raw = get()
            if not _raw and abort:
                return self.abort(name)
            if _raw:
                setattr(self._collector, name, _raw)
        return getattr(self._collector, name, {}) or {}

    _load = _load_s3

    @property
    def severity(self) -> dict:
        return self._load('severity',
                          self._s3_settings_service.rules_to_severity)

    @property
    def service_section(self) -> dict:
        return self._load('service_section',
                          self._s3_settings_service.rules_to_service_section)

    @property
    def standard(self) -> dict:
        return self._load('standard',
                          self._s3_settings_service.rules_to_standards)

    @property
    def mitre(self) -> dict:
        return self._load('mitre',
                          self._s3_settings_service.rules_to_mitre)

    @property
    def cloud_rules(self) -> dict:
        """
        This mapping is kind of special. It's just auxiliary. It means that
        the functions, this mapping is designed for, can be used without the
        map (obviously they will work worse, but still, they will work),
        whereas all the other mappings are required for the functional they
        provided for
        :return:
        """
        return self._load(
            'cloud_rules',
            self._s3_settings_service.cloud_to_rules,
            abort=False
        )

    @property
    def human_data(self) -> dict:
        return self._load(
            'human_data',
            self._s3_settings_service.human_data,
            abort=False
        )

    @property
    def aws_standards_coverage(self) -> dict:
        def get():
            return self._s3_settings_service.aws_standards_coverage()
            # if val and isinstance(val.value, dict):
            #     return val.value.get('value')

        return self._load('aws_standards_coverage', get)

    @property
    def azure_standards_coverage(self) -> dict:
        def get():
            return self._s3_settings_service.azure_standards_coverage()
            # if val and isinstance(val.value, dict):
            #     return val.value.get('value')

        return self._load('azure_standards_coverage', get)

    @property
    def google_standards_coverage(self) -> dict:
        def get():
            return self._s3_settings_service.google_standards_coverage()
            # if val and isinstance(val.value, dict):
            #     return val.value.get('value')

        return self._load('google_standards_coverage', get)

    @property
    def aws_events(self) -> dict:
        return self._load('aws_events',
                          self._s3_settings_service.aws_events)

    @property
    def azure_events(self) -> dict:
        return self._load('azure_events',
                          self._s3_settings_service.azure_events)

    @property
    def google_events(self) -> dict:
        return self._load('google_events',
                          self._s3_settings_service.google_events)


class RuleMetaService(BaseDataService[RuleMeta]):
    def get_rule_meta(self, rule: Rule) -> Optional[RuleMeta]:
        """
        If the rule does not contain version we must receive the latest
        available meta for this rule
        :param rule:
        :return:
        """
        name = rule.name
        version = rule.version
        _LOG.debug(f'Going to retrieve meta for rule: {name}')
        if version:
            _LOG.debug('Rule has version. '
                       'Retrieving meta of the same version')
            return next(self.get_by(name, version,
                                    ascending=False, limit=1), None)
        _LOG.debug('Rule does not have version. Retrieving the latest meta')
        return self.get_latest_meta(name)

    def get_by(self, name: str, version: Optional[str] = None,
               ascending: bool = False, limit: Optional[int] = None,
               last_evaluated_key: Optional[dict] = None,
               filter_condition: Optional[Condition] = None,
               attributes_to_get: Optional[list] = None) -> Iterator[RuleMeta]:
        rkc = None
        if version:
            rkc = self.model_class.version.startswith(version)
        return self.model_class.query(
            hash_key=name,
            range_key_condition=rkc,
            scan_index_forward=ascending,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
            filter_condition=filter_condition,
            attributes_to_get=attributes_to_get
        )

    def get_latest_meta(self, name: str,
                        attributes_to_get: Optional[list] = None
                        ) -> Optional[RuleMeta]:
        return next(self.get_by(
            name=name,
            ascending=False,
            limit=1,
            attributes_to_get=attributes_to_get
        ), None)
