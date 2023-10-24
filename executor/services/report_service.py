import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import cached_property
from itertools import chain
from pathlib import Path, PurePosixPath
from typing import Dict, Generator, List, Optional, TypedDict, Tuple, Iterable

from c7n.provider import get_resource_class
from c7n.resources import load_resources
from modular_sdk.models.parent import Parent
from modular_sdk.models.tenant import Tenant

from helpers import filter_dict, hashable, json_path_get
from helpers.constants import FINDINGS_FOLDER, \
    AZURE_COMMON_REGION, MULTIREGION, AWS, AZURE, GOOGLE, KUBERNETES
from helpers.log_helper import get_logger
from helpers.time_helper import utc_datetime
from services.environment_service import EnvironmentService
from services.s3_settings_service import S3SettingsService

_LOG = get_logger(__name__)

DETAILED_REPORT_FILE = 'detailed_report.json'
REPORT_FILE = 'report.json'  # `digest-report`
DIFFERENCE_FILE = 'difference.json'

REPORT_FIELDS = {'id', 'name', 'arn'}  # + date


class PolicyReportItem(TypedDict):
    description: str
    region: str
    multiregional: str  # "true" or "false"
    resources: List[Dict]
    remediation: Optional[str]
    impact: Optional[str]
    standard: Optional[Dict]
    severity: Optional[str]
    article: Optional[str]
    service: str
    vuln_id_from_tool: Optional[str]
    tags: List[str]


class ReportFieldsLoader:
    """
    Each resource type has its own class in Custom Core. That class has
    resource_type inner class with some resource_type meta attributes.
    There is always `id` attribute which points to the fields that is ID for
    this particular type. Also, there are such attributes as: name, arn
    (for aws).
    """
    _fields = REPORT_FIELDS
    _mapping = {}

    @classmethod
    def _load_for_resource_type(cls, rt: str) -> Optional[dict]:
        """
        Updates mapping for the given resource type.
        It must be loaded beforehand
        :param rt:
        :return:
        """
        try:
            factory = get_resource_class(rt)
        except (KeyError, AssertionError) as e:
            _LOG.warning(f'Could not load resource type: {rt}')
            return
        resource_type = getattr(factory, 'resource_type', None)
        if not resource_type:
            _LOG.warning('Somehow resource type factory does not contain '
                         'inner resource_type class')
            return
        kwargs = {}
        for field in cls._fields:
            f = getattr(resource_type, field, None)
            if not f:
                continue
            kwargs[field] = f
        return kwargs

    @classmethod
    def get(cls, rt: str) -> dict:
        if rt not in cls._mapping:
            fields = cls._load_for_resource_type(rt)
            if not isinstance(fields, dict):
                return {}
            cls._mapping[rt] = fields
        return cls._mapping[rt]

    @classmethod
    def load(cls, resource_types: tuple = ('*',)):
        """
        Loads all the modules. In theory, we must use this class after
        performing scan. Till that moment all the necessary resources must be
        already loaded
        :param resource_types:
        :return:
        """
        load_resources(set(resource_types))


class JobResult:
    class RuleRawOutput(TypedDict):
        metadata: dict
        resources: List[Dict]

    class FormattedItem(TypedDict):  # our detailed report item
        policy: dict
        resources: List[Dict]

    class DigestReport(TypedDict):
        total_checks_performed: int
        successful_checks: int
        failed_checks: int
        total_resources_violated_rules: int

    RegionRuleOutput = Tuple[str, str, RuleRawOutput]

    def __init__(self, work_dir: str, cloud: str):
        self._work_dir = Path(work_dir)
        self._cloud = cloud

    @cached_property
    def environment_service(self) -> EnvironmentService:
        from services import SP
        return SP.environment_service()

    @staticmethod
    def cloud_to_resource_type_prefix() -> dict:
        return {
            AWS: 'aws',
            AZURE: 'azure',
            GOOGLE: 'gcp',
            KUBERNETES: 'k8s'
        }

    def adjust_resource_type(self, rt: str) -> str:
        rt = rt.split('.', maxsplit=1)[-1]
        return '.'.join((
            self.cloud_to_resource_type_prefix()[self._cloud], rt
        ))

    @staticmethod
    def _load_raw_rule_output(root: Path) -> Optional[RuleRawOutput]:
        """
        Folder with rule output contains three files:
        'custodian-run.log' -> logs in text
        'metadata.json' -> dict
        'resources.json' -> list or resources
        In case resources.json files does not exist we deem this execution
        invalid and do not load it
        :param root:
        :return:
        """
        logs = root / 'custodian-run.log'
        metadata = root / 'metadata.json'
        resources = root / 'resources.json'

        if not all(map(Path.exists, [logs, metadata, resources])):
            _LOG.debug(f'{root} will not be loaded. Its execution '
                       f'is invalid: {list(map(str, root.iterdir()))}')
            return
        with open(metadata, 'r') as file:
            metadata_data = json.load(file)
        with open(resources, 'r') as file:
            resources_data = json.load(file)
        return {
            'metadata': metadata_data,
            'resources': resources_data
        }

    def _format_item(self, item: RuleRawOutput) -> FormattedItem:
        """
        Keeps only description, name and resource type from metadata.
        Inserts Custom Core report fields in resources (id, name, arn)
        :param item:
        :return:
        """
        policy = item['metadata'].get('policy')
        rt = self.adjust_resource_type(policy.get('resource'))
        ReportFieldsLoader.load((rt,))  # should be loaded before
        fields = ReportFieldsLoader.get(rt)
        updated_resources = []
        for res in item['resources']:
            report_fields = {
                field: json_path_get(res, path)
                for field, path in fields.items()
            }
            updated_resources.append({**res, **report_fields})
        return {
            'policy': {
                'name': policy.get('name'),
                'resourceType': policy.get('resource'),
                'description': policy.get('description')
            },
            'resources': updated_resources
        }

    def iter_raw(self) -> Generator[RegionRuleOutput, None, None]:
        dirs = filter(
            lambda x: x.name != FINDINGS_FOLDER,
            filter(Path.is_dir, self._work_dir.iterdir())
        )
        for region in dirs:
            for rule in filter(Path.is_dir, region.iterdir()):
                loaded = self._load_raw_rule_output(rule)
                if not loaded:
                    continue
                yield region.name, rule.name, loaded

    def iter_raw_threads(self) -> Generator[RegionRuleOutput, None, None]:
        """
        The same as previous but reads file in multiple threads
        :return:
        """
        dirs = filter(
            lambda x: x.name != FINDINGS_FOLDER,
            filter(Path.is_dir, self._work_dir.iterdir())
        )
        with ThreadPoolExecutor() as executor:
            futures = {}
            for region in dirs:
                for rule in filter(Path.is_dir, region.iterdir()):
                    fut = executor.submit(self._load_raw_rule_output, rule)
                    futures[fut] = (region, rule)
            for future in as_completed(futures):
                output = future.result()
                if output:
                    region, rule = futures[future]
                    yield region.name, rule.name, output

    @staticmethod
    def resolve_azure_locations(it: Iterable[RegionRuleOutput]
                                ) -> Generator[RegionRuleOutput, None, None]:
        """
        The thing is: Custodian Custom Core cannot scan Azure
        region-dependently. A rule covers the whole subscription
        (or whatever, i don't know) and then each found resource has
        'location' field with its real location.
        In order to adhere to AWS logic, when a user wants to receive
        reports only for regions he activated, we need to filter out only
        appropriate resources.
        Also note that Custom Core has such a thing as `AzureCloud`. From
        my point of view it's like a mock for every region (because,
        I believe, in the beginning Core was designed for AWS and therefore
        there are regions). With the current scanner implementation
        (3.3.1) incoming `detailed_report` will always have one key:
        `AzureCloud` with a list of all the scanned rules. We must remap it.
        All the resources that does not contain
        'location' will be congested to 'multiregion' region.
        :return:
        """
        for _, rule, item in it:
            if not item['resources']:  # we cannot know
                yield MULTIREGION, rule, item
                continue
            # resources exist
            _loc_res = {}
            for res in item['resources']:
                loc = res.get('location') or MULTIREGION
                _loc_res.setdefault(loc, []).append(res)
            for location, resources in _loc_res.items():
                yield location, rule, {
                    'metadata': item['metadata'], 'resources': resources
                }

    def build_default_iterator(self) -> Iterable[RegionRuleOutput]:
        it = self.iter_raw_threads()
        if self._cloud == AZURE:
            it = self.resolve_azure_locations(it)
            regions = self.environment_service.target_regions()
            if regions:
                it = filter(lambda x: x[0] in regions, it)
        return it

    def raw_detailed_report(self) -> dict:
        """
        Produces region-specific object of un-formatted custodian detailed
        reports, found within a working directory.
        Note: does not retain error (failed to execute) reports.
        :return: Dict[str, List[Dict]] {
            $region: [
                {
                    'metadata': Dict,
                    'resources': List[Dict]
                }
            ]
        }
        """
        it = self.build_default_iterator()  # get iterator from outside
        res = {}
        for region, rule, output in it:
            res.setdefault(region, []).append(output)
        return res

    def detailed_report(self) -> dict:
        it = self.build_default_iterator()  # get iterator from outside
        res = {}
        for region, rule, output in it:
            res.setdefault(region, []).append(self._format_item(output))
        return res

    @staticmethod
    def digest_report(detailed_report: dict) -> DigestReport:
        total_checks = 0
        failed_checks = 0
        successful_checks = 0
        total_resources = set()
        for region, items in detailed_report.items():
            _total = len(items)
            _failed = len(list(
                item for item in items if item.get('resources')
            ))
            total_checks += _total
            failed_checks += _failed
            successful_checks += (_total - _failed)

            resources = chain.from_iterable(
                item.get('resources') or [] for item in items
            )
            for res in resources:
                total_resources.add(hashable(filter_dict(res, REPORT_FIELDS)))
        return {
            'total_checks_performed': total_checks,
            'successful_checks': successful_checks,
            'failed_checks': failed_checks,
            'total_resources_violated_rules': len(total_resources)
        }


class ReportService:
    def __init__(self, s3_settings_service: S3SettingsService):
        self.s3_settings_service = s3_settings_service

    @classmethod
    def raw_to_dojo_policy_reports(
            cls, detailed_report: Dict[str, List[Dict]], cloud: str
    ):
        """
        v3.3.1 Returns a Dojo compatible resources report out of a single
         non-formatted detailed report.
        :param detailed_report: Dict[str, List[Dict]]
        :param cloud: str
        :return: List[Dict]
        """
        report = []
        for region, detailed_reports in detailed_report.items():
            for detailed_report in detailed_reports:
                # detailed_custodian_run_log = detailed_report.get(
                #     'custodian-run', ''
                # )

                detailed_metadata = detailed_report.get('metadata', {})
                detailed_resources = detailed_report.get('resources', [])
                # detailed_errors = detailed_report.get('errors', [])

                policy = detailed_metadata.get("policy", {})

                resource_type = policy.get("resource", "")
                policy_metadata = policy.get("metadata", {})
                report_fields = policy_metadata.get("report_fields", [])

                # run_result = 'No errors'
                # if 'ERROR' in detailed_custodian_run_log:
                #    run_result = "Errors were found"

                _policy_name = policy.get('name', '')
                if region and region != "default":
                    _policy_name += ":" + region

                _multi_regional = policy_metadata.get("multiregional")
                if not _multi_regional:
                    _multi_regional = 'false'

                resources = cls._derive_policy_report_resources(
                    report_fields=report_fields,
                    resource_type=resource_type,
                    resources=detailed_resources,
                    name=_policy_name
                )

                severity = policy_metadata.get("severity", 'Medium').title()

                entity = {
                    "description": policy.get("description", "no description"),
                    "resources #": len(resources),
                    "region": region,
                    "multiregional": _multi_regional,
                    "resources": resources,
                    "remediation": policy_metadata.get("remediation"),
                    "impact": policy_metadata.get("impact"),
                    "standard": policy_metadata.get("standard"),
                    "severity": severity,
                    "article": policy_metadata.get("article"),

                    # DefectDojo: Finding Model unhandled/non-proxied keys
                    # "policy_name": _policy_name,
                    # "run_result": run_result,
                    # "errors": detailed_errors,

                    # DefectDojo: Finding Model safe-to-provide fields
                    # pre v3.3.1
                    "service": resource_type,
                    "vuln_id_from_tool": _policy_name,
                    "tags": [region],

                    # DefectDojo: Report-helper keys, omitted when pushed
                    "report_fields": report_fields
                }
                if cloud.upper() != 'AWS':
                    del entity["region"]
                    del entity["multiregional"]
                    del entity["tags"]

                report.append(entity)

        return report

    def formatted_to_dojo_policy_report(self,
                                        detailed_report: Dict[str, List[Dict]],
                                        cloud: Optional[str] = None
                                        ) -> List[PolicyReportItem]:
        """
        Returns a dojo policy report out of formatted, region-specific
        policy report.
        :param detailed_report: Dict[str, List[Dict]]
        :param cloud: Optional[str] = None
        :return: List[Dict]
        """
        policy_report: List[Dict] = []
        _human = self.s3_settings_service.human_data() or {}
        _severity = self.s3_settings_service.rules_to_severity() or {}
        for region, policies in detailed_report.items():
            for policy_scope in policies:
                policy = policy_scope.get('policy') or {}
                name = policy.get('name')
                resources = policy_scope.get('resources') or []

                _multi_regional = policy.get("multiregional")
                if not _multi_regional:
                    _multi_regional = 'false'

                resources = self._derive_policy_report_resources(
                    report_fields=_human.get(name, {}).get('report_fields'),
                    resource_type=policy.get('resourceType'),
                    resources=resources,
                    name=name
                )
                policy_report.append({
                    "description": policy.get("description"),
                    # "region": region,
                    # "multiregional": _multi_regional,
                    "resources": resources,
                    "remediation": _human.get(name, {}).get('remediation'),
                    "impact": _human.get(name, {}).get('impact'),
                    "standard": {},
                    "severity": _severity.get(name),
                    "article": _human.get(name, {}).get('article'),
                    "service": policy.get('resourceType'),
                    "vuln_id_from_tool": name,
                    "tags": [region],
                    "report_fields": _human.get(name, {}).get('report_fields')
                })
        return policy_report

    @staticmethod
    def _derive_policy_report_resources(
            name: str, resource_type: str, resources: List,
            report_fields: List[str]
    ):

        # No `custodian-run-log` to check within.
        # run_result = 'Unknown'

        skey = report_fields[0] if report_fields else None
        if skey:
            try:
                _resources = sorted(resources, key=lambda r: r[skey])
            except (BaseException, Exception) as e:
                msg = f'Sorting of resources, bound to \'{name}\' policy'
                msg += f', by {skey} has run into an issue: {e}.'
                msg += ' Using the unsorted ones.'
                _LOG.warning(msg)

        _resources = []
        for resource in resources:
            _resource = {}
            for resource_key, resource_value in resource.items():

                # `report_fields` are toggled during the Dojo upload.
                # ergo, keeping key-values pairs of all fields.

                if isinstance(resource_value, (str, int, float)):
                    _resource[resource_key] = resource_value

                elif resource_type.startswith("gcp."):
                    if type(resource_value) is dict:
                        for inner_key in resource_value:
                            _key = resource_key + "_" + inner_key
                            _resource[_key] = resource_value[inner_key]

                    elif type(resource_value) is list:
                        _len = len(resource_value) > 0
                        _target = resource_value[0]
                        _predicate = not isinstance(_target, dict)
                        # v3.3.1 todo check for all non-str values?
                        if _len and _predicate:
                            _resource[resource_key] = "\n".join(
                                resource_value
                            )

            _resources.append(_resource or resource)

        return _resources

    @staticmethod
    def tenant_findings_path(tenant: Tenant) -> str:
        return str(PurePosixPath(
            FINDINGS_FOLDER, utc_datetime().date().isoformat(),
            f'{tenant.project}.json'
        ))

    @staticmethod
    def platform_findings_path(platform: Parent) -> str:
        """
        Platform is a parent with type PLATFORM_K8S currently. It's meta
        can contain name and possibly region in case we talk about EKS
        :param platform:
        :return:
        """
        meta = platform.meta.as_dict()
        name = meta.get('name')
        region = meta.get('region') or 'no-region'
        return str(PurePosixPath(
            FINDINGS_FOLDER, utc_datetime().date().isoformat(), 'k8s',
            region, f'{name}.json'
        ))


class FindingsCollection:
    keys_to_keep: set = {'description', 'resourceType'}
    # currently rules does not contain report fields
    only_report_fields: bool = True

    def __init__(self, data: dict = None, rules_data: dict = None):
        self._data: Dict[tuple, set] = data or {}
        self._rules_data: Dict[str, Dict] = rules_data or {}

    @property
    def rules_data(self) -> dict:
        return self._rules_data

    @classmethod
    def from_detailed_report(cls, report: dict) -> 'FindingsCollection':
        """Imports data from detailed_report's format. In addition, collects
        the descriptions and other info about a rule."""
        result, rules_data = {}, {}
        for region, region_policies in report.items():
            for policy in region_policies:
                p_data = policy['policy']
                if p_data['name'] not in rules_data:  # retrieve rules info
                    rules_data[p_data['name']] = filter_dict(
                        p_data, cls.keys_to_keep)
                result.setdefault((p_data['name'], region), set())
                for resource in policy.get('resources', []):
                    result[(p_data['name'], region)].add(
                        hashable(filter_dict(resource, set()))
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

    def json(self) -> str:
        return json.dumps(self.serialize(), separators=(',', ':'))

    def update(self, other: 'FindingsCollection') -> None:
        self._data.update(other._data)
        self._rules_data.update(other._rules_data)

        # ----- patch -----
        # removing AzureCloud from findings
        for rule, region in list(self._data.keys()):
            if region == AZURE_COMMON_REGION:
                self._data.pop((rule, region))
        # ----- patch -----

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
