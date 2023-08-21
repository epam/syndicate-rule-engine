import csv
import json
import os
from typing import Dict, Generator, List, Optional, TypedDict

from helpers import deep_get, filter_dict, hashable
from helpers.constants import POLICY_KEYS_MAPPING, CLOUD_TO_FOLDER_MAPPING, \
    PATTERN_FOR_JSON_SUBSTRING, FINDINGS_FOLDER, \
    AZURE_COMMON_REGION, MULTIREGION
from helpers.log_helper import get_logger
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService
from services.os_service import OSService
from services.s3_settings_service import S3SettingsService

_LOG = get_logger(__name__)

RESOURCE_NAME_HEADER = 'Resource Name'
RESOURCE_TYPE_HEADER = 'Resource Type'
REGION_HEADER = 'Region'
RULE_NAME_HEADER = 'Rule Name'
RULE_DESCRIPTION_HEADER = 'Rule Description'

DETAILED_REPORT_FILE = 'detailed_report.json'
REPORT_FILE = 'report.json'  # `digest-report`
DIFFERENCE_FILE = 'difference.json'


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


class ReportService:
    def __init__(self, os_service: OSService, s3_client: S3Client,
                 environment_service: EnvironmentService,
                 s3_settings_service: S3SettingsService):
        self.os_service = os_service
        self.s3_client = s3_client
        self.environment_service = environment_service
        self.s3_settings_service = s3_settings_service
        self._resources_mapping = {}

    def resources_mapping(self, cloud: str) -> dict:
        if cloud not in self._resources_mapping:
            self._resources_mapping[cloud] = \
                self.os_service.get_resource_mapping(cloud)
        return self._resources_mapping[cloud]

    @staticmethod
    def generate_report(detailed_report):
        total_checks_performed = 0
        failed_checks = 0
        successful_checks = 0
        total_resources_violated_rules = 0
        for region, reports in detailed_report.items():
            region_total = len(reports)
            failed_summaries = [summary for summary in reports if
                                summary.get('resources')]
            region_failed = len(failed_summaries)
            region_successful = region_total - region_failed
            total_checks_performed += region_total
            failed_checks += region_failed
            successful_checks += region_successful
            total_resources_violated_rules += sum(
                len(summary.get('resources'))
                for summary in failed_summaries)
        return {
            'total_checks_performed': total_checks_performed,
            'successful_checks': successful_checks,
            'failed_checks': failed_checks,
            'total_resources_violated_rules': total_resources_violated_rules
        }

    def generate_detailed_report(self, work_dir):
        """
        Produces region-specific object of un-formatted custodian detailed
        reports, found within a working directory.
        Note: does not retain error (failed to execute) reports.
        :param work_dir: str
        :return: Dict[str, List[Dict]] {
            $region: [
                {
                    'custodian-run': str,
                    'metadata': Dict,
                    'resources': List[Dict]
                }
            ]
        }
        """
        _LOG.debug(f'Generating detailed report. Workdir: {work_dir};')
        detailed_report_map = {}
        region_folders = (folder for folder in os.listdir(work_dir)
                          if folder != FINDINGS_FOLDER)

        for region_folder in region_folders:
            if os.path.isfile(os.path.join(work_dir, region_folder)):
                continue
            region_detailed_report = self._generate_region_report(
                region_folder=region_folder,
                work_dir=work_dir
            )
            detailed_report_map[region_folder] = region_detailed_report

        return detailed_report_map

    def _generate_region_report(self, region_folder, work_dir):
        _LOG.debug(f'Processing region \'{region_folder}\'')
        region_detailed_report = []
        region_work_dir = os.path.join(work_dir, region_folder)

        for policy_folder in os.listdir(region_work_dir):
            entity = self._format_region_report_item(
                policy_folder=policy_folder,
                region_work_dir=region_work_dir
            )
            if entity:
                region_detailed_report.append(entity)
        return region_detailed_report

    def format_detailed_report(self, detailed_report: dict, cloud_name: str):
        result_report = {region: list() for region in detailed_report}

        for region, report_items in detailed_report.items():
            _LOG.debug(f'Processing region: {region}')
            result_report[region] = list(self._format_region(
                report_items, cloud_name
            ))
        return result_report

    def _format_region(self, region_report: list, cloud: str) -> Generator[
        Dict, None, None
    ]:
        """
        :param region_report: List[Dict] - [
            {
                'custodian-run': custodian_run,
                'metadata': metadata,
                'resources': resources
            }
        ]
        :return: Generator[Dict, None, None]
        """
        return (
            self._format_item(item, cloud)
            for item in region_report
        )

    def _format_item(self, item: dict, cloud: str):
        """
        :param item: {
            'custodian-run': custodian_run,
            'metadata': metadata,
            'resources': resources
        }
        :return: Dict
        """
        formatted_item = {'policy': {}, 'resources': []}

        policy_meta = item['metadata']['policy']
        for target_key, report_key in POLICY_KEYS_MAPPING.items():
            keys = report_key.split('__')
            report_value = deep_get(policy_meta, keys)
            if report_value:
                if isinstance(report_value, str):
                    report_value = report_value.strip()
                formatted_item['policy'][target_key] = report_value

        resources = item.get('resources')
        if not resources:
            return formatted_item
        item_resources = []
        resources_mapping = self.resources_mapping(cloud)
        for resource in resources:
            resource_type = formatted_item['policy'].get('resourceType')
            if '.' in resource_type:
                resource_type = resource_type[resource_type.index('.') + 1:]

            cloud_folder = CLOUD_TO_FOLDER_MAPPING.get(cloud)
            resource_map_key = f'{cloud_folder}.{resource_type}'
            resource_keys_mapping = resources_mapping.get(resource_map_key)

            if resource_keys_mapping:
                item_resource = {target_key: resource.get(report_key)
                                 for target_key, report_key
                                 in resource_keys_mapping.items()
                                 if resource.get(report_key)}
                resource.update(item_resource)
            else:
                _LOG.warning(f'{cloud} resource: \'{resource_type}\' doesn\'t '
                             f'have an associated value in ResourceMap. Make '
                             f'sure resource_mappings files are up-to-date!')
            item_resources.append(resource)
        formatted_item['resources'] = item_resources
        return formatted_item

    def _format_region_report_item(
            self, policy_folder, region_work_dir
    ):

        _LOG.debug(f'Processing {policy_folder}')
        policy_path = os.path.join(region_work_dir, policy_folder)

        run_path = os.path.join(policy_path, 'custodian-run.log')
        metadata_path = os.path.join(policy_path, 'metadata.json')
        resources_path = os.path.join(policy_path, 'resources.json')

        entity = None

        if (os.path.exists(run_path) and os.path.exists(metadata_path)
                and os.path.exists(resources_path)):
            custodian_run = self.os_service.read_file(
                file_path=run_path,
                json_content=False
            )
            metadata = self.os_service.read_file(file_path=metadata_path)
            resources = self.os_service.read_file(file_path=resources_path)
            entity = {
                'custodian-run': custodian_run,
                'metadata': metadata,
                'resources': resources
            }

        # v3.3.1 Error data is unreachable, see - _assemble_report_errors.
        return entity

    @staticmethod
    def _assemble_report_errors(policy_name, region_run_output):
        # v3.3.1
        # todo obsolete, as policy-run output is not attainable separately.
        #  Given error reports are required, has to:
        #   - persist each policy specific output
        #   - mention failed-to-execute policy name

        _LOG.debug(f'Assembling errors of \'{policy_name}\' policy execution.')
        output = region_run_output[policy_name]
        policy_strings = PATTERN_FOR_JSON_SUBSTRING.findall(output)

        policy = {"name": policy_name}
        if len(policy_strings) > 0:
            try:
                policy = json.loads(policy_strings[0])
            except (BaseException, Exception) as e:
                _LOG.warning(f'Output of \'{policy_name}\' policy is not'
                             f' JSON serializable, due to {e}.')
        if len(policy_strings) > 0:
            output = output.replace(policy_strings[0], '')

        output_list = output.split("\n")
        output_list = [output for output in output_list if
                       output.strip() != ""]

        entity = {
            "custodian-run": "\n".join(output_list),
            "metadata": {'policy': policy},
            "resources": [],
            "errors": output_list
        }

        return entity

    def reformat_azure_report(self, detailed_report: dict,
                              target_regions: Optional[set] = None):
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
        :param detailed_report:
        {
            'AzureCloud': [{..., 'resources': [{'location': 'eastus'}]}, {}]
        }
        :param target_regions: {'eastus', 'westus2'}. If empty, all the
        location are kept. All the resources that does not contain
        'location' will be congested to 'multiregion' region. Multiregion
        resources are always kept
        :return:
        """
        assert len(
            detailed_report) == 1, f'Azure scans must embrace ' \
                                   f'one "region" ({AZURE_COMMON_REGION})'
        key = list(detailed_report)[0]
        if key != AZURE_COMMON_REGION:
            _LOG.warning(
                f'Common region somehow became {key} '
                f'instead of {AZURE_COMMON_REGION}')

        result = {}
        for rule in detailed_report[key]:
            metadata = rule.get('metadata') or {}
            custodian_run = rule.get('custodian-run') or ''
            resources = rule.get('resources') or []
            if not resources:  # to multi-region
                result.setdefault(MULTIREGION, []).append({
                    'metadata': metadata,
                    'custodian-run': custodian_run,
                    'resources': resources
                })
                continue
            # some resources found. Dispatching them to different locations
            _loc_resources = {}  # location to resources for the current rule
            for res in resources:
                res_loc = res.get('location') or MULTIREGION
                if res_loc != MULTIREGION and target_regions \
                        and res_loc not in target_regions:
                    _LOG.info(f'Skipping resource with loc: {res_loc}')
                    continue
                _loc_resources.setdefault(res_loc, []).append(res)
            for _loc, _resources in _loc_resources.items():
                result.setdefault(_loc, []).append({
                    'metadata': metadata,
                    'custodian-run': custodian_run,
                    'resources': _resources
                })
        return result

    @staticmethod
    def findings_to_csv(findings: 'FindingsCollection'):
        data = []

        for rule, v in findings.serialize().items():
            description = v.get('description')
            resource_type = v.get('resourceType')
            for region, resources in v.get('resources', {}).items():
                for resource_name in resources:
                    _name = ', '.join([v for v in resource_name.values()
                                       if isinstance(v, str)])
                    if not _name:
                        _LOG.warning(f'Report fields not found in in rule: '
                                     f'\'{rule}\';\n'
                                     f'Resources: \'{resources}\'')
                    data.append({
                        RESOURCE_NAME_HEADER: _name,
                        RESOURCE_TYPE_HEADER: resource_type,
                        REGION_HEADER: region,
                        RULE_NAME_HEADER: rule,
                        RULE_DESCRIPTION_HEADER: description})
        data = sorted(data, key=lambda d: d[RESOURCE_NAME_HEADER])

        with open('temp_workbook.csv', 'w', newline='') as csvfile:
            _LOG.debug('CSV-file assembling')
            headers = [
                RESOURCE_NAME_HEADER, RESOURCE_TYPE_HEADER, REGION_HEADER,
                RULE_NAME_HEADER, RULE_DESCRIPTION_HEADER]
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            writer.writerows(data)

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
