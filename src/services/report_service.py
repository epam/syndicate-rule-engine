import json
import os
import tempfile
from datetime import datetime
from pathlib import PurePosixPath
from typing import Dict, List, Optional, Union, TypedDict

from xlsxwriter import Workbook
from xlsxwriter.utility import xl_col_to_name

from helpers.log_helper import get_logger
from helpers.reports import Standard, FindingsCollection
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService
from services.rule_meta_service import LazyLoadedMappingsCollector
_LOG = get_logger(__name__)

DETAILED_REPORT_FILE = 'detailed_report.json'
USER_REPORT_FILE = 'user_detailed_report.json'
DIGEST_REPORT_FILE = 'report.json'

STATISTICS_FILE = 'statistics.json'
API_CALLS_FILE = 'api_calls.json'

KEYS_TO_EXCLUDE_FOR_USER = {'standard_points', }

Coverage = Dict[str, Dict[str, float]]

# Failed rule types.
ACCESS_TYPE = 'access'
CORE_TYPE = 'core'


class PolicyReportItem(TypedDict):
    """
    Incoming data
    """
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
    def __init__(self, s3_client: S3Client,
                 environment_service: EnvironmentService,
                 mappings_collector: LazyLoadedMappingsCollector):
        self.s3_client = s3_client
        self.environment_service = environment_service
        self.mappings_collector = mappings_collector

    @property
    def job_report_bucket(self):
        return self.environment_service.default_reports_bucket_name()

    def pull_job_statistics(self, path: str):
        bucket_name = self.environment_service.get_statistics_bucket_name()
        path = path.replace(':', '_')
        _LOG.info(f'Pulling {path} of job-source, within {bucket_name}.')
        try:
            data = self.s3_client.get_json_file_content(
                bucket_name=bucket_name, full_file_name=path
            )
        except (BaseException, Exception) as e:
            _LOG.error(f'{path} of job-source could not be pulled,'
                       f' due to - {e}.')
            data = None
        return data

    def pull_job_report(self, path: str):
        bucket_name = self.environment_service.default_reports_bucket_name()
        path = path.replace(':', '_')
        _LOG.info(f'Pulling {path} of job-source, within {bucket_name}.')
        try:
            data = self.s3_client.get_json_file_content(
                bucket_name=bucket_name, full_file_name=path
            )
        except (BaseException, Exception) as e:
            _LOG.error(f'{path} of job-source could not be pulled,'
                       f' due to - {e}.')
            data = None
        return data

    # def pull_job_findings(self, job_id: str):
    #     # todo thread-safe.
    #     reports_bucket_name = self.job_report_bucket
    #     findings_key = str(PurePosixPath(job_id, 'findings'))
    #     keys = (
    #         k for k in
    #     self.s3_client.list_dir(reports_bucket_name, findings_key)
    #         if k.endswith('.json') or k.endswith('.json.gz')
    #     )
    #     findings = []
    #     for _, file in self.s3_client.get_json_batch(
    #         reports_bucket_name, keys
    #     ):
    #         findings.extend(file)
    #     return findings

    def put_job_report(self, data: dict, path: str):
        bucket_name = self.environment_service.default_reports_bucket_name()
        path = path.replace(':', '_')
        _LOG.info(f'Putting {path} job-source data, within {bucket_name}.')
        try:
            data = self.s3_client.put_object(
                bucket_name=bucket_name, object_name=path,
                body=json.dumps(data)
            )
        except (BaseException, Exception) as e:
            _LOG.error(f'Data, could not be put within {path}, due to - {e}.')
            data = None
        return data

    def href_job_report(self, path: str, check: bool = True):
        bucket_name = self.environment_service.default_reports_bucket_name()
        path = path.replace(':', '_')

        if check:
            _LOG.info(f'Verifying whether {path} of {bucket_name} exists.')
            if not self.s3_client.file_exists(
                    bucket_name=bucket_name, key=path
            ):
                return None

        _LOG.info(f'Generating presigned-url of {path}, within {bucket_name}.')
        try:
            url = self.s3_client.generate_presigned_url(
                bucket_name=bucket_name, full_file_name=path
            )
        except (BaseException, Exception) as e:
            _LOG.error(f'Presigned URL of {path} path, could not be generated'
                       f' due to - {e}.')
            url = None
        return url

    def pull_concrete_report(self, path: str):
        bucket_name = self.environment_service.default_reports_bucket_name()
        path = path.replace(':', '_')
        _LOG.info(f'Pulling {path} of job-source, within {bucket_name}.')
        try:
            data = self.s3_client.get_json_file_content(
                bucket_name=bucket_name, full_file_name=path
            )
        except (BaseException, Exception) as e:
            _LOG.error(f'{path} of job-source could not be pulled,'
                       f' due to - {e}.')
            data = None
        return data

    def put_json_concrete_report(self, data: Union[Dict, List], path: str):
        bucket_name = self.environment_service.default_reports_bucket_name()
        _LOG.info(f'Putting {path} file within {bucket_name}.')
        return self.s3_client.put_object(
            bucket_name=bucket_name, object_name=path, body=json.dumps(data)
        )

    def put_path_retained_concrete_report(self, stream_path: str,
                                          object_path: str):
        bucket_name = self.environment_service.default_reports_bucket_name()
        path = object_path
        _LOG.info(f'Reading {stream_path}, meant for {path}.')
        with open(stream_path, 'rb') as f:
            body = f.read()
        if body:
            _LOG.info(f'Putting {path} file within {bucket_name}.')
            return self.s3_client.put_object(
                bucket_name=bucket_name, object_name=path, body=body
            )

    def href_concrete_report(self, path: str, check: bool = True):
        bucket_name = self.environment_service.default_reports_bucket_name()
        if check:
            _LOG.info(f'Verifying whether {path} of {bucket_name} exists.')
            if not self.s3_client.file_exists(
                    bucket_name=bucket_name, key=path
            ):
                return None

        _LOG.info(f'Generating presigned-url of {path}, within {bucket_name}.')
        try:
            url = self.s3_client.generate_presigned_url(
                bucket_name=bucket_name, full_file_name=path
            )
        except (BaseException, Exception) as e:
            _LOG.error(f'Presigned URL of {path} path, could not be generated'
                       f' due to - {e}.')
            url = None
        return url

    def check_concrete_report(self, path: str):
        bucket_name = self.environment_service.default_reports_bucket_name()
        _LOG.info(f'Checking whether {path} file within {bucket_name} exists.')
        return self.s3_client.file_exists(bucket_name=bucket_name, key=path)

    @staticmethod
    def derive_name_of_report_object_path(object_path: str):
        return PurePosixPath(object_path).name

    @staticmethod
    def derive_digests_report_object_path(
            entity_attr: str, entity_value: str, start: datetime, end: datetime
    ):
        time_range = f'{start.timestamp()}_{end.timestamp()}'
        time_range = time_range.replace('.', '-')
        key = f'{entity_attr}_digests_report_{entity_value}_{time_range}.json'
        return str(PurePosixPath('digests', key))

    @staticmethod
    def derive_details_report_object_path(
            entity_attr: str, entity_value: str, start: datetime, end: datetime
    ):
        time_range = f'{start.timestamp()}_{end.timestamp()}'
        time_range = time_range.replace('.', '-')
        key = f'details_report_{entity_attr}_{entity_value}_{time_range}.json'
        return str(PurePosixPath('details_report', key))

    @staticmethod
    def derive_compliance_report_object_path(
            entity_attr: Optional[str] = None,
            entity_value: Optional[str] = None,
            start: Optional[datetime] = None, end: Optional[datetime] = None,
            job_id: Optional[str] = None, fext: str = 'xlsx'
    ):
        named = (entity_attr and entity_value)
        ranged = (named and (start and end))
        # Backward compatible with the previous implementation.
        assert named or ranged or job_id, \
            'Report must either be range, job or named specific.'

        key = 'compliance_report'
        if job_id:
            key += f'_{job_id}'
        else:
            key += f'_{entity_attr}_{entity_value}'
            if ranged:
                time_range = f'{start.isoformat()}_{end.isoformat()}'
                time_range = time_range.replace('.', '-')
                key += f'_{time_range}'
        key += f'.{fext}'
        return str(PurePosixPath('compliance_report', key))

    @staticmethod
    def derive_error_report_object_path(
            subtype: Optional[str] = None,
            entity_attr: Optional[str] = None,
            entity_value: Optional[str] = None,
            start: Optional[datetime] = None,
            end: Optional[datetime] = None,
            job_id: Optional[str] = None,
            fext: str = 'xlsx'
    ):
        named = (entity_attr and entity_value)
        ranged = (named and (start and end))

        # Backward compatible with the previous implementation.
        assert named or ranged or job_id, \
            'Report must either be range, job or named specific.'

        key = 'error'
        if subtype:
            key += f'_{subtype}'
        key += '_report'

        if job_id:
            key += f'_{job_id}'
        else:
            key += f'_{entity_attr}_{entity_value}'
            if ranged:
                time_range = f'{start.isoformat()}_{end.isoformat()}'
                time_range = time_range.replace('.', '-')
                key += f'_{time_range}'

        key += f'.{fext}'
        return str(PurePosixPath('error_report', key))

    @staticmethod
    def derive_rule_report_object_path(
            entity_attr: Optional[str] = None,
            entity_value: Optional[str] = None,
            start: Optional[datetime] = None,
            end: Optional[datetime] = None,
            job_id: Optional[str] = None,
            fext: str = 'xlsx'
    ):
        named = (entity_attr and entity_value)
        ranged = (named and (start and end))

        # Backward compatible with the previous implementation.
        assert named or ranged or job_id, \
            'Report must either be range, job or named specific.'

        key = 'rule_report'

        if job_id:
            key += f'_{job_id}'
        else:
            key += f'_{entity_attr}_{entity_value}'
            if ranged:
                time_range = f'{start.isoformat()}_{end.isoformat()}'
                time_range = time_range.replace('.', '-')
                key += f'_{time_range}'

        key += f'.{fext}'
        return str(PurePosixPath('rule_report', key))

    @staticmethod
    def derive_job_object_path(job_id: str, typ: str):
        _files = (
            DIGEST_REPORT_FILE, DETAILED_REPORT_FILE, USER_REPORT_FILE,
            STATISTICS_FILE, API_CALLS_FILE
        )
        assert typ in _files, f'{typ} file is not job recognizable.'
        return str(PurePosixPath(job_id, typ))

    @staticmethod
    def derive_findings_from_report(
            report: dict, user_detailed: bool
    ):
        kte = None if user_detailed else list(KEYS_TO_EXCLUDE_FOR_USER)
        return FindingsCollection.from_detailed_report(
            report=report, only_report_fields=False,
            retain_all_keys=True,
            keys_to_exclude=kte
        )

    @staticmethod
    def generate_digest(detailed_report: dict):
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

    @staticmethod
    def derive_failed_rule_map(
            job_id: str, statistics: List[Dict]
    ) -> Optional[Dict[str, List[Dict]]]:
        ref = {}
        for rule in statistics:
            rid = rule.get('id')
            is_failed = 'reason' in rule or rule.get('status') == 'FAILED'
            if not rid or not is_failed:
                continue
            scope = ref.setdefault(rid, [])
            scope.append(
                {
                    job_id: {
                        'account_name': rule.get('account_display_name'),
                        'tenant': rule.get('tenant_display_name'),
                        'region': rule.get('region'),
                        'failure_reason': rule.get('reason'),
                        'traceback': rule.get('traceback',
                                              'Non-source error'),
                        # 'cw_logs_snippet': cw_logs
                    }
                }
            )
        return ref

    @staticmethod
    def derive_type_based_failed_rule_map(
            typ: str, failed_rule_map: Dict[str, List[Dict]]
    ) -> Optional[Dict[str, List[Dict]]]:

        value_typ_ref = {
            'Non-source error': ACCESS_TYPE
        }
        typed: Dict[str, List[Dict]] = {}
        for rule_id, rule_errors in failed_rule_map.items():
            for rule_error in rule_errors:
                for job_id, error in rule_error.items():
                    traceback = error.get('traceback')
                    # Defaults to `core`.
                    to_typ = value_typ_ref.get(traceback, CORE_TYPE)
                    if to_typ == typ:
                        rule_scope = typed.setdefault(rule_id, [])
                        rule_scope.append({job_id: error})
        return typed

    @staticmethod
    def derive_clean_filtered_statistics_list(
            statistic_list: List[List[Dict]], target_rule: Optional[str] = None
    ):
        result: Union[List[List[Dict]], list] = []
        if target_rule:
            for stat_item in statistic_list:
                temp = []
                for rule in stat_item:
                    if rule.get('id') == target_rule:
                        temp.append(rule)
                result.append(temp)

        # Cleans up statistics, by removing duplicates
        result = statistic_list if not result else result
        for account in result:
            duplicates = []
            for index, stat in enumerate(account, start=0):
                if any(word in ' '.join(stat.get('id').split('_')) for word in
                       ('iam', 's3', 'mfa', 'access key', 'cloudfront')):
                    if stat.get('id') in duplicates:
                        del account[index]
                    else:
                        duplicates.append(stat.get('id'))
        return result

    @staticmethod
    def average_out_statistics(statistics_list: List[List[Dict]],
                               list_format=True):
        full_statistics = []
        metrics = {}
        for stat_list in statistics_list:
            full_statistics.extend(stat_list)

        for rule in full_statistics:
            rid = rule.get('id')
            rule_scope = metrics.setdefault(rid, [])
            rule_scope.append(rule)

        if list_format:
            result_data = []
        else:
            result_data = {}
        for rule_name, rules in metrics.items():
            result_item = {
                'invocations': len(rules)
            }
            success = 0
            failed = 0
            skipped = 0
            resources_scanned = 0
            violating_resources = 0
            min_execution_time = float('inf')
            max_execution_time = 0.0
            total_execution_time = 0.0
            for rule in rules:
                elapsed_time = float(rule.get('elapsed_time')) if rule.get(
                    'elapsed_time') else 0
                resources_scanned += int(rule.get('resources_scanned')) \
                    if rule.get('resources_scanned') else 0
                violating_resources += len(
                    rule.get('failed_resources')) if rule.get(
                    'failed_resources') else 0
                total_execution_time += elapsed_time
                min_execution_time = min_execution_time if \
                    min_execution_time < elapsed_time or \
                    elapsed_time == 0 else elapsed_time
                max_execution_time = max_execution_time if \
                    max_execution_time > elapsed_time else elapsed_time
                success += 1 if rule.get('status') == 'SUCCEEDED' else 0
                failed += 1 if rule.get('status') == 'FAILED' else 0
                skipped += 1 if rule.get('status') == 'SKIPPED' else 0

            result_item.update({
                'succeeded': success,
                'failed': failed,
                'skipped': skipped,
                'resources_scanned': resources_scanned,
                'violating_resources': violating_resources,
                'min_exec': round(
                    min_execution_time, 3) if min_execution_time != float(
                    'inf') else 0.0,
                'max_exec': round(max_execution_time, 3),
                'total_exec': round(total_execution_time, 3)
            })
            if list_format:
                result_item.update({'policy_name': rule_name})
                result_data.append(result_item)
            else:
                result_data.update({rule_name: result_item})
        return result_data

    @staticmethod
    def accumulate_digest(digest_list: List[Dict[str, int]]):
        keys = (
            'total_checks_performed', 'successful_checks',
            'failed_checks', 'total_resources_violated_rules'
        )
        accumulated = {}
        for digest in digest_list:
            for key in keys:
                pending = accumulated.get(key, 0)
                value = digest.get(key)
                if isinstance(value, int):
                    accumulated[key] = pending + value
        return accumulated

    @classmethod
    def accumulate_details(
            cls, detailed_report_list: List[Dict], user_detailed: bool
    ) -> Optional[FindingsCollection]:
        if not detailed_report_list:
            return
        base = cls.derive_findings_from_report(
            report=detailed_report_list[0], user_detailed=user_detailed
        )
        for report in detailed_report_list[1:]:
            base.update(
                other=cls.derive_findings_from_report(
                    report=report, user_detailed=user_detailed
                )
            )
        return base

    @staticmethod
    def accumulate_compliance(
            coverages: List[Coverage],
            regions: Optional[List[str]] = None
    ) -> Coverage:
        """
        Aggregates coverages, by averaging out standard-specific
        points within regions.

        :param coverages: List[Dict[str, Dict[str, float]]]
        :param regions: Optional[List[str]], denotes regions to derive for,
         given None - assumes to calculate for any region.
        :param: Dict[str, Dict[str, float]]
        """
        averaged = {}
        if not coverages:
            return averaged

        # Maintains accumulation counter.
        count_ref: Dict[str, Dict[str, int]] = {}
        for pending in coverages:
            for region in pending:

                if regions and region not in regions:
                    continue

                region_scope = averaged.get(region, {})
                for standard, value in pending[region].items():
                    if standard in region_scope:
                        # Given standard already seen, start counter at 2
                        rg_scope = count_ref.setdefault(region, {})
                        rg_scope[standard] = rg_scope.get(standard, 1) + 1
                    retained = region_scope.get(standard, 0)
                    region_scope[standard] = retained + value

                if region_scope:
                    averaged[region] = region_scope

        for region in count_ref:
            for standard, count in count_ref[region].items():
                averaged[region][standard] /= count

        return averaged

    @staticmethod
    def derive_compliance_report_excel_path(
            file_name, coverages, standards_coverage
    ) -> str:

        path = os.path.join(tempfile.gettempdir(), file_name)

        wb = Workbook(path)
        percent_fmt = wb.add_format({'num_format': '0.00%'})

        report_ws = wb.add_worksheet("Compliance Report")
        coverage_ws = wb.add_worksheet("Standards Coverage")
        to_write = []
        _standards = set()  # to be able to get length and format cells
        for region, standards in coverages.items():
            to_write.append({
                'region': region,
                **{k: standards[k] for k in sorted(standards)}
            })
            _standards.update(standards.keys())

        # Write lists of dicts.
        data = to_write
        breakdown_field = None
        headers = []
        for item in data:
            headers.extend(item.keys())
        headers = list(dict.fromkeys(headers))
        row = 1
        for item_index, item in enumerate(data):
            for index, key in enumerate(headers):
                value = item.get(key, '')
                report_ws.write(row, index, value)
            if breakdown_field and (len(data) - 1 != item_index) \
                    and (data[item_index + 1].get(breakdown_field) != data[
                item_index].get(breakdown_field)):
                row += 1
            row += 1

        # Amends headers
        headers = [header.replace('_', ' ').capitalize() for header in headers]
        first_row = 0
        for index, header in enumerate(headers):
            report_ws.write(first_row, index, header)

        # formatting necessary cells to percentage and writing averages
        row = len(to_write) + 1
        report_ws.write(row, 0, 'AVERAGE')
        report_ws.conditional_format(1, 1, row, len(_standards), {
            'type': 'cell', 'criteria': '>=', 'value': 0, 'format': percent_fmt
        })
        for i in range(1, len(_standards) + 1):
            col = xl_col_to_name(i)
            report_ws.write_formula(row, i,
                                    f'{{=AVERAGE({col}2:{col}{row})}}')
        # writing the maximum available percentage to a separate sheet
        for index, standard_name in enumerate(sorted(standards_coverage)):
            for version, info in standards_coverage[standard_name].items():
                coverage_ws.write(0, index,
                                  Standard(standard_name, version).full_name)
                coverage_ws.write(1, index, info.get('%'), percent_fmt)

        wb.close()
        return path

    @staticmethod
    def derive_errors_report_excel_path(
            file_name: str, failed_rules: Dict[str, List[Dict]],
            subtype: Optional[str] = None
    ):
        subtype_to_sheet_ref = {
            ACCESS_TYPE: 'Access Errors',
            CORE_TYPE: 'Core Errors'
        }

        headers = ['rule', 'account_name', 'job_id', 'region',
                   'failure_reason', 'traceback']
        errors_report = []
        for rule, errors in failed_rules.items():
            for job_errors in errors:
                for job_id, job_error in job_errors.items():
                    job_error['rule'] = rule
                    job_error['job_id'] = job_id
                    errors_report.append(job_error)

        if os.name == 'nt':
            # there are problems with xlsx files on Windows if there is
            # a colon in the name
            file_name = file_name.replace(':', '_')
        path = os.path.join(tempfile.gettempdir(), file_name)

        wb = Workbook(filename=path)
        sheet_name = subtype_to_sheet_ref.get(subtype, 'Execution Errors')
        ws = wb.add_worksheet(sheet_name)
        first_row = 0
        for index, header in enumerate(headers):
            ws.write(first_row, index, header)
        row = 1
        for item in errors_report:
            for index, key in enumerate(headers):
                value = item.get(key, '')
                ws.write(row, index, value)
            row += 1

        wb.close()
        return path

    @staticmethod
    def derive_rule_statistics_report_xlsx_path(
            file_name: str, averaged_statistics: List[Dict]
    ):
        """
        Writes to xlsx
        :param file_name:
        :param averaged_statistics:
        :return:
        """
        headers = [
            'policy_name', 'invocations', 'succeeded', 'failed', 'skipped',
            'resources_scanned', 'violating_resources', 'min_exec', 'max_exec',
            'total_exec'
        ]
        if os.name == 'nt':
            # there are problems with xlsx files on Windows if there is
            # a colon in the name
            file_name = file_name.replace(':', '_')
        path = os.path.join(tempfile.gettempdir(), file_name)
        wb = Workbook(path)
        ws = wb.add_worksheet('Rules statistics')
        first_row = 0
        for index, header in enumerate(headers):
            ws.write(first_row, index, header)
        row = 1
        for item in averaged_statistics:
            for index, key in enumerate(headers):
                value = item.get(key, '')
                ws.write(row, index, value)
            row += 1
        wb.close()
        return path

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
        _human = self.mappings_collector.human_data or {}
        _severity = self.mappings_collector.severity or {}
        for region, policies in detailed_report.items():
            for policy_scope in policies:
                policy = policy_scope.get('policy') or {}
                name = policy.get('name')
                resources = policy_scope.get('resources') or []

                # _cloud = cloud
                # if not cloud and '.' in resource_type:
                #     _cloud, _ = resource_type.split(sep='.', maxsplit=1)

                # No `custodian-run-log` to check within.
                # run_result = 'Unknown'

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
                    "description": policy.get('description'),
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
            name: str,  resource_type: str, resources: List[Dict],
            report_fields: List[str]) -> List[Dict]:

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
