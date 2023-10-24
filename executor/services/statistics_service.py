import json
import os
from collections import Counter
from datetime import datetime
from pathlib import PurePosixPath

from modular_sdk.models.tenant import Tenant

from helpers.constants import STEP_COLLECT_STATISTICS, FINDINGS_FOLDER
from helpers.exception import ExecutorException
from helpers.log_helper import get_logger
from helpers.time_helper import utc_iso
from services.environment_service import EnvironmentService
from services.s3_service import S3Service

_LOG = get_logger(__name__)
STATISTICS_FILE = 'statistics.json'
API_CALLS_FILE = 'api_calls.json'


class StatisticsService:
    def __init__(self, s3_service: S3Service,
                 environment_service: EnvironmentService):
        self.s3_service = s3_service
        self.environment_service = environment_service

    @property
    def statistics_bucket(self) -> str:
        return self.environment_service.statistics_bucket_name()

    def collect_statistics(self, work_dir, failed_policies, skipped_policies,
                           tenant: Tenant):
        items = []
        resources = {}
        result_api_calls = {}

        folders = [folder for folder in os.listdir(work_dir)
                   if folder != FINDINGS_FOLDER]
        for folder in folders:
            if os.path.isfile(os.path.join(work_dir, folder)):
                continue
            folder_items, folder_resources, api_calls = \
                self._collect_statistics_in_folder(
                    work_dir=work_dir,
                    folder=folder,
                    failed_policies=failed_policies,
                    skipped_policies=skipped_policies,
                    tenant=tenant
                )
            items.extend(folder_items)
            for resource, count in folder_resources.items():
                if resource in resources:
                    resources[resource] += count
                else:
                    resources[resource] = count
            result_api_calls = dict(
                Counter(result_api_calls) + Counter(api_calls))
            _LOG.debug(f'Result API calls dict: {result_api_calls}')

        # Go through the list of skipped policies in case they were skipped
        # before the policy was processed, so the metadata folder was not
        # created
        for region, rule in skipped_policies.items():
            if not rule:
                continue
            for rule_id, reason in rule.items():
                item = {
                    'id': rule_id,
                    'region': region,
                    'status': 'SKIPPED',
                    'reason': reason.get('reason'),
                    'tenant_display_name': tenant.name,
                    'customer_display_name': tenant.customer_name
                }
                items.append(item)

        # we used to save resources amount to account (not tenant) here,
        # but we can calculate it dynamically from findings, so need
        # new_acc = self.account_service.get_by_uuid(account.id)
        # new_acc.resources_amount = resources
        # new_acc.save()
        return items, result_api_calls

    def upload_statistics(self, statistics):
        stats_path = str(PurePosixPath(
            self.environment_service.batch_job_id(), 'statistics.json'))

        stats_log = [f'{item.get("id", "None")}:{item.get("status", "None")}'
                     for item in statistics]
        _LOG.debug(F"Statistics short content: {stats_log}")
        self.s3_service.put_json_object(
            bucket_name=self.statistics_bucket,
            file_key=stats_path,
            content=statistics
        )
        _LOG.debug(f'The file \'statistics.json\' was uploaded '
                   f'to the bucket {self.statistics_bucket}: {stats_path}')

    def upload_api_calls_statistics(self, api_calls):
        api_calls_path = str(PurePosixPath(
            self.environment_service.batch_job_id(), 'api_calls.json'))

        _LOG.debug(f'API calls content: {api_calls}')
        self.s3_service.put_json_object(
            bucket_name=self.statistics_bucket,
            file_key=api_calls_path,
            content=api_calls
        )
        _LOG.debug(f'The file \'api_calls.json\' was uploaded '
                   f'to the bucket {self.statistics_bucket}: {api_calls_path}')

    def _collect_statistics_in_folder(self, work_dir, folder,
                                      failed_policies, skipped_policies,
                                      tenant: Tenant):
        items = []
        resources = {}
        temp_path = os.path.join(work_dir, str(folder))
        _LOG.debug(f'Processing folder: {temp_path}')
        for rule_id in os.listdir(temp_path):
            rule_item, rule_resources = self._collect_statistics_in_rule(
                rule_id=rule_id,
                folder=folder,
                temp_path=temp_path,
                failed_policies=failed_policies,
                skipped_policies=skipped_policies,
                tenant=tenant
            )
            items.append(rule_item)
            resources.update(rule_resources)

        api_calls = self._count_api_calls(temp_path=temp_path)
        return items, resources, api_calls

    def _collect_statistics_in_rule(self, rule_id, folder, temp_path,
                                    failed_policies, skipped_policies,
                                    tenant: Tenant):
        _LOG.debug(f'Adding rule {rule_id} to statistics')
        _LOG.debug(f'Rule folder content: '
                   f'{os.listdir(os.path.join(temp_path, rule_id))}')
        resources_scanned = 0
        metadata_path = os.path.join(temp_path, rule_id,
                                     'metadata.json')
        with open(metadata_path, 'r') as file:
            metadata_json = json.loads(file.read())

        execution = metadata_json.get('execution')
        if not execution:
            raise ExecutorException(
                reason='Invalid metadata.json format: missing execution '
                       'block',
                step_name=STEP_COLLECT_STATISTICS)
        duration = round(execution.get('duration'), 3)
        end_time = utc_iso(datetime.fromtimestamp(execution.get('end_time')))
        start_time = utc_iso(datetime.fromtimestamp(execution.get('start')))
        failed_resources = None
        resource = metadata_json.get('policy', {}).get('resource')
        resource_path = os.path.join(temp_path, rule_id,
                                     'resources.json')
        for metric in metadata_json.get('metrics'):
            if metric.get('MetricName') == 'AllScannedResourcesCount':
                resources_scanned = int(metric.get('Value'))
            if metric.get('MetricName') == 'ResourceCount' and \
                    metric.get('Value') and os.path.exists(resource_path):
                with open(resource_path, 'r') as resources_file:
                    failed_resources = json.loads(
                        resources_file.read())
            else:
                _LOG.debug(
                    f'Resource path {resource_path} does not exist, '
                    f'failed rules will be empty')
        item = {
            'id': rule_id,
            'region': folder,
            'started_at': start_time,
            'finished_at': end_time,
            'status': 'SUCCEEDED',
            'resources_scanned': resources_scanned,
            'elapsed_time': str(duration),
            'failed_resources': failed_resources,
            'tenant_display_name': tenant.name,
            'customer_display_name': tenant.customer_name
        }
        if skipped_policies.get(folder) and rule_id in skipped_policies.get(
                folder).keys():
            item = {
                'id': rule_id,
                'region': folder,
                'status': 'SKIPPED',
                'reason': skipped_policies.get(folder).get(rule_id).get(
                    'reason'),
                'tenant_display_name': tenant.name,
                'customer_display_name': tenant.customer_name
            }
            skipped_policies.get(folder).pop(rule_id)
        if failed_policies.get(folder) and rule_id in failed_policies.get(
                folder).keys():
            item.update({
                'status': 'FAILED',
                'reason': failed_policies.get(folder).get(rule_id)[0],
                'traceback': failed_policies.get(folder).get(rule_id)[1]
            })
        return item, {resource: resources_scanned}

    @staticmethod
    def _count_api_calls(temp_path):
        _LOG.debug('Counting API calls')
        api_calls = {}
        for folder in os.listdir(temp_path):
            metadata_path = os.path.join(temp_path, folder, 'metadata.json')
            _LOG.debug(f'Opening file {metadata_path}')
            with open(metadata_path, 'r') as file:
                metadata_json = json.loads(file.read())

            api_stats = metadata_json.get('api-stats') or {}
            if not api_stats:
                _LOG.warning(f'Invalid metadata.json format: missing \''
                             f'api-stats\' block in {metadata_path}')
            for key, value in api_stats.items():
                if key in api_calls:
                    api_calls[key] = api_calls[key] + int(value)
                else:
                    api_calls[key] = value
        return api_calls
