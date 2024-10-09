import json
from datetime import datetime

from dateutil.relativedelta import relativedelta
from modular_sdk.modular import Modular

from helpers import get_logger
from helpers.constants import JobState
from helpers.time_helper import utc_datetime
from services import SERVICE_PROVIDER
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService
from services.job_statistics_service import JobStatisticsService
from services.report_service import ReportService
from services.report_statistics_service import ReportStatisticsService
from services.reports_bucket import StatisticsBucketKeysBuilder
from services.scheduler_service import SchedulerService

_LOG = get_logger(__name__)


class DiagnosticMetrics:
    def __init__(self, modular_client: Modular,
                 environment_service: EnvironmentService,
                 s3_service: S3Client,
                 report_statistics_service: ReportStatisticsService,
                 job_statistics_service: JobStatisticsService,
                 scheduler_service: SchedulerService,
                 report_service: ReportService):
        self.modular_client = modular_client
        self.environment_service = environment_service
        self.s3_service = s3_service
        self.report_statistics_service = report_statistics_service
        self.job_statistics_service = job_statistics_service
        self.scheduler_service = scheduler_service
        self.report_service = report_service

        self.stat_bucket_name = \
            self.environment_service.get_statistics_bucket_name()

        self.today_date = utc_datetime()
        self.end_date_obj = datetime.combine(
            self.today_date.replace(day=1),
            datetime.min.time()
        )
        self.end_date = self.end_date_obj.isoformat()
        self.start_date_obj = datetime.combine(
            self.today_date + relativedelta(months=-1, day=1),
            datetime.min.time()
        )
        self.start_date = self.start_date_obj.isoformat()

        self.TO_UPDATE_MARKER = False
        self.tenant_obj_mapping = {}

    @classmethod
    def build(cls) -> 'DiagnosticMetrics':
        return cls(
            modular_client=SERVICE_PROVIDER.modular_client,
            environment_service=SERVICE_PROVIDER.environment_service,
            s3_service=SERVICE_PROVIDER.s3,
            report_statistics_service=SERVICE_PROVIDER.report_statistics_service,
            job_statistics_service=SERVICE_PROVIDER.job_statistics_service,
            scheduler_service=SERVICE_PROVIDER.scheduler_service,
            report_service=SERVICE_PROVIDER.report_service
        )

    def process_data(self, event):
        for customer in self.modular_client.customer_service().i_get_customer():
            name = customer.name
            key_path = StatisticsBucketKeysBuilder.report_statistics(
                self.start_date_obj, name)

            report_dto = {
                'report_type': 'DIAGNOSTIC', 'from': self.start_date,
                'to': self.end_date, 'customer': name,
                'data': {
                    'scans_data': {
                        'scheduled_scans_data':
                            list(self._i_get_scheduled_jobs(name)),
                        'executed_scans_data':
                            self._get_scans_statistics(name)
                    },
                    'reports_data': {
                        'triggered_reports_data':
                            self._get_report_statistics(name)
                    },
                    'rules_data': {
                        'execution_data': self.get_rule_statistics(name)
                    }
                }
            }

            _LOG.debug(f'Saving diagnostic file by key {key_path}')
            self.s3_service.gz_put_json(
                bucket=self.stat_bucket_name, key=key_path, obj=report_dto)
        return {}

    def _get_scans_statistics(self, customer):
        scans_data = {
            'failed': 0,
            'succeeded': 0,
            'cloud_data': []
        }
        cloud_data = {}
        tenants_data = {}
        region_scans_data = {}

        items = self.job_statistics_service.get_by_customer_and_date(
            customer=customer, from_date=self.start_date,
            to_date=self.end_date
        )
        for item in items:
            scans_data['failed'] += item.failed
            scans_data['succeeded'] += item.succeeded
            for tenant_id, data in json.loads(item.to_json()).get(
                    'scanned_regions', {}).items():
                if not (tenant := self.tenant_obj_mapping.get(tenant_id)):
                    tenant = next(self.modular_client.tenant_service().i_get_by_acc(
                        tenant_id
                    ), None)
                    self.tenant_obj_mapping[tenant_id] = tenant
                region_scans_data.setdefault(tenant.name, {})
                for region, number in data.items():
                    region_scans_data[tenant.name].setdefault(region, 0)
                    region_scans_data[tenant.name][region] += number

            for tenant_id, scans in item.tenants.attribute_values.items():
                if not (tenant := self.tenant_obj_mapping.get(tenant_id)):
                    tenant = next(self.modular_client.tenant_service().i_get_by_acc(
                        tenant_id
                    ), None)
                self.tenant_obj_mapping[tenant_id] = tenant
                tenants_data.setdefault(tenant.name, {
                    'failed_scans': 0,
                    'succeeded_scans': 0,
                    'failed_scans_reasons': {},
                    'region_scans_data': {}
                })
                tenants_data[tenant.name]['failed_scans'] += scans.get(
                    'failed_scans', 0)
                tenants_data[tenant.name]['succeeded_scans'] += scans.get(
                    'succeeded_scans', 0)
                tenants_data[tenant.name]['cloud'] = tenant.cloud
                for reason, number in item.__dict__.get('reason', {}).items():
                    tenants_data[tenant.name][
                        'failed_scans_reasons'].setdefault(reason, 0)
                    tenants_data[tenant.name]['failed_scans_reasons'][
                        reason] += number

        for tenant, data in region_scans_data.items():
            tenants_data[tenant]['region_scans_data'] = data

        for tenant, data in tenants_data.items():
            cloud = data.pop('cloud')
            cloud_data.setdefault(cloud, {'tenants_data': []})
            cloud_data[cloud]['tenants_data'].append({
                'tenant_name': tenant, **data
            })
        scans_data['cloud_data'] = [{'cloud': cloud, **data} for cloud, data in
                                    cloud_data.items()]
        return scans_data

    def _i_get_scheduled_jobs(self, customer):
        items = self.scheduler_service.list(customer=customer)
        for i in items:
            dto = self.scheduler_service.dto(i)
            if dto.get('context', {}).get('is_enabled'):
                dto.pop('customer_name')
                yield dto

    def get_rule_statistics(self, customer):
        items = []
        files = self.s3_service.list_dir(
            bucket_name=self.stat_bucket_name,
            key=StatisticsBucketKeysBuilder.tenant_statistics(
                self.start_date_obj, customer=customer))
        for file in files:
            items.extend(self.s3_service.gz_get_json(
                bucket=self.stat_bucket_name,
                key=file
            ))
        return self.report_service.sum_average_statistics(items)

    def _get_report_statistics(self, customer: str):
        result = {}
        items = self.report_statistics_service.iter_by_customer(
            customer, triggered_at=self.start_date, end_date=self.end_date
        )
        for item in items:
            level = item.attribute_values.pop('level')
            item.attribute_values.pop('customer_name')
            item.attribute_values.pop('id')
            item.attribute_values.pop('event')
            item.attribute_values.pop('attempt')
            result.setdefault(level, {
                'triggers': 0,
                'failed': 0,
                'succeeded': 0,
                'failed_reports_data': []
            })
            result[level]['triggers'] += 1
            if item.status == JobState.FAILED:
                result[level]['failed'] += 1
                result[level]['failed_reports_data'].append(
                    item.attribute_values)
            elif item.status == JobState.SUCCEEDED:
                result[level]['succeeded'] += 1

        return [{'level': level, **data} for level, data in result.items()]


DIAGNOSTIC_METRICS = DiagnosticMetrics.build()
