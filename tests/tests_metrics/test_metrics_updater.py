import importlib
import json
import logging
import os
import pathlib
import uuid
from typing import Dict, List
from unittest import TestCase
from unittest.mock import MagicMock

from modular_sdk.models.tenant import Tenant

from helpers import SingletonMeta
from models.batch_results import BatchResults
from models.customer_metrics import CustomerMetrics
from models.job import Job
from models.job_statistics import JobStatistics
from models.tenant_metrics import TenantMetrics

CURRENT_WEEK_DATE = '2023-10-15'
LAST_WEEK_DATE = '2023-10-09'
CLOUDS = ['AWS', 'AZURE', 'GOOGLE']
regions = {
    'AWS': [{'native_name': 'eu-west-1'}, {'native_name': 'multiregion'}],
    'AZURE': [{'native_name': 'westeurope'}, {'native_name': 'multiregion'}],
    'GOOGLE': [{'native_name': 'multiregion'}]
}
manual_jobs = []
ed_jobs = []
tenants = []

# mock job items
for cloud in CLOUDS:
    manual_jobs.append(Job(**{'id': str(uuid.uuid4()),
                              'batch_job_id': str(uuid.uuid4()),
                              'tenant_name': f'{cloud}-TEST',
                              'customer_name': 'EPAM Systems',
                              'created_at': '2023-10-10T10:10:10',
                              'started_at': '2023-10-10T10:10:10',
                              'submitted_at': '2023-10-10T10:10:10',
                              'status': 'FAILED'
                              }))

    for _ in range(2):
        ed_jobs.append(BatchResults(**{
            'id': str(uuid.uuid4()),
            'status': 'SUCCEEDED',
            'tenant_name': f'{cloud}-TEST',
            'customer_name': 'EPAM Systems',
            'registration_start': '2023-10-11T11:11:00',
            'submitted_at': '2023-10-11T11:11:00'
        }))

    # mock tenant
    tenants.append(Tenant(**{
        'name': f'{cloud}-TEST',
        'display_name': 'test',
        'display_name_to_lower': 'test',
        'is_active': True,
        'customer_name': 'EPAM Systems',
        'cloud': cloud,
        'activation_date': '2022-01-01T10:00:00',
        'project': f'{cloud}-1234567890123',
        'regions': regions[cloud],
    }))


class TestMetricsUpdater(TestCase):
    HANDLER_IMPORT_PATH = 'lambdas.custodian_metrics_updater.handler'
    TENANT_IMPORT_PATH = \
        'lambdas.custodian_metrics_updater.processors.tenant_metrics_processor'
    TENANT_GROUP_IMPORT_PATH = 'lambdas.custodian_metrics_updater.processors.tenant_group_metrics_processor'
    TOP_IMPORT_PATH = \
        'lambdas.custodian_metrics_updater.processors.top_metrics_processor'
    DIFFERENCE_IMPORT_PATH = 'lambdas.custodian_metrics_updater.processors.metric_difference_processor'
    SERVICE_PROVIDER_PATH = 'services.service_provider'

    def setUp(self) -> None:
        super().setUp()
        logging.disable(logging.NOTSET)

        self.tenants = tenants
        self.mappings = {}

        self.service_provider = importlib.import_module(
            self.SERVICE_PROVIDER_PATH).ServiceProvider()
        self.service_provider.settings_service = MagicMock()
        self.service_provider.settings_service.get_report_date_marker = MagicMock(
            return_value={'current_week_date': CURRENT_WEEK_DATE,
                          'last_week_date': LAST_WEEK_DATE}
        )
        self.tenant_handler = importlib.import_module(
            self.TENANT_IMPORT_PATH)
        self.tenant_group_handler = importlib.import_module(
            self.TENANT_GROUP_IMPORT_PATH)
        self.top_handler = importlib.import_module(self.TOP_IMPORT_PATH)
        self.difference_handler = importlib.import_module(
            self.DIFFERENCE_IMPORT_PATH)
        self.handler = importlib.import_module(self.HANDLER_IMPORT_PATH)

        mocked_response = MagicMock(last_evaluated_key=None)
        mocked_response.last_evaluated_key = None
        tenants_item = MagicMock()
        tenants_item.as_dict = MagicMock(return_value={
            "AWS-1234567890123": {
                "failed_scans": 5,
                "succeeded_scans": 5
            }}
        )
        job_stats = MagicMock()
        job_stats.cloud = 'aws'
        job_stats.tenants.attribute_values = {
            "AWS-1234567890123": {
                "failed_scans": 5,
                "succeeded_scans": 5
            }
        }
        job_stats.attribute_values = {
            "id": str(uuid.uuid4()),
            "cloud": "aws",
            "customer_name": "EPAM Systems",
            "failed": 5,
            "from_date": "2023-10-10",
            "last_scan_date": "2023-09-26T16:18:00.185909Z",
            "succeeded": 5,
            "tenants": tenants_item,
            "to_date": "2023-10-17"
        }
        self.service_provider.modular_client.customer_service().i_get_customer = MagicMock(
            return_value=[self.default_customer()]
        )

        TenantMetrics.customer_date_index.query = MagicMock(return_value=[])
        CustomerMetrics.customer_date_index.query = MagicMock(return_value=[])

        JobStatistics.customer_name_from_date_index.query = mocked_response()
        JobStatistics.customer_name_from_date_index.query.return_value = mocked_response

        self.service_provider.report_service.s3_client = S3Client()
        self.service_provider.mappings_collector._s3_settings_service._s3 = S3Client()
        self.service_provider.modular_client.tenant_service().get = MagicMock()
        self.service_provider.modular_client.tenant_service().get.side_effect = self.get_tenant  # todo seems like side_effect is used for other purposes
        self.service_provider.modular_client.tenant_service().i_get_by_acc = MagicMock()
        self.service_provider.modular_client.tenant_service().i_get_by_acc.side_effect = self.get_tenant_by_acc
        self.service_provider.modular_client.tenant_service().i_get_by_accN = MagicMock()
        self.service_provider.modular_client.tenant_service().i_get_by_accN.side_effect = self.get_tenant_by_accN
        self.service_provider.batch_results_service. \
            get_between_period_by_customer = MagicMock(return_value=ed_jobs)
        self.service_provider.job_service.get_customer_jobs_between_period = \
            MagicMock(return_value=manual_jobs)
        self.service_provider.job_service.get_customer_jobs = MagicMock(
            return_value=manual_jobs
        )
        self.service_provider.job_statistics_service.save = MagicMock(
            return_value={}
        )
        self.service_provider.job_statistics_service. \
            get_by_customer_and_date = MagicMock(return_value=[job_stats])
        self.service_provider.lambda_client.invoke_function_async = MagicMock()
        self.service_provider.lambda_client.invoke_function_async.side_effect = self.invoke_lambda
        self.service_provider.tenant_metrics_service.batch_save = MagicMock(
            return_value={}
        )
        self.service_provider.customer_metrics_service.batch_save = \
            MagicMock(return_value={})
        self.service_provider.environment_service.get_metrics_bucket_name = \
            MagicMock(return_value='metrics')
        self.service_provider.batch_results_service.get_by_customer_name = \
            MagicMock(return_value=list(ed_jobs))
        self.service_provider.job_service.get_by_customer_name = MagicMock(
            return_value=list(manual_jobs))
        self.service_provider.batch_results_service.get_by_tenant_name = \
            MagicMock(return_value=list(ed_jobs))
        self.service_provider.job_service.get_by_tenant_name = MagicMock(
            return_value=list(manual_jobs))

        self.HANDLER = self.handler.MetricsUpdater(
            lambda_client=MagicMock()
        )
        self.TENANT_HANDLER = self.tenant_handler.TenantMetrics(
            ambiguous_job_service=self.service_provider.ambiguous_job_service,
            report_service=self.service_provider.report_service,
            s3_client=S3Client(),
            environment_service=self.service_provider.environment_service,
            settings_service=self.service_provider.settings_service,
            modular_client=self.service_provider.modular_client,
            coverage_service=self.service_provider.coverage_service,
            mappings_collector=self.service_provider.mappings_collector,
            metrics_service=self.service_provider.metrics_service,
            job_statistics_service=self.service_provider.job_statistics_service,
            license_service=MagicMock(),
            platform_service=MagicMock()
        )
        self.TENANT_GROUP_HANDLER = self.tenant_group_handler.TenantGroupMetrics(
            s3_client=S3Client(),
            environment_service=self.service_provider.environment_service,
            settings_service=self.service_provider.settings_service,
            modular_client=self.service_provider.modular_client,
            mappings_collector=self.service_provider.mappings_collector
        )
        self.TOP_HANDLER = self.top_handler.TopMetrics(
            s3_client=S3Client(),
            environment_service=self.service_provider.environment_service,
            modular_client=MagicMock(),
            job_statistics_service=self.service_provider.job_statistics_service,
            tenant_metrics_service=self.service_provider.tenant_metrics_service,
            customer_metrics_service=self.service_provider.customer_metrics_service
        )
        self.DIFFERENCE_HANDLER = self.difference_handler.TenantMetricsDifference(
            s3_client=S3Client(),
            environment_service=self.service_provider.environment_service,
            settings_service=self.service_provider.settings_service
        )

    @staticmethod
    def default_customer():
        customer = MagicMock()
        customer.name = 'EPAM Systems'
        return customer

    @staticmethod
    def get_tenant(tenant: str):
        for t in tenants:
            if t.name == tenant:
                return t
        return

    def get_tenant_by_acc(self, acc: str, active: bool = True,
                          attributes_to_get=None):
        for t in self.tenants:
            if t.project == acc:
                return [t]
        return

    def get_tenant_by_accN(self, acc: str):
        for t in self.tenants:
            if t.account_number == acc:
                yield t
        yield

    def invoke_lambda(self, event):
        if event.get('data_type') == 'tenant_groups':
            return self.TENANT_GROUP_HANDLER.lambda_handler()
        elif event.get('data_type') == 'customer':
            return self.TOP_HANDLER.lambda_handler()
        elif event.get('data_type') == 'difference':
            return self.TOP_HANDLER.lambda_handler()


class S3Client(metaclass=SingletonMeta):
    def __init__(self):
        self.put_object_mapping = {}
        self.deleted_items = []

    def gz_get_json(self, bucket: str, key: str) -> Dict | List | None:
        # TODO we should not change inner logic, only mock the data.
        if bucket == 'statistics':
            with open(os.path.join(pathlib.Path(__file__).parent.resolve(),
                                   'mock_files', bucket, 'statistics.json'),
                      'r') as j:
                data = json.loads(j.read())
        else:
            key = key.split('/')
            with open(os.path.join(pathlib.Path(__file__).parent.resolve(),
                                   'mock_files', bucket, *key), 'r') as j:
                data = json.loads(j.read())
        return data

    def gz_get_object(self, bucket: str, key: str) -> Dict | List | None:
        # TODO we should not change inner logic, only mock the data.
        if bucket == 'statistics':
            with open(os.path.join(pathlib.Path(__file__).parent.resolve(),
                                   'mock_files', bucket, 'statistics.json'),
                      'r') as j:
                data = json.loads(j.read())
        else:
            key = key.split('/')
            with open(os.path.join(pathlib.Path(__file__).parent.resolve(),
                                   'mock_files', bucket, *key), 'r') as j:
                data = json.loads(j.read())
        return data

    def put_object(self, bucket: str, key: str, body,
                   content_type: str = None, content_encoding: str = None):
        if '.gz' not in key:
            key += '.gz'
        self.put_object_mapping.update({(bucket, key): body})

    def gz_put_object(self, bucket: str, key: str, body,
                      gz_buffer=None, content_type: str = None,
                      content_encoding: str = None):
        if '.gz' not in key:
            key += '.gz'
        self.put_object_mapping.update({(bucket, key): body})

    def gz_put_json(self, bucket: str, key: str, obj):
        if '.gz' not in key:
            key += '.gz'
        self.put_object_mapping.update({(bucket, key): obj})

    def gz_delete_object(self, bucket: str, key: str):
        self.deleted_items.append(key)

    def list_dir(self, bucket_name: str, key=None, page_size=None,
                 limit=None, start_after=None):
        return []
