import datetime
import json
import os
import pathlib
import pytest
from unittest.mock import patch, MagicMock

from .test_metrics_updater import (TestMetricsUpdater,
                                   LAST_WEEK_DATE, CLOUDS, S3Client)

AWS_OVERVIEW_LEN = 486
AZURE_OVERVIEW_LEN = 258
GOOGLE_OVERVIEW_LEN = 277
AWS_RESOURCES_LEN = 546
AZURE_RESOURCES_LEN = 258
GOOGLE_RESOURCES_LEN = 278
AWS_FINOPS_LEN = 131
AZURE_FINOPS_LEN = 12
GOOGLE_FINOPS_LEN = 25
DEPARTMENT_TYPES = ['RESOURCES_BY_CLOUD', 'RESOURCES_BY_TENANT',
                    'COMPLIANCE_BY_TENANT', 'COMPLIANCE_BY_CLOUD',
                    'ATTACK_BY_TENANT', 'ATTACK_BY_CLOUD']
CUSTOMER_TYPES = ['OVERVIEW', 'COMPLIANCE', 'ATTACK_VECTOR']
MONTH_FIRST_DAY = '2023-11-01'


class TestMetricsAggregationFlow(TestMetricsUpdater):
    def setUp(self) -> None:
        super().setUp()
        self.tenant_metrics = {}
        self.maxDiff = None

    @pytest.mark.skip
    def test_happy_path(self):
        self.tenant_metrics_happy_path()
        self.tenant_groups_metrics_happy_path()
        self.customer_metrics_happy_path()
        self.difference_metrics_no_prev_happy_path()
        self.difference_metrics_with_prev_happy_path()

    @pytest.mark.skip
    def test_mark_archive(self):
        for t in self.tenants:
            t.is_active = False
        with (patch.object(self.TENANT_HANDLER, 'today_date',
                           datetime.datetime(2023, 10, 1)),
              patch.object(self.TENANT_HANDLER, 'month_first_day_iso',
                           datetime.datetime(2023, 10, 1).date().isoformat())):
            self.TENANT_HANDLER.process_data({})
        self.assertNotEqual([], S3Client().deleted_items)

    def assertFieldsMatch(self, actual, expected_fields):
        """Test that json fields are correct"""
        for field in expected_fields:
            path, expected_value = field
            value = self.get_nested(actual, path)

            self.assertIsNotNone(value, f"Field '{path}' not found in JSON")
            if type(value) == dict:
                self.assertDictEqual(value, expected_value,
                                     f"Field '{path}' has unexpected value")
            else:
                self.assertEqual(value, expected_value,
                                 f"Field '{path}' has unexpected value")

    def tenant_metrics_happy_path(self):
        with (patch.object(self.TENANT_HANDLER, 'yesterday',
                           datetime.datetime(2023, 10, 10)),
              patch.object(self.TENANT_HANDLER, 'next_month_date',
                           MONTH_FIRST_DAY)):
            tenant_result = self.TENANT_HANDLER.process_data(
                {'data_type': 'tenants'})

        self.assertDictEqual(
            {'data_type': 'tenant_groups', 'end_date': None,
             'continuously': None},
            tenant_result)
        for tenant in self.tenants:
            if tenant.cloud == 'AWS':
                self.compare_aws_tenant_metrics(
                    S3Client().put_object_mapping.get(
                        ('metrics',
                         f'{tenant.customer_name}/accounts/2023-10-15/{tenant.project}.json.gz'),
                        {}))
            elif tenant.cloud == 'AZURE':
                self.compare_azure_tenant_metrics(
                    S3Client().put_object_mapping.get(
                        ('metrics',
                         f'{tenant.customer_name}/accounts/2023-10-15/{tenant.project}.json.gz'),
                        {}))
            elif tenant.cloud == 'GOOGLE':
                self.compare_google_tenant_metrics(
                    S3Client().put_object_mapping.get(
                        ('metrics',
                         f'{tenant.customer_name}/accounts/2023-10-15/{tenant.project}.json.gz'),
                        {}))

    def tenant_groups_metrics_happy_path(self):
        self.TENANT_GROUP_HANDLER.s3_client.gz_get_json = MagicMock()
        self.TENANT_GROUP_HANDLER.s3_client.gz_get_json = self.get_tenant_metrics
        self.TENANT_GROUP_HANDLER.s3_client.list_dir = self.list_dir

        with patch.object(self.TENANT_GROUP_HANDLER, 'next_month_date',
                          MONTH_FIRST_DAY):
            tenant_group_result = self.TENANT_GROUP_HANDLER.process_data(
                {'data_type': 'tenant_groups'})

        self.assertDictEqual(
            {'data_type': 'customer', 'end_date': None, 'continuously': None},
            tenant_group_result)

        self.compare_tenant_groups_metrics(S3Client().put_object_mapping.get(
            ('metrics',
             f'{self.tenants[0].customer_name}/tenants/2023-10-15/{self.tenants[0].display_name}.json.gz'),
            {}))

    def difference_metrics_with_prev_happy_path(self):
        self.TENANT_GROUP_HANDLER.s3_client.list_dir = self.list_dir

        difference_result = self.DIFFERENCE_HANDLER.process_data(
            {'data_type': 'difference'})

        self.assertDictEqual({}, difference_result)

        self.compare_difference_metrics(
            S3Client().put_object_mapping.get(('metrics',
                                               f'{self.tenants[0].customer_name}/tenants/2023-10-15/{self.tenants[0].display_name}.json.gz'),
                                              {}), prev=True)

    def difference_metrics_no_prev_happy_path(self):
        self.TENANT_GROUP_HANDLER.s3_client.list_dir = self.list_dir_no_prev

        difference_result = self.DIFFERENCE_HANDLER.process_data(
            {'data_type': 'difference'})

        self.assertDictEqual({}, difference_result)

        self.compare_difference_metrics(S3Client().put_object_mapping.get((
            'metrics',
            f'{self.tenants[0].customer_name}/tenants/2023-10-15/{self.tenants[0].display_name}.json.gz'),
            {}),
            prev=False)

    def customer_metrics_happy_path(self):
        self.TOP_HANDLER.s3_client.list_dir = self.list_dir
        self.TENANT_GROUP_HANDLER.s3_client.gz_get_json = MagicMock()
        self.TENANT_GROUP_HANDLER.s3_client.gz_get_json = self.gz_get_json
        with patch.object(self.TOP_HANDLER, 'month_first_day', MONTH_FIRST_DAY):
            customer_result = self.TOP_HANDLER.process_data(
                {'data_type': 'customer'})
        self.assertDictEqual({'data_type': 'difference', 'end_date': None,
                              'continuously': None}, customer_result)
        self.compare_customer_metrics()

    def compare_aws_tenant_metrics(self, actual_body: dict):
        actual_body['resources'] = sorted(actual_body['resources'],
                                          key=lambda d: d['policy'])
        with open(os.path.join(
                pathlib.Path(__file__).parent.resolve(),
                'expected_metrics', 'aws_tenant_resources.json'), 'r') as f:
            expected_resources_body = f.read()
        expected_fields = [
            ('resources', json.loads(expected_resources_body)),
            ('rule.violated_resources_length', 486)
        ]
        self.assertEqual(len(actual_body['resources']), AWS_RESOURCES_LEN)
        self.assertEqual(21, len(actual_body['finops']))
        self.assertEqual(14, len(actual_body['attack_vector']))
        self.assertFieldsMatch(actual_body, expected_fields)

        self.tenant_metrics[actual_body['id']] = actual_body

    def compare_azure_tenant_metrics(self, actual_body: dict):
        actual_body['resources'] = sorted(actual_body['resources'],
                                          key=lambda d: d['policy'])
        with open(os.path.join(
                pathlib.Path(__file__).parent.resolve(),
                'expected_metrics', 'azure_tenant_resources.json'), 'r') as f:
            expected_resources_body = f.read()
        expected_fields = [
            ('resources', json.loads(expected_resources_body)),
            ('rule.violated_resources_length', 258)
        ]
        self.assertEqual(len(actual_body['resources']), AZURE_RESOURCES_LEN)
        self.assertEqual(len(actual_body['finops']), 1)
        self.assertEqual(len(actual_body['attack_vector']), 10)
        self.assertFieldsMatch(actual_body, expected_fields)

        self.tenant_metrics[actual_body['id']] = actual_body

    def compare_google_tenant_metrics(self, actual_body: dict):
        actual_body['resources'] = sorted(actual_body['resources'],
                                          key=lambda d: d['policy'])
        with open(os.path.join(
                pathlib.Path(__file__).parent.resolve(),
                'expected_metrics', 'google_tenant_resources.json'), 'r') as f:
            expected_resources_body = f.read()
        expected_fields = [
            ('rule.violated_resources_length', 277),
            ('resources', json.loads(expected_resources_body))
        ]
        self.assertEqual(len(actual_body['resources']), GOOGLE_RESOURCES_LEN)
        self.assertEqual(len(actual_body['finops']), 1)
        self.assertEqual(len(actual_body['attack_vector']), 14)
        self.assertFieldsMatch(actual_body, expected_fields)

        self.tenant_metrics[actual_body['id']] = actual_body

    def compare_tenant_groups_metrics(self, actual_body: dict):
        # overview
        overview = actual_body['overview']['aws'][0]
        expected_fields = [
            ('resources_violated', AWS_OVERVIEW_LEN),
            ('regions_data.multiregion.severity_data',
             {'Low': 25, 'Info': 6, 'High': 14, 'Medium': 8}),
            ('regions_data.multiregion.resource_types_data',
             {'AWS Identity and Access Management': 14, 'Amazon S3': 14,
              'Amazon CloudFront': 16, 'AWS Account': 2, 'Amazon Route 53': 4,
              'AWS Web Application Firewall': 3}),
            ('regions_data.eu-west-1.severity_data',
             {'Low': 158, 'High': 173, 'Info': 44, 'Medium': 59}),
            ('regions_data.eu-west-1.resource_types_data',
             {'Amazon Virtual Private Cloud': 17,
              'Amazon Relational Database Service': 68, 'Amazon Route 53': 2,
              'Amazon Elastic Load Balancing': 15, 'AWS CloudFormation': 3,
              'Amazon EC2': 50, 'Amazon Elastic Block Store': 10,
              'Amazon Simple Queue Service': 4,
              'Amazon Elastic Kubernetes Service': 8, 'AWS CloudTrail': 9,
              'AWS Key Management Service': 3, 'AWS Account': 2,
              'AWS CodeBuild': 7, 'Amazon EC2 Auto Scaling': 11,
              'Amazon OpenSearch Service': 14, 'AWS Lambda': 10,
              'Amazon Redshift': 15, 'Amazon SageMaker': 7,
              'Amazon Kinesis': 7, 'AWS Certificate Manager': 8,
              'Amazon Elastic Container Service': 13, 'Amazon API Gateway': 12,
              'Amazon DynamoDB': 3, 'Amazon Elastic File System': 5,
              'Amazon ElastiCache': 14, 'AWS Database Migration Service': 6,
              'Amazon DynamoDB Accelerator': 3, 'AWS Elastic Beanstalk': 7,
              'Amazon EMR': 8, 'AWS Secrets Manager': 3,
              'Amazon Simple Notification Service': 4,
              'Amazon Elastic Container Registry': 4, 'AWS Transit Gateway': 4,
              'Amazon AppFlow': 2, 'AWS Glue': 11, 'Amazon DocumentDB': 1,
              'Amazon WorkSpaces Family': 11, 'AWS Backup': 2,
              'Amazon EventBridge': 1, 'Amazon S3 Glacier': 2, 'AWS Config': 1,
              'Amazon FSx': 8, 'Amazon MQ': 6,
              'Amazon Managed Streaming for Apache Kafka': 4,
              'Amazon Managed Workflows for Apache Airflow': 8,
              'AWS Directory': 1, 'Amazon Data Lifecycle Manager': 1,
              'Amazon Lightsail': 1, 'Amazon CloudWatch': 3, 'Amazon QLDB': 3,
              'AWS AppSync': 4, 'AWS CodeDeploy': 3, 'AWS CodePipeline': 1,
              'AWS Identity and Access Management': 1,
              'AWS Identity and Access Management Access Analyzer': 1,
              'AWS Web Application Firewall': 1, 'AWS Step Functions': 1})
        ]
        self.assertFieldsMatch(overview, expected_fields)

        overview = actual_body['overview']['azure'][0]
        expected_fields = [
            ('resources_violated', AZURE_OVERVIEW_LEN),
            ('regions_data.multiregion.severity_data',
             {'Low': 158, 'Medium': 13, 'Info': 34, 'High': 53}),
            ('regions_data.multiregion.resource_types_data',
             {'Azure RBAC': 1, 'Microsoft Defender for Cloud': 18,
              'Azure Storage Accounts': 18, 'Azure SQL Database': 16,
              'Azure Database for PostgreSQL': 20,
              'Azure Database for MySQL': 14, 'Key Vault': 7,
              'Azure Subscription': 12, 'Network security groups': 18,
              'Azure Disk Storage': 3, 'Azure Kubernetes Service': 12,
              'App Service': 28, 'Virtual Machines': 17, 'Virtual Network': 5,
              'API Management': 3, 'Azure Cosmos DB': 4,
              'Cognitive Services': 3, 'Azure Container Registry': 6,
              'Azure Database for MariaDB': 3, 'App Configuration': 1,
              'Azure Cache for Redis': 4, 'Event Grid': 2,
              'Azure Machine Learning': 3, 'Azure SignalR Service': 1,
              'Azure Spring Apps': 1, 'Application Gateway': 4,
              'Azure Front Door': 2, 'Azure Service Fabric': 2,
              'Azure SQL Managed Instance': 3, 'Automation': 1,
              'Azure Data Lake Storage': 1, 'Azure Stream Analytics': 1,
              'Batch': 2, 'Data Lake Analytics': 1, 'Azure IoT Hub': 2,
              'Azure Logic Apps': 1, 'Azure Cognitive Search': 1,
              'Service Bus': 1, 'Azure Virtual Machine Scale Sets': 7,
              'Azure Data Explorer': 3, 'Azure Data Factory': 2,
              'Azure Databricks': 1, 'Azure Synapse Analytics': 2,
              'Azure Monitor': 1}),
            ('regions_data.westeurope.severity_data',
             {'Low': 158, 'Medium': 13, 'Info': 34, 'High': 53}),
            ('regions_data.westeurope.resource_types_data',
             {'Azure RBAC': 1, 'Microsoft Defender for Cloud': 18,
              'Azure Storage Accounts': 18, 'Azure SQL Database': 16,
              'Azure Database for PostgreSQL': 20,
              'Azure Database for MySQL': 14, 'Key Vault': 7,
              'Azure Subscription': 12, 'Network security groups': 18,
              'Azure Disk Storage': 3, 'Azure Kubernetes Service': 12,
              'App Service': 28, 'Virtual Machines': 17, 'Virtual Network': 5,
              'API Management': 3, 'Azure Cosmos DB': 4,
              'Cognitive Services': 3, 'Azure Container Registry': 6,
              'Azure Database for MariaDB': 3, 'App Configuration': 1,
              'Azure Cache for Redis': 4, 'Event Grid': 2,
              'Azure Machine Learning': 3, 'Azure SignalR Service': 1,
              'Azure Spring Apps': 1, 'Application Gateway': 4,
              'Azure Front Door': 2, 'Azure Service Fabric': 2,
              'Azure SQL Managed Instance': 3, 'Automation': 1,
              'Azure Data Lake Storage': 1, 'Azure Stream Analytics': 1,
              'Batch': 2, 'Data Lake Analytics': 1, 'Azure IoT Hub': 2,
              'Azure Logic Apps': 1, 'Azure Cognitive Search': 1,
              'Service Bus': 1, 'Azure Virtual Machine Scale Sets': 7,
              'Azure Data Explorer': 3, 'Azure Data Factory': 2,
              'Azure Databricks': 1, 'Azure Synapse Analytics': 2,
              'Azure Monitor': 1})
        ]
        self.assertFieldsMatch(overview, expected_fields)

        overview = actual_body['overview']['google'][0]
        expected_fields = [
            ('resources_violated', GOOGLE_OVERVIEW_LEN),
            ('regions_data.multiregion.severity_data',
             {'High': 99, 'Low': 121, 'Medium': 10, 'Info': 47}),
            ('regions_data.multiregion.resource_types_data',
             {'Cloud IAM': 12, 'Cloud KMS': 5, 'Cloud APIs': 4,
              'Cloud Logging': 10, 'Cloud Storage': 14,
              'Virtual Private Cloud': 39, 'Cloud DNS': 4,
              'Compute Engine': 38, 'Cloud SQL': 46,
              'Google Kubernetes Engine': 39, 'Secret Manager': 2,
              'Cloud Load Balancing': 12, 'BigQuery': 4, 'Cloud Functions': 10,
              'App Engine': 3, 'Cloud Bigtable': 3, 'Dataproc': 4,
              'Cloud Run': 6, 'Cloud Armor': 4, 'Pub/Sub': 3,
              'Cloud Spanner': 5, 'Cloud Memorystore': 2, 'Dataflow': 1,
              'Vertex AI Workbench': 1, 'Cloud Data Fusion': 3,
              'Access Transparency': 1, 'Access Approval': 1,
              'Cloud Asset Inventory': 1}),
        ]
        self.assertFieldsMatch(overview, expected_fields)

        # resource
        self.assertEqual(AWS_RESOURCES_LEN,
                         len(actual_body['resources']['aws'][0]['data']))
        self.assertEqual(AZURE_RESOURCES_LEN,
                         len(actual_body['resources']['azure'][0]['data']))
        self.assertEqual(GOOGLE_RESOURCES_LEN,
                         len(actual_body['resources']['google'][0]['data']))

        # finops
        for c in CLOUDS:
            c = c.lower()
            actual_body['finops'][c][0]['service_data'] = sorted(
                actual_body['finops'][c][0]['service_data'],
                key=lambda d: d['service_section'])
            with open(os.path.join(
                    pathlib.Path(__file__).parent.resolve(),
                    'expected_metrics', f'{c}_tenant_group_finops.json'),
                    'r') as f:
                expected_finops_body = f.read()
            self.assertEqual(actual_body['finops'][c][0]['service_data'],
                             json.loads(expected_finops_body))

    def compare_customer_metrics(self):
        # department
        tenant_items = \
        self.TOP_HANDLER.tenant_metrics_service.batch_save.call_args.args[0]
        for t in tenant_items:
            if t.type in DEPARTMENT_TYPES:
                DEPARTMENT_TYPES.remove(t.type)
        self.assertListEqual(DEPARTMENT_TYPES, [],
                             f'Not all department report types were created: '
                             f'{DEPARTMENT_TYPES} are missing')
        # TODO What is that and other files in mock_file?
        with open(os.path.join(
                pathlib.Path(__file__).parent,
                'expected_metrics', 'department_metrics.json'), 'r') as f:
            expected_department_items = f.read()
        for item in tenant_items:
            for c in CLOUDS:
                if item.attribute_values.get(c.lower()) == '{}':
                    continue
                if 'ATTACK_BY_CLOUD' == item.type:
                    self.assertCountEqual(
                        item.attribute_values.get(c.lower(), {}).get('data',
                                                                     []),
                        json.loads(expected_department_items).get(item.type,
                                                                  {}).get(
                            c.lower()),
                        f'Invalid structure for {item.type} department item')
                elif 'ATTACK_BY_TENANT' in item.type:
                    self.assertCountEqual(
                        item.attribute_values.get(c.lower()),
                        json.loads(expected_department_items).get(item.type,
                                                                  {}).get(
                            c.lower()),
                        f'Invalid structure for {item.type} department item')
                elif 'COMPLIANCE' in item.type:
                    self.assertCountEqual(
                        item.attribute_values.get(c.lower()).get(
                            'average_data'),
                        json.loads(expected_department_items).get(item.type,
                                                                  {}).get(
                            c.lower()).get('average_data'),
                        f'Invalid structure for {item.type} department item')
                elif 'RESOURCES' in item.type:
                    self.assertDictEqual(
                        item.attribute_values.get(c.lower(), {}).get(
                            'resource_types_data'),
                        json.loads(expected_department_items).get(item.type,
                                                                  {}).get(
                            c.lower(), {}).get('resource_types_data'),
                        f'Invalid structure for {item.type} department item')
                    self.assertDictEqual(
                        item.attribute_values.get(c.lower(), {}).get(
                            'severity_data'),
                        json.loads(expected_department_items).get(item.type,
                                                                  {}).get(
                            c.lower(), {}).get('severity_data'),
                        f'Invalid structure for {item.type} department item')
                else:
                    self.assertDictEqual(
                        item.attribute_values.get(c.lower()),
                        json.loads(expected_department_items).get(item.type,
                                                                  {}).get(
                            c.lower()),
                        f'Invalid structure for {item.type} department item')

        # c-level
        customer_items = \
        self.TOP_HANDLER.customer_metrics_service.batch_save.call_args.args[0]
        for c in customer_items:
            if c.type in CUSTOMER_TYPES:
                CUSTOMER_TYPES.remove(c.type)
        self.assertListEqual(CUSTOMER_TYPES, [],
                             f'Not all c-level report types were created: '
                             f'{CUSTOMER_TYPES} are missing')

        with open(os.path.join(
                pathlib.Path(__file__).parent,
                'expected_metrics', f'customer_metrics.json'), 'r') as f:
            expected_customer_items = f.read()
        for item in customer_items:
            for c in CLOUDS:
                if item.type == 'OVERVIEW':
                    self.assertEqual(
                        json.loads(item.to_json()).get(c.lower()),
                        json.loads(expected_customer_items).get(item.type,
                                                                {}).get(
                            c.lower()),
                        f'Invalid structure for {item.type} c-level item')
                elif item.type == 'COMPLIANCE':
                    self.assertCountEqual(
                        json.loads(item.to_json()).get(c.lower(), {}).get(
                            'average_data'),
                        json.loads(expected_customer_items).get(item.type,
                                                                {}).get(
                            c.lower(), {}).get('average_data'),
                        f'Invalid structure for {item.type} c-level item')
                elif item.type == 'ATTACK_VECTOR':
                    self.assertCountEqual(
                        json.loads(item.to_json()).get(c.lower(), {}).get(
                            'data'),
                        json.loads(expected_customer_items).get(item.type,
                                                                {}).get(
                            c.lower(), {}).get('data'),
                        f'Invalid structure for {item.type} c-level item')

    def compare_difference_metrics(self, actual_body: dict, prev: bool):
        self.assertEqual(len(actual_body['resources']['aws'][0]['data']),
                         AWS_RESOURCES_LEN)
        self.assertEqual(len(actual_body['resources']['azure'][0]['data']),
                         AZURE_RESOURCES_LEN)
        self.assertEqual(len(actual_body['resources']['google'][0]['data']),
                         GOOGLE_RESOURCES_LEN)

        for cloud in CLOUDS:
            c = cloud.lower()
            # compliance
            if not actual_body['compliance'][c][0].get('average_data'):
                for item in actual_body['compliance'][c][0]['regions_data'][0][
                    'standards_data']:
                    self.assert_diff_compliance_structure(item,
                                                          is_diff_none=prev)
            else:
                for item in actual_body['compliance'][c][0]['average_data']:
                    self.assert_diff_compliance_structure(item,
                                                          is_diff_none=prev)
            # resources
            for data in actual_body['resources'][c][0]['data']:
                for region, item in data['regions_data'].items():
                    self.assert_diff_resource_structure(
                        item['total_violated_resources'], is_diff_none=prev)
            # finops
            for service in actual_body['finops'][c][0]['service_data']:
                for rule in service['rules_data']:
                    for region, item in rule['regions_data'].items():
                        self.assert_diff_finops_structure(
                            item['total_violated_resources'],
                            is_diff_none=prev)
            # overview
            for region, item in actual_body['overview'][c][0][
                'regions_data'].items():
                self.assert_diff_overview_structure(item, is_diff_none=prev)

    def assert_diff_compliance_structure(self, container, is_diff_none=True):
        self.assertIn('diff', container,
                      'Invalid compliance diff structure: No \'diff\' field')
        self.assertIn('value', container,
                      'Invalid compliance diff structure: No \'value\' field')
        self.assertIn('name', container,
                      'Invalid compliance diff structure: No \'name\' field')
        self.assertIn(type(container['diff']), (int, float, type(None)),
                      'Invalid compliance diff value: Difference must be a '
                      'number or None')
        self.assertIn(type(container['value']), (int, float),
                      'Invalid compliance diff value: Value must be number')
        self.assertEqual(type(container['name']), str,
                         'Invalid compliance diff value: Name must be int or'
                         ' None')
        if not is_diff_none:
            self.assertIsNone(container['diff'],
                              'Invalid compliance diff value: Must be '
                              '\'None\', because there are no previous metrics')

    def assert_diff_resource_structure(self, container, is_diff_none=True):
        self.assertIn('diff', container,
                      'Invalid resource diff structure: No \'diff\' field')
        self.assertIn('value', container,
                      'Invalid resource diff structure: No \'value\' field')
        self.assertIn(type(container['diff']), (int, type(None)),
                      'Invalid resource diff value: Difference must be int '
                      'or None')
        self.assertEqual(type(container['value']), int,
                         'Invalid resource diff value: Value must be int')
        if not is_diff_none:
            self.assertIsNone(container['diff'],
                              'Invalid resource diff value: Must be \'None\', '
                              'because there are no previous metrics')

    def assert_diff_finops_structure(self, container, is_diff_none=True):
        self.assertIn('diff', container,
                      'Invalid finops diff structure: No \'diff\' field')
        self.assertIn('value', container,
                      'Invalid finops diff structure: No \'value\' field')
        self.assertIn(type(container['diff']), (int, type(None)),
                      'Invalid finops diff value: Difference must be int '
                      'or None')
        self.assertEqual(type(container['value']), int,
                         'Invalid finops diff value: Value must be int')
        if not is_diff_none:
            self.assertIsNone(container['diff'],
                              'Invalid finops diff value: Must be \'None\', '
                              'because there are no previous metrics')

    def assert_diff_overview_structure(self, container, is_diff_none=True):
        for t in ['severity_data', 'resource_types_data']:
            for severity, data in container[t].items():
                self.assertIn('diff', data, f'Invalid overview {t} diff '
                                            f'structure: No \'diff\' field')
                self.assertIn('value', data,
                              f'Invalid overview {t} diff structure: '
                              f'No \'value\' field')
                self.assertIn(type(data['diff']), (int, type(None)),
                              f'Invalid overview {t} diff value: Difference '
                              f'must be int or None')
                self.assertEqual(type(data['value']), int,
                                 f'Invalid overview {t} diff value: Value '
                                 f'must be int')
                if not is_diff_none:
                    self.assertIsNone(
                        data['diff'],
                        f'Invalid overview {t} diff value: Must be \'None\', '
                        f'because there are no previous metrics')

    def get_tenant_metrics(self, bucket, key):
        name = key.replace('.json.gz', '').split('/')[-1]
        if 'GOOGLE' in name:
            for t in self.tenants:
                if t.cloud == 'GOOGLE':
                    return self.tenant_metrics.get(t.account_number, {})

        return self.tenant_metrics.get(name, {})

    def list_dir(self, bucket_name, key=None):
        if key is None:
            yield 'EPAM Systems/'
        elif 'tenants' in key:
            yield f'EPAM Systems/tenants/2023-10-15/{self.tenants[0].display_name}.json.gz'
        else:
            for tenant in self.tenants:
                yield f'EPAM Systems/accounts/2023-10-15/{tenant.project}.json.gz'

    def list_dir_no_prev(self, bucket_name, key=None):
        if key is None:
            yield 'EPAM Systems/'
        elif 'tenants' in key and LAST_WEEK_DATE in key:
            yield ''
        elif 'tenants' in key:
            yield f'EPAM Systems/tenants/2023-10-15/{self.tenants[0].display_name}.json.gz'

    def gz_get_json(self, bucket, key):
        if LAST_WEEK_DATE in key:
            with open(os.path.join(
                    pathlib.Path(__file__).parent.resolve(),
                    'mock_files', 'old_metrics.json'), 'r') as file:
                parsed_json = json.load(file)
            return parsed_json
        else:
            parsed_json = S3Client().put_object_mapping.get((bucket, key), {})
            return parsed_json

    @staticmethod
    def get_nested(json_obj, path):
        keys = path.split('.')
        value = json_obj

        for key in keys:
            if key in value:
                value = value[key]
            else:
                return None

        return value
