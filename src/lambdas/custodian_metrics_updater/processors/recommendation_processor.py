import io
import json
import uuid
from copy import deepcopy
from datetime import datetime, timezone

from modular_sdk.commons.constants import RABBITMQ_TYPE, ParentType
from modular_sdk.modular import Modular

from helpers import get_logger
from helpers.constants import ARTICLE_ATTR, IMPACT_ATTR, RESOURCE_TYPE_ATTR, \
    Cloud
from helpers.recommendations import RULE_RECOMMENDATION_MAPPING, \
    K8S_RECOMMENDATION_MODEL
from helpers.time_helper import utc_datetime, make_timestamp_java_compatible
from services import SERVICE_PROVIDER
from services.clients.s3 import S3Client, ModularAssumeRoleS3Service
from services.environment_service import EnvironmentService
from services.mappings_collector import LazyLoadedMappingsCollector
from services.platform_service import PlatformService, Platform
from services.rabbitmq_service import RabbitMQService
from services.report_service import ReportService
from services.reports_bucket import ReportsBucketKeysBuilder, \
    PlatformReportsBucketKeysBuilder, TenantReportsBucketKeysBuilder

_LOG = get_logger(__name__)

COMMAND_NAME = 'SAVE_CADF_EVENT'
CADF_EVENT = {
    "id": "",
    "eventType": "activity",
    "eventTime": '',
    "action": "create",
    "outcome": "success",
    "initiator": {
        "name": "custodian"
    },
    "target": {},
    "observer": {},
    "attachments": []
}

K8S_TYPE_ACTION_MAPPING = {
    'ConfigMap': 'CONFIG',
    'ClusterRole': 'ROLE',
    'Role': 'ROLE',
    'Deployment': 'DEPLOYMENT',
    'Secret': 'SECRET',
    'ServiceAccount': 'SERVICE_ACCOUNT',
    'Namespace': 'NAMESPACE'
}


class Recommendation:
    def __init__(self, environment_service: EnvironmentService,
                 s3_client: S3Client,
                 modular_client: Modular,
                 rabbitmq_service: RabbitMQService,
                 assume_role_s3: ModularAssumeRoleS3Service,
                 mappings_collector: LazyLoadedMappingsCollector,
                 report_service: ReportService,
                 platform_service: PlatformService):
        self.environment_service = environment_service
        self.s3_client = s3_client
        self.assume_role_s3 = assume_role_s3
        self.modular_client = modular_client
        self.rabbitmq_service = rabbitmq_service
        self.mappings_collector = mappings_collector
        self.report_service = report_service
        self.platform_service = platform_service

        self.today = utc_datetime(utc=False).date().isoformat()
        self.bucket = self.environment_service.default_reports_bucket_name()

        self.customer_rabbit_mapping = {}
        self.recommendation_to_article_impact = {}
        self.cluster_platform_mapping = {}
        self.tenant_obj_mapping = {}
        self.now = datetime.now(timezone.utc)
        self.timestamp = make_timestamp_java_compatible(self.now.timestamp())

    def process_data(self, _):
        self.get_cluster_parents()
        # get cluster recommendation
        for name, platform in self.cluster_platform_mapping.items():
            key_builder = PlatformReportsBucketKeysBuilder(platform)
            file = key_builder.latest_key()
            k8s_recommendations = self.get_k8s_recommendations(platform, file)
            if not k8s_recommendations:
                continue

            # path to store /customer/cloud/tenant/timestamp/region.jsonl
            tenant = self.modular_client.tenant_service().get(platform.tenant_name)
            if not tenant:
                continue
            self.tenant_obj_mapping[tenant.project] = tenant
            tenant_key_builder = TenantReportsBucketKeysBuilder(tenant)
            _LOG.debug(f'Get file {tenant_key_builder.latest_key()} content')
            collection = self.report_service.tenant_latest_collection(
                tenant)
            collection.fetch_all()
            collection.fetch_meta()
            recommendations = self._build_recommendations(
                RULE_RECOMMENDATION_MAPPING, collection)

            if not recommendations:
                for region, recommend in k8s_recommendations.items():
                    _LOG.debug(f'No recommendations based on findings '
                               f'{tenant_key_builder.latest_key()}')
                    content = self._json_to_jsonl(recommend)
                    self.save_recommendation(region=region, tenant=tenant,
                                             content=content)
                self.send_request(tenant)
            else:
                for region, recommend in recommendations.items():
                    if k8s_region_recommend := k8s_recommendations.get(region):
                        recommend.extend(k8s_region_recommend)
                    content = self._json_to_jsonl(recommend)
                    self.save_recommendation(region=region, tenant=tenant,
                                             content=content)
                self.send_request(tenant)

        # get tenant recommendations
        prefixes = self.s3_client.common_prefixes(
            bucket=self.bucket,
            delimiter=ReportsBucketKeysBuilder.latest,
            prefix=ReportsBucketKeysBuilder.prefix
        )
        for prefix in prefixes:
            if Cloud.KUBERNETES in prefix:
                _LOG.debug('Skipping folder with k8s findings - '
                           'already processed')
                continue
            _LOG.debug(f'Processing key: {prefix}')
            objects = [
                o for o in self.s3_client.list_objects(bucket=self.bucket,
                                                       prefix=prefix)
                if o.key.endswith('json.gz') or o.key.endswith('json')
            ]

            for obj in objects:
                project_id = obj.key.split('/')[3]
                tenant = self.tenant_obj_mapping.get(project_id)
                if tenant:
                    _LOG.debug(
                        f'Tenant {tenant.name} have already been processed')
                    continue
                tenant = next(self.modular_client.tenant_service().i_get_by_acc(
                    project_id, active=True
                ), None)
                if not tenant:
                    _LOG.warning(
                        f'Cannot find tenant with project id {project_id}')
                    continue
                self.tenant_obj_mapping[project_id] = tenant
                _LOG.debug(f'Get file {obj.key} content')
                collection = self.report_service.tenant_latest_collection(
                    tenant)
                collection.fetch_all()
                collection.fetch_meta()
                recommendations = self._build_recommendations(
                    RULE_RECOMMENDATION_MAPPING, collection)

                # path to store /customer/cloud/tenant/timestamp/region.jsonl
                for region, recommend in recommendations.items():
                    content = self._json_to_jsonl(recommend)
                    self.save_recommendation(region, tenant, content)
                if not recommendations:
                    _LOG.debug(
                        f'No recommendations based on findings {obj.key}')
                    continue

                self.send_request(tenant)
        return {}

    def send_request(self, tenant):
        customer = tenant.customer_name
        if not self.customer_rabbit_mapping.get(customer):
            application = self.rabbitmq_service.get_rabbitmq_application(
                customer)
            if not application:
                _LOG.warning(
                    f'No application with type {RABBITMQ_TYPE} found')
                return
            self.customer_rabbit_mapping[customer] = \
                self.rabbitmq_service.build_maestro_mq_transport(
                    application)

        CADF_EVENT['id'] = str(uuid.uuid4().hex)
        CADF_EVENT['eventTime'] = self.now.astimezone().isoformat()
        CADF_EVENT['attachments'] = [
            {
                "contentType": "map",
                "content": {
                    "tenant": tenant.name,
                    "timestamp": self.timestamp
                },
                "name": "collectCustodianRecommendations"
            }
        ]
        code, status, response = self.customer_rabbit_mapping[customer]. \
            send_sync(
            command_name=COMMAND_NAME,
            parameters={'event': CADF_EVENT,
                        'qualifier': 'custodian_data'},
            is_flat_request=False, async_request=False,
            secure_parameters=None, compressed=True)
        _LOG.debug(f'Response code: {code}, response message: {response}')
        return code

    @staticmethod
    def _json_to_jsonl(recommendations: list[dict]) -> str:
        buffer = io.StringIO()
        for item in recommendations:
            buffer.write(json.dumps(item, separators=(',', ':')))
            buffer.write('\n')
        content = buffer.getvalue()
        buffer.close()
        return content

    def get_cluster_parents(self):
        """
        Get all parents and applications related to k8s clusters
        :return:
        """
        for cust in self.modular_client.customer_service().i_get_customer():
            parents = self.modular_client.parent_service().i_get_parent_by_customer(
                customer_id=cust.name,
                parent_type=ParentType.PLATFORM_K8S.value,
                is_deleted=False
            )
            for parent in parents:
                platform = Platform(parent)
                self.platform_service.fetch_application(platform)
                self.cluster_platform_mapping[platform.name] = platform

    def get_k8s_recommendations(self, platform: Platform, file):

        _LOG.debug(f'Get file {file} content')
        collection = self.report_service.platform_latest_collection(platform)
        collection.fetch_all()
        collection.fetch_meta()

        recommendations = self._build_k8s_recommendations(
            collection, platform.id, platform.region)
        return recommendations

    def _build_recommendations(self, rule_mapping: dict, collection) -> dict:
        recommendations = {}
        for _, shard in collection:
            for part in shard:
                if not (item := rule_mapping.get(part.policy)):
                    continue

                if not self.recommendation_to_article_impact.get(part.policy):
                    data = self.mappings_collector.human_data.get(part.policy, {})
                    article = data.get('article') or ''
                    impact = data.get('impact') or ''
                    resource_type = self.mappings_collector.service.get(
                        part.policy, '')
                    self.recommendation_to_article_impact[part.policy] = {
                        ARTICLE_ATTR: article,
                        IMPACT_ATTR: impact,
                        RESOURCE_TYPE_ATTR: resource_type
                    }
                item['recommendation'][ARTICLE_ATTR] = \
                    self.recommendation_to_article_impact[part.policy][ARTICLE_ATTR]
                item['recommendation'][IMPACT_ATTR] = \
                    self.recommendation_to_article_impact[part.policy][IMPACT_ATTR]
                item['recommendation']['description'] = collection.meta.get(
                    part.policy, {}).get('description', 'Description')

                _id = item['resource_id']
                for res in part.resources:
                    item_copy = item.copy()
                    item_copy['resource_id'] = _id.format(**res)
                    recommendations.setdefault(part.location, []).append(
                        item_copy)
        return recommendations

    def _build_k8s_recommendations(self, collection, application_uuid,
                                   region) -> dict:
        recommendations = {}
        for _, shard in collection:
            for part in shard:
                if not self.recommendation_to_article_impact.get(part.policy):
                    data = self.mappings_collector.human_data.get(part.policy, {})
                    article = data.get('article') or ''
                    impact = data.get('impact') or ''
                    resource_type = self.mappings_collector.service.get(
                        part.policy, '')
                    self.recommendation_to_article_impact[part.policy] = {
                        ARTICLE_ATTR: article,
                        IMPACT_ATTR: impact,
                        RESOURCE_TYPE_ATTR: resource_type
                    }

                item = deepcopy(K8S_RECOMMENDATION_MODEL)
                item['recommendation'][ARTICLE_ATTR] = \
                    self.recommendation_to_article_impact[part.policy][ARTICLE_ATTR]
                item['recommendation'][IMPACT_ATTR] = \
                    self.recommendation_to_article_impact[part.policy][IMPACT_ATTR]
                item['recommendation']['description'] = collection.meta.get(
                    part.policy, {}).get('description', 'Description')
                item['recommendation']['resource_type'] = \
                    self.recommendation_to_article_impact[part.policy][RESOURCE_TYPE_ATTR]
                item['resource_id'] = application_uuid
                item['general_actions'] = [K8S_TYPE_ACTION_MAPPING.get(
                    self.recommendation_to_article_impact[part.policy][RESOURCE_TYPE_ATTR], 'POD')]

                _id = item['recommendation']['resource_id']
                for res in part.resources:
                    item_copy = deepcopy(item)
                    item_copy['recommendation']['resource_id'] = _id.format(**res)
                    recommendations.setdefault(region if region else part.location, []).append(item_copy)
        return recommendations

    def save_recommendation(self, region, tenant, content):
        file_path = f'{tenant.customer_name}/{tenant.cloud}/{tenant.name}/' \
                    f'{self.timestamp}/{region}.jsonl'
        _LOG.debug(f'Saving file {file_path}, region {region}')
        recommendation_bucket = self.environment_service. \
            get_recommendation_bucket()
        self.assume_role_s3.put_object(
                bucket=recommendation_bucket,
                key=file_path,
                body=content.encode())


# RECOMMENDATION_METRICS = Recommendation(
#     environment_service=SERVICE_PROVIDER.environment_service,
#     s3_client=SERVICE_PROVIDER.s3,
#     modular_client=SERVICE_PROVIDER.modular_client,
#     rabbitmq_service=SERVICE_PROVIDER.rabbitmq_service,
#     assume_role_s3=SERVICE_PROVIDER.assume_role_s3,
#     mappings_collector=SERVICE_PROVIDER.mappings_collector,
#     report_service=SERVICE_PROVIDER.report_service,
#     platform_service=SERVICE_PROVIDER.platform_service
# )
