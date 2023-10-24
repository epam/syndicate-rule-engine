import io
import json
import uuid
from datetime import datetime, timezone
from http import HTTPStatus

from modular_sdk.commons.constants import RABBITMQ_TYPE

from helpers import get_logger, CustodianException
from helpers.constants import ARTICLE_ATTR, IMPACT_ATTR
from helpers.recommendations import RULE_RECOMMENDATION_MAPPING
from helpers.time_helper import utc_datetime, make_timestamp_java_compatible
from services import SERVICE_PROVIDER
from services.clients.s3 import S3Client, ModularAssumeRoleS3Service
from services.environment_service import EnvironmentService
from services.findings_service import FindingsService
from services.modular_service import ModularService
from services.rabbitmq_service import RabbitMQService
from services.rule_meta_service import RuleMetaService, \
    LazyLoadedMappingsCollector

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


class Recommendation:
    def __init__(self, findings_service: FindingsService,
                 environment_service: EnvironmentService, s3_client: S3Client,
                 modular_service: ModularService,
                 rabbitmq_service: RabbitMQService,
                 assume_role_s3: ModularAssumeRoleS3Service,
                 rule_meta_service: RuleMetaService,
                 mappings_collector: LazyLoadedMappingsCollector):
        self.findings_service = findings_service
        self.environment_service = environment_service
        self.s3_client = s3_client
        self.assume_role_s3 = assume_role_s3
        self.modular_service = modular_service
        self.rabbitmq_service = rabbitmq_service
        self.rule_meta_service = rule_meta_service
        self.mappings_collector = mappings_collector

        self.today = utc_datetime(utc=False).date().isoformat()

        self.customer_rabbit_mapping = {}
        self.recommendation_to_article_impact = {}

    def process_data(self, _):
        bucket = self.environment_service.get_statistics_bucket_name()
        prefix = f'findings/{self.today}/'
        objects = list(self.s3_client.list_objects(
            bucket_name=bucket, prefix=prefix
        ))

        # todo take this duplicate to separate function
        _LOG.debug(f'Retrieved objects (first attempt): {objects}')
        if not objects:
            prefix = 'findings/'
            objects = [o for o in self.s3_client.list_objects(
                bucket_name=bucket, prefix=prefix)
                       if o.get('Key').endswith('json.gz') or o.get('Key').
                       endswith('json')]
            _LOG.debug(f'Retrieved objects (second attempt): {objects}')

        # timestamp = 1690363338133
        now = datetime.now(timezone.utc)
        timestamp = make_timestamp_java_compatible(now.timestamp())
        for obj in objects:
            file = obj.get('Key')
            _LOG.debug(f'Get file {file} content')
            content = self.s3_client.get_file_content(
                bucket_name=bucket, full_file_name=file)
            if not content:
                _LOG.debug(f'Skipping file {file}, no findings content')
                continue

            project_id = file.split(prefix)[-1].split('.json')[0]
            tenant = next(
                self.modular_service.i_get_tenants_by_acc(project_id, True),
                None
            )
            if not tenant:
                _LOG.warning(
                    f'Cannot find tenant with project id {project_id}')
                continue

            recommendations = {}
            for rule, resources in json.loads(content).items():
                item = RULE_RECOMMENDATION_MAPPING.get(rule)
                if not item:
                    continue

                if not self.recommendation_to_article_impact.get(rule):
                    data = self.mappings_collector.human_data.get(rule, {})
                    article = data.get('article') or ''
                    impact = data.get('impact') or ''
                    self.recommendation_to_article_impact[rule] = {
                        ARTICLE_ATTR: article,
                        IMPACT_ATTR: impact
                    }
                item['recommendation'][ARTICLE_ATTR] = \
                    self.recommendation_to_article_impact[rule][ARTICLE_ATTR]
                item['recommendation'][IMPACT_ATTR] = \
                    self.recommendation_to_article_impact[rule][IMPACT_ATTR]
                item['recommendation']['description'] = resources.get(
                    'description', 'Description')

                _id = item['resource_id']
                for region, resource in resources.get(
                        'resources', {}).items():
                    for i in resource:
                        item = item.copy()
                        item['resource_id'] = _id.format(**i)
                        recommendations.setdefault(region, []).append(item)

            # path to store /customer/cloud/tenant/timestamp/region.jsonl
            file = file.split(prefix)[-1]
            for region, recommend in recommendations.items():
                contents = self._json_to_jsonl(recommend)
                _LOG.debug(f'Saving file {file}, region {region}')
                recommendation_bucket = self.environment_service. \
                    get_recommendation_bucket()
                result = self.assume_role_s3.put_object(
                    bucket_name=recommendation_bucket,
                    object_name=f'{tenant.customer_name}/'
                                f'{tenant.cloud}/{tenant.name}/{timestamp}/'
                                f'{region}.jsonl',
                    body=contents.encode(),
                    is_zipped=False)
                if not result:
                    raise CustodianException(
                        code=HTTPStatus.INTERNAL_SERVER_ERROR.value,
                        content='There is no recommendations bucket'
                    )
            if not recommendations:
                _LOG.debug(f'No recommendations based on findings {file}')
                continue

            customer = tenant.customer_name
            if not self.customer_rabbit_mapping.get(customer):
                application = self.rabbitmq_service.get_rabbitmq_application(
                    customer)
                if not application:
                    _LOG.warning(
                        f'No application with type {RABBITMQ_TYPE} found')
                    continue
                self.customer_rabbit_mapping[customer] = \
                    self.rabbitmq_service.build_maestro_mq_transport(
                        application)

            CADF_EVENT['id'] = str(uuid.uuid4().hex)
            CADF_EVENT['eventTime'] = now.astimezone().isoformat()
            CADF_EVENT['attachments'] = [
                {
                    "contentType": "map",
                    "content": {
                        "tenant": tenant.name,
                        "timestamp": timestamp
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
        return {}

    @staticmethod
    def _json_to_jsonl(recommendations: dict):
        with io.StringIO() as body:
            for item in recommendations:
                body.write(json.dumps(item) + '\n')
            body.seek(0)
            contents = body.getvalue()
            body.close()

        return contents


RECOMMENDATION_METRICS = Recommendation(
    findings_service=SERVICE_PROVIDER.findings_service(),
    environment_service=SERVICE_PROVIDER.environment_service(),
    s3_client=SERVICE_PROVIDER.s3(),
    modular_service=SERVICE_PROVIDER.modular_service(),
    rabbitmq_service=SERVICE_PROVIDER.rabbitmq_service(),
    assume_role_s3=SERVICE_PROVIDER.assume_role_s3(),
    rule_meta_service=SERVICE_PROVIDER.rule_meta_service(),
    mappings_collector=SERVICE_PROVIDER.mappings_collector()
)
