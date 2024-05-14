from datetime import datetime
from http import HTTPStatus
import json
import random
from typing import cast
from unittest.mock import MagicMock
from uuid import uuid4

from modular_sdk.commons import ModularException
from modular_sdk.commons.constants import ApplicationType
from modular_sdk.models.application import Application
from modular_sdk.modular import Modular
from modular_sdk.services.impl.maestro_credentials_service import (
    MaestroCredentialsService,
    RabbitMQCredentials,
)
from modular_sdk.services.impl.maestro_rabbit_transport_service import (
    MaestroRabbitConfig,
    MaestroRabbitMQTransport,
)

from helpers.lambda_response import ReportNotSendException, ResponseFactory
from helpers.log_helper import get_logger
from services import SP
from services import cache
from services.clients.s3 import S3Url
from services.environment_service import EnvironmentService

_LOG = get_logger(__name__)


class MockedRabbitMQTransport:
    """
    Fake version of MaestroRabbitMQTransport. Can be used for testing.
    Writes each call to a specified location in s3
    """
    __slots__ = '_client', '_bucket', '_key', '_rate'

    def __init__(self, bucket: str, key: str, success_rate: float = 1.0):
        self._client = SP.s3
        self._bucket = bucket
        self._key = key
        self._rate = success_rate

    def __getattr__(self, item):
        _LOG.info(f'trying to access {item} of {self.__class__.__name__}. '
                  f'Returning mock')
        return MagicMock()

    def send_sync(self, **kwargs):
        is_success = random.random() < self._rate
        if is_success:
            fmt = '%Y-%m-%d-%H-%M-%S-success.json'
        else:
            fmt = '%Y-%m-%d-%H-%M-%S-fail.json'
        name = self._key.strip('/') + '/' + datetime.now().strftime(fmt)
        data = json.dumps(kwargs, default=str, sort_keys=True,
                          separators=(',', ':'))
        _LOG.info(f'Writing rabbitmq report to s3://{self._bucket}/{name}')
        self._client.put_object(
            bucket=self._bucket,
            key=name,
            body=data.encode(),
            content_type='application/json',
        )
        _LOG.info('Data was saved to s3')
        if is_success:
            _LOG.info('Mocking successful rabbit response')
            return 200, 'SUCCESS', 'Successfully sent'
        _LOG.info('Mocking failed rabbit response')
        return 500, 'FAIL', 'Internal server error'


class RabbitMQService:
    __slots__ = ('modular_client', 'customer_rabbit_cache',
                 'environment_service')

    def __init__(self, modular_client: Modular,
                 environment_service: EnvironmentService):
        self.modular_client = modular_client
        self.environment_service = environment_service
        self.customer_rabbit_cache = cache.factory()

    def get_rabbitmq_application(self, customer: str) -> Application | None:
        aps = self.modular_client.application_service()
        _LOG.debug(f'Getting rabbit mq application by customer: {customer}')
        return next(aps.list(
            customer=customer,
            _type=ApplicationType.RABBITMQ.value,
            deleted=False,
            limit=1
        ), None)

    @staticmethod
    def no_rabbit_configuration():
        raise ResponseFactory(HTTPStatus.SERVICE_UNAVAILABLE).message(
            'No valid RabbitMq configuration found'
        ).exc()

    def build_maestro_mq_transport(self, application: Application
                                   ) -> MaestroRabbitMQTransport | None:
        if tpl := self.environment_service.mock_rabbitmq_s3_url():
            url, rate = tpl
            parsed = S3Url(url)
            _LOG.warning('Mocked rabbitmq specified in envs. Returning'
                         ' mocked client')
            return cast(MaestroRabbitMQTransport, MockedRabbitMQTransport(
                bucket=parsed.bucket,
                key=parsed.key,
                success_rate=rate
            ))
        assert application.type == ApplicationType.RABBITMQ.value
        mcs = MaestroCredentialsService.build(
            ssm_service=self.modular_client.ssm_service()  # not assume role ssm service
        )
        creds: RabbitMQCredentials = mcs.get_by_application(application)
        if not creds:
            return
        maestro_config = MaestroRabbitConfig(
            request_queue=creds.request_queue,
            response_queue=creds.response_queue,
            rabbit_exchange=creds.rabbit_exchange,
            sdk_access_key=creds.sdk_access_key,
            sdk_secret_key=creds.sdk_secret_key,
            maestro_user=creds.maestro_user
        )
        return self.modular_client.rabbit_transport_service(
            connection_url=creds.connection_url,
            config=maestro_config
        )

    @staticmethod
    def send_notification_to_m3(command_name: str,
                                json_model: list | dict,
                                rabbitmq: MaestroRabbitMQTransport,
                                is_event_driven: bool = False):
        _LOG.debug('Pushing to rabbitmq')
        _LOG.debug(json.dumps(json_model))
        factory = ResponseFactory(HTTPStatus.INTERNAL_SERVER_ERROR)
        try:
            code, status, response = rabbitmq.send_sync(
                command_name=command_name,
                parameters=json_model,
                is_flat_request=False, async_request=False,
                secure_parameters=None, compressed=True)
            _LOG.debug(f'Response code: {code}, response message: {response}')
            if code == HTTPStatus.INTERNAL_SERVER_ERROR and \
                    not is_event_driven:
                raise factory.message(response).exc(ReportNotSendException)
            return code
        except ModularException as e:
            _LOG.error(f'Modular error: {e}')
            raise factory.message(str(e)).exc(ReportNotSendException)
        except Exception as e:  # can occur in case access data is invalid
            _LOG.exception('An error occurred sending a message to rabbit')
            raise factory.message(str(e)).exc(ReportNotSendException)

    @staticmethod
    def build_m3_json_model(notification_type, data):
        return {
            'viewType': 'm3',
            'model': {
                "uuid": str(uuid4()),
                "notificationType": notification_type,
                "notificationAsJson": json.dumps(data,
                                                 separators=(",", ":")),
                "notificationProcessorTypes": ["MAIL"]
            }
        }

    def get_customer_rabbitmq(self, customer: str
                              ) -> MaestroRabbitMQTransport | None:
        if rabbitmq := self.customer_rabbit_cache.get(customer):
            _LOG.debug('Rabbitmq found in cache')
            return rabbitmq

        application = self.get_rabbitmq_application(customer)
        if not application:
            _LOG.warning(
                f'No application with type {ApplicationType.RABBITMQ.value} '
                f'found for customer {customer}')
            return
        _LOG.debug(f'Application found: {application}')
        rabbitmq = self.build_maestro_mq_transport(application)
        if not rabbitmq:
            _LOG.warning(f'Could not build rabbit client from application '
                         f'for customer {customer}')
            return
        self.customer_rabbit_cache[customer] = rabbitmq
        return rabbitmq
