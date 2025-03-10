from http import HTTPStatus
from uuid import uuid4

import msgspec
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

from helpers.constants import RabbitCommand
from helpers.lambda_response import LambdaResponse, ResponseFactory
from helpers.log_helper import get_logger
from services import cache
from services.environment_service import EnvironmentService

_LOG = get_logger(__name__)


class RabbitMQService:
    __slots__ = (
        'modular_client',
        'customer_rabbit_cache',
        'environment_service',
        '_encoder',
    )

    def __init__(
        self, modular_client: Modular, environment_service: EnvironmentService
    ):
        self.modular_client = modular_client
        self.environment_service = environment_service
        self.customer_rabbit_cache = cache.factory()
        self._encoder = msgspec.json.Encoder()

    def get_rabbitmq_application(self, customer: str) -> Application | None:
        aps = self.modular_client.application_service()
        _LOG.debug(f'Getting rabbit mq application by customer: {customer}')
        return next(
            aps.list(
                customer=customer,
                _type=ApplicationType.RABBITMQ.value,
                deleted=False,
                limit=1,
            ),
            None,
        )

    @staticmethod
    def no_rabbitmq_response() -> LambdaResponse:
        return ResponseFactory(HTTPStatus.SERVICE_UNAVAILABLE).message(
            'No valid RabbitMq configuration found'
        )

    def build_maestro_mq_transport(
        self, application: Application
    ) -> MaestroRabbitMQTransport | None:
        assert application.type == ApplicationType.RABBITMQ.value
        mcs = MaestroCredentialsService.build()
        creds: RabbitMQCredentials = mcs.get_by_application(application)
        if not creds:
            return
        maestro_config = MaestroRabbitConfig(
            request_queue=creds.request_queue,
            response_queue=creds.response_queue,
            rabbit_exchange=creds.rabbit_exchange,
            sdk_access_key=creds.sdk_access_key,
            sdk_secret_key=creds.sdk_secret_key,
            maestro_user=creds.maestro_user,
        )
        return self.modular_client.rabbit_transport_service(
            connection_url=creds.connection_url,
            config=maestro_config,
            timeout=30,
        )

    @staticmethod
    def send_to_m3(
        rabbitmq: MaestroRabbitMQTransport,
        command: RabbitCommand,
        models: list[dict] | dict,
    ) -> int | None:
        _LOG.info('Going to send data to rabbitMQ')
        try:
            code, status, response = rabbitmq.send_sync(
                command_name=command.value,
                parameters=models,
                is_flat_request=False,
                async_request=False,
                secure_parameters=None,
                compressed=True,
            )
        except ModularException:
            _LOG.exception('Could not send message to m3')
            return
        _LOG.info(f'Response from rabbit: {code}, {status}, {response}')
        return int(code)

    def build_m3_json_model(self, notification_type: str, data: dict):
        return {
            'viewType': 'm3',
            'model': {
                'uuid': str(uuid4()),
                'notificationType': notification_type,
                'notificationAsJson': self._encoder.encode(data).decode(),
                'notificationProcessorTypes': ['MAIL'],
            },
        }

    def get_customer_rabbitmq(
        self, customer: str
    ) -> MaestroRabbitMQTransport | None:
        if rabbitmq := self.customer_rabbit_cache.get(customer):
            _LOG.debug('Rabbitmq found in cache')
            return rabbitmq

        application = self.get_rabbitmq_application(customer)
        if not application:
            _LOG.warning(
                f'No application with type {ApplicationType.RABBITMQ.value} '
                f'found for customer {customer}'
            )
            return
        _LOG.debug(f'Application found: {application}')
        rabbitmq = self.build_maestro_mq_transport(application)
        if not rabbitmq:
            _LOG.warning(
                f'Could not build rabbit client from application '
                f'for customer {customer}'
            )
            return
        self.customer_rabbit_cache[customer] = rabbitmq
        return rabbitmq
