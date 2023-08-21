from typing import Optional

from modular_sdk.commons.constants import RABBITMQ_TYPE
from modular_sdk.services.impl.maestro_credentials_service import \
    MaestroCredentialsService, RabbitMQCredentials
from modular_sdk.services.impl.maestro_rabbit_transport_service import \
    MaestroRabbitMQTransport, MaestroRabbitConfig

from helpers import get_logger, build_response, \
    RESPONSE_SERVICE_UNAVAILABLE_CODE
from models.modular.application import Application
from services.modular_service import ModularService

_LOG = get_logger(__name__)


class RabbitMQService:

    def __init__(self, modular_service: ModularService):
        self.modular_service = modular_service

    def get_rabbitmq_application(self, customer: str) -> Optional[Application]:
        # TODO cache
        return next(self.modular_service.get_applications(
            customer=customer,
            _type=RABBITMQ_TYPE,
            limit=1
        ), None)

    @staticmethod
    def no_rabbit_configuration() -> dict:
        return build_response(
            code=RESPONSE_SERVICE_UNAVAILABLE_CODE,
            content='No valid RabbitMq configuration found'
        )

    def build_maestro_mq_transport(self, application: Application
                                   ) -> Optional[MaestroRabbitMQTransport]:
        # TODO cache
        assert application.type == RABBITMQ_TYPE
        modular = self.modular_service.modular_client
        mcs = MaestroCredentialsService.build(
            ssm_service=modular.ssm_service()  # not assume role ssm service
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
        return modular.rabbit_transport_service(
            connection_url=creds.connection_url,
            config=maestro_config
        )
