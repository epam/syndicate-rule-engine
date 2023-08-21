from modular_sdk.commons.constants import RABBITMQ_TYPE
from modular_sdk.services.impl.maestro_credentials_service import \
    RabbitMQApplicationMeta, RabbitMQApplicationSecret

from handlers.abstracts.abstract_handler import AbstractHandler
from helpers import RESPONSE_RESOURCE_NOT_FOUND_CODE, \
    build_response, RESPONSE_CONFLICT, RESPONSE_NO_CONTENT, \
    RESPONSE_BAD_REQUEST_CODE
from helpers.constants import POST_METHOD, GET_METHOD, DELETE_METHOD, \
    CUSTOMER_ATTR, MAESTRO_USER_ATTR, RABBIT_EXCHANGE_ATTR, \
    REQUEST_QUEUE_ATTR, RESPONSE_QUEUE_ATTR, SDK_ACCESS_KEY_ATTR, \
    CONNECTION_URL_ATTR, SDK_SECRET_KEY_ATTR
from helpers.log_helper import get_logger
from models.modular.application import Application
from services import SERVICE_PROVIDER
from services.modular_service import ModularService
from services.ssm_service import SSMService

_LOG = get_logger(__name__)


class RabbitMQHandler(AbstractHandler):
    def __init__(self, modular_service: ModularService, ssm_service: SSMService):
        self._modular_service = modular_service
        self._ssm_service = ssm_service

    @classmethod
    def build(cls) -> 'RabbitMQHandler':
        return cls(
            modular_service=SERVICE_PROVIDER.modular_service(),
            ssm_service=SERVICE_PROVIDER.ssm_service()
        )

    def define_action_mapping(self) -> dict:
        return {
            '/customers/rabbitmq': {
                POST_METHOD: self.post,
                GET_METHOD: self.get,
                DELETE_METHOD: self.delete
            },
        }

    @staticmethod
    def get_dto(application: Application) -> dict:
        """
        Very specific case,
        :param application:
        :return:
        """
        return {
            CUSTOMER_ATTR: application.customer_id,
            **application.meta.as_dict()
        }

    def post(self, event: dict) -> dict:
        customer = event[CUSTOMER_ATTR]
        item = next(self._modular_service.get_applications(
            customer=customer,
            _type=RABBITMQ_TYPE,
            limit=1,
            deleted=False
        ), None)
        if item:
            return build_response(
                code=RESPONSE_CONFLICT,
                content='RabbitMQ configuration already exists'
            )
        meta = RabbitMQApplicationMeta(
            maestro_user=event[MAESTRO_USER_ATTR],
            rabbit_exchange=event.get(RABBIT_EXCHANGE_ATTR),
            request_queue=event[REQUEST_QUEUE_ATTR],
            response_queue=event[RESPONSE_QUEUE_ATTR],
            sdk_access_key=event[SDK_ACCESS_KEY_ATTR]
        )
        name = self._ssm_service.save_data(
            name=f'{customer}-rabbitmq-configuration',
            value=RabbitMQApplicationSecret(
                connection_url=event[CONNECTION_URL_ATTR],
                sdk_secret_key=event[SDK_SECRET_KEY_ATTR]
            ).dict(),
            prefix='caas'
        )
        application = self._modular_service.create_application(
            customer=customer,
            _type=RABBITMQ_TYPE,
            description='RabbitMQ configuration for Custodian',
            meta=meta.dict(),
            secret=name
        )
        _LOG.info('Saving application item')
        self._modular_service.save(application)
        return build_response(content=self.get_dto(application))

    def get(self, event) -> dict:
        customer = event[CUSTOMER_ATTR]
        application = next(self._modular_service.get_applications(
            customer=customer,
            _type=RABBITMQ_TYPE,
            limit=1,
            deleted=False
        ), None)
        if not application:
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content=f'RabbitMQ configuration not found'
            )
        return build_response(content=self.get_dto(application))

    def delete(self, event) -> dict:
        customer = event[CUSTOMER_ATTR]
        application = next(self._modular_service.get_applications(
            customer=customer,
            _type=RABBITMQ_TYPE,
            limit=1,
            deleted=False
        ), None)
        if not application:
            return build_response(code=RESPONSE_NO_CONTENT)
        erased = self._modular_service.delete(application)
        if not erased:
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE,
                content='Could not remove the application. '
                        'Probably it\'s used by some parents.'
            )
        # erased
        if application.secret:
            _LOG.info(f'Removing application secret: {application.secret}')
            if not self._ssm_service.delete_secret(application.secret):
                _LOG.warning(f'Could not remove secret: {application.secret}')
        # Modular sdk does not remove the app, just sets is_deleted
        self._modular_service.save(application)
        return build_response(code=RESPONSE_NO_CONTENT)
