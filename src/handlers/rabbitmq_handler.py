from http import HTTPStatus
from typing import TYPE_CHECKING

from modular_sdk.commons.constants import ApplicationType
from modular_sdk.models.application import Application
from modular_sdk.services.application_service import ApplicationService
from modular_sdk.services.impl.maestro_credentials_service import (
    RabbitMQApplicationMeta,
    RabbitMQApplicationSecret,
)

from handlers import AbstractHandler, Mapping
from helpers.constants import CUSTOMER_ATTR, CustodianEndpoint, HTTPMethod
from helpers.lambda_response import ResponseFactory, build_response
from helpers.log_helper import get_logger
from services import SP
from services.abs_lambda import ProcessedEvent
from validators.swagger_request_models import (
    RabbitMQDeleteModel,
    RabbitMQGetModel,
    RabbitMQPostModel,
)
from validators.utils import validate_kwargs

if TYPE_CHECKING:
    from modular_sdk.services.ssm_service import SSMClientCachingWrapper

_LOG = get_logger(__name__)


class RabbitMQHandler(AbstractHandler):
    def __init__(
        self,
        application_service: ApplicationService,
        ssm: 'SSMClientCachingWrapper',
    ):
        self._application_service = application_service
        self._ssm = ssm

    @classmethod
    def build(cls) -> 'RabbitMQHandler':
        return cls(
            application_service=SP.modular_client.application_service(),
            ssm=SP.modular_client.assume_role_ssm_service(),
        )

    @property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.CUSTOMERS_RABBITMQ: {
                HTTPMethod.POST: self.post,
                HTTPMethod.GET: self.get,
                HTTPMethod.DELETE: self.delete,
            }
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
            **application.meta.as_dict(),
        }

    @validate_kwargs
    def post(self, event: RabbitMQPostModel, _pe: ProcessedEvent):
        customer = event.customer_id
        item = next(
            self._application_service.list(
                customer=customer,
                _type=ApplicationType.RABBITMQ.value,
                limit=1,
                deleted=False,
            ),
            None,
        )
        if item:
            raise (
                ResponseFactory(HTTPStatus.CONFLICT)
                .message('RabbitMQ configuration already exists')
                .exc()
            )
        meta = RabbitMQApplicationMeta(
            maestro_user=event.maestro_user,
            rabbit_exchange=event.rabbit_exchange,
            request_queue=event.request_queue,
            response_queue=event.response_queue,
            sdk_access_key=event.sdk_access_key,
        )
        name = self._ssm.safe_name(
            name=customer, prefix='m3.custodian.rabbitmq'
        )
        name = self._ssm.put_parameter(
            name=name,
            value=RabbitMQApplicationSecret(
                connection_url=str(event.connection_url),
                sdk_secret_key=event.sdk_secret_key,
            ).dict(),
        )
        application = self._application_service.build(
            customer_id=customer,
            type=ApplicationType.RABBITMQ,
            created_by=_pe['cognito_user_id'],
            is_deleted=False,
            description='RabbitMQ configuration for Custodian',
            meta=meta.dict(),
            secret=name,
        )
        _LOG.info('Saving application item')
        self._application_service.save(application)
        return build_response(content=self.get_dto(application))

    @validate_kwargs
    def get(self, event: RabbitMQGetModel):
        customer = event.customer
        application = next(
            self._application_service.list(
                customer=customer,
                _type=ApplicationType.RABBITMQ,
                limit=1,
                deleted=False,
            ),
            None,
        )
        if not application:
            raise (
                ResponseFactory(HTTPStatus.NOT_FOUND)
                .message('RabbitMQ configuration not found')
                .exc()
            )
        return build_response(content=self.get_dto(application))

    @validate_kwargs
    def delete(self, event: RabbitMQDeleteModel):
        customer = event.customer
        application = next(
            self._application_service.list(
                customer=customer,
                _type=ApplicationType.RABBITMQ,
                limit=1,
                deleted=False,
            ),
            None,
        )
        if not application:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).default().exc()
        self._application_service.mark_deleted(application)
        if application.secret:
            _LOG.info(f'Removing application secret: {application.secret}')
            if not self._ssm.delete_parameter(application.secret):
                _LOG.warning(f'Could not remove secret: {application.secret}')
        # Modular sdk does not remove the app, just sets is_deleted
        return build_response(code=HTTPStatus.NO_CONTENT)
