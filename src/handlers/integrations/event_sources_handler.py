"""
Event sources handler for SQS configuration.
"""

from __future__ import annotations

from typing_extensions import Self
import uuid
from http import HTTPStatus
from typing import TYPE_CHECKING, Any

from modular_sdk.models.application import Application
from modular_sdk.services.application_service import ApplicationService

from handlers import AbstractHandler, Mapping
from helpers.constants import (
    CUSTODIAN_EVENT_SOURCE_TYPE,
    Endpoint,
    HTTPMethod,
)
from helpers.lambda_response import ResponseFactory, build_response
from helpers.log_helper import get_logger
from services import SP
from services.abs_lambda import LambdaOutput, ProcessedEvent
from validators.swagger_request_models import (
    EventSourceDeleteModel,
    EventSourceGetModel,
    EventSourcePostModel,
    EventSourcePutModel,
)
from validators.utils import validate_kwargs

if TYPE_CHECKING:
    from modular_sdk.services.ssm_service import SSMClientCachingWrapper

_LOG = get_logger(__name__)

META_QUEUE_URL = "queue_url"
META_REGION = "region"
META_ENABLED = "enabled"
META_ROLE_ARN = "role_arn"


def _get_dto(application: Application) -> dict:
    meta = application.meta.as_dict() if application.meta else {}
    return {
        "id": application.application_id,
        "customer_id": application.customer_id,
        META_QUEUE_URL: meta.get(META_QUEUE_URL),
        META_REGION: meta.get(META_REGION),
        META_ENABLED: meta.get(META_ENABLED, True),
        META_ROLE_ARN: meta.get(META_ROLE_ARN),
    }


class EventSourcesHandler(AbstractHandler):
    def __init__(
        self,
        application_service: ApplicationService,
        ssm: SSMClientCachingWrapper,
    ):
        self._application_service = application_service
        self._ssm = ssm

    @classmethod
    def build(cls) -> Self:
        return cls(
            application_service=SP.modular_client.application_service(),
            ssm=SP.modular_client.assume_role_ssm_service(),
        )

    @property
    def mapping(self) -> Mapping:
        return {
            Endpoint.INTEGRATIONS_EVENT_SOURCES: {
                HTTPMethod.POST: self.post,
                HTTPMethod.GET: self.list,
            },
            Endpoint.INTEGRATIONS_EVENT_SOURCES_ID: {
                HTTPMethod.GET: self.get,
                HTTPMethod.PUT: self.put,
                HTTPMethod.DELETE: self.delete,
            },
        }

    @validate_kwargs
    def post(
        self,
        event: EventSourcePostModel,
        _pe: ProcessedEvent,
    ) -> LambdaOutput:
        customer = event.customer
        if not customer:
            raise ResponseFactory(HTTPStatus.BAD_REQUEST).message(
                "customer_id is required"
            ).exc()
        meta = {
            META_QUEUE_URL: event.queue_url,
            META_REGION: event.region,
            META_ENABLED: event.enabled,
        }
        if event.role_arn is not None:
            meta[META_ROLE_ARN] = event.role_arn
        secret = None
        if event.aws_access_key_id and event.aws_secret_access_key:
            creds: dict[str, str] = {
                "aws_access_key_id": event.aws_access_key_id,
                "aws_secret_access_key": event.aws_secret_access_key,
            }
            if event.aws_session_token:
                creds["aws_session_token"] = event.aws_session_token
            name = self._ssm.safe_name(
                name=f"sqs-{uuid.uuid4().hex}",
                prefix="m3.custodian.event_source",
            )
            name = self._ssm.put_parameter(name=name, value=creds)
            secret = name
        application = self._application_service.build(
            customer_id=customer,
            type=CUSTODIAN_EVENT_SOURCE_TYPE,
            created_by=_pe["cognito_user_id"],  # type: ignore
            is_deleted=False,
            description="Event source configuration for Syndicate Rule Engine",
            meta=meta,
            secret=secret,
        )
        _LOG.info("Saving event source application item")
        self._application_service.save(application)
        return build_response(content=_get_dto(application))

    @validate_kwargs
    def list(
        self,
        event: EventSourceGetModel,
    ) -> LambdaOutput:
        customer = event.customer
        if not customer:
            raise ResponseFactory(HTTPStatus.BAD_REQUEST).message(
                "customer_id is required"
            ).exc()
        applications = list(
            self._application_service.list(
                customer=customer,
                _type=CUSTODIAN_EVENT_SOURCE_TYPE,
                deleted=False,
                limit=100,
            )
        )
        items = [_get_dto(a) for a in applications]
        return build_response(content=items)

    @validate_kwargs
    def get(
        self,
        id: str,
        event: EventSourceGetModel,
        _pe: ProcessedEvent,
    ) -> LambdaOutput:
        application = self._application_service.get_application_by_id(id)
        if not application:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                "Event source not found"
            ).exc()
        if application.type != CUSTODIAN_EVENT_SOURCE_TYPE or application.is_deleted:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                "Event source not found"
            ).exc()
        if event.customer and application.customer_id != event.customer:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                "Event source not found"
            ).exc()
        return build_response(content=_get_dto(application))

    @validate_kwargs
    def put(
        self,
        id: str,
        event: EventSourcePutModel,
        _pe: ProcessedEvent,
    ) -> LambdaOutput:
        application = self._application_service.get_application_by_id(id)
        if not application:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                "Event source not found"
            ).exc()
        if application.type != CUSTODIAN_EVENT_SOURCE_TYPE or application.is_deleted:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                "Event source not found"
            ).exc()
        if event.customer and application.customer_id != event.customer:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                "Event source not found"
            ).exc()
        meta = (application.meta.as_dict() if application.meta else {}).copy()
        meta.update(self._get_meta(event))
        application.meta = meta
        self._application_service.save(application)
        return build_response(content={"data": _get_dto(application)})

    @validate_kwargs
    def delete(
        self,
        id: str,
        event: EventSourceDeleteModel,
        _pe: ProcessedEvent,
    ) -> LambdaOutput:
        application = self._application_service.get_application_by_id(id)
        if not application:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                "Event source not found"
            ).exc()
        if application.type != CUSTODIAN_EVENT_SOURCE_TYPE or application.is_deleted:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                "Event source not found"
            ).exc()
        if event.customer and application.customer_id != event.customer:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                "Event source not found"
            ).exc()
        self._application_service.mark_deleted(application)
        if application.secret:
            _LOG.info(f"Removing application secret: {application.secret}")
            if not self._ssm.delete_parameter(application.secret):
                _LOG.warning(f"Could not remove secret: {application.secret}")
        return build_response(code=HTTPStatus.NO_CONTENT)

    def _get_meta(
        self,
        event: EventSourcePostModel | EventSourcePutModel,
    ) -> dict[str, Any]:
        meta = {}
        if hasattr(event, "queue_url") and event.queue_url is not None:
            meta[META_QUEUE_URL] = event.queue_url
        if hasattr(event, "region") and event.region is not None:
            meta[META_REGION] = event.region
        if hasattr(event, "enabled") and event.enabled is not None:
            meta[META_ENABLED] = event.enabled
        if hasattr(event, "role_arn") and event.role_arn is not None:
            meta[META_ROLE_ARN] = event.role_arn
        return meta
