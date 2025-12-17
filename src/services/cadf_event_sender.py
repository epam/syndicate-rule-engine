import uuid
from datetime import datetime, timezone
from typing import Any, TypedDict, overload

from modular_sdk.commons.constants import RABBITMQ_TYPE
from modular_sdk.models.tenant import Tenant
from modular_sdk.services.impl.maestro_rabbit_transport_service import MaestroRabbitMQTransport

from helpers.log_helper import get_logger
from services.rabbitmq_service import RabbitMQService

_LOG = get_logger(__name__)

DEFAULT_COMMAND_NAME = "SAVE_CADF_EVENT"

SRE_INITIATOR_NAME = "syndicate_rule_engine"
SRE_QUALIFIER_NAME = f"{SRE_INITIATOR_NAME}_data"


class CadfInitiator(TypedDict):
    name: str


class CadfAttachment(TypedDict):
    contentType: str
    content: dict
    name: str


CadfAttachments = list[CadfAttachment]


class CadfEvent(TypedDict):
    id: str
    eventType: str
    eventTime: str
    action: str
    outcome: str
    initiator: CadfInitiator
    target: dict[str, Any]
    observer: dict[str, Any]
    attachments: CadfAttachments


class CadfEventSender:
    """
    Service responsible for building and sending CADF events via RabbitMQ.
    """

    __slots__ = (
        "rabbitmq_service",
        "_customer_rabbit_mapping",
    )

    def __init__(self, rabbitmq_service: RabbitMQService) -> None:
        self.rabbitmq_service = rabbitmq_service
        self._customer_rabbit_mapping: dict = {}

    def build_cadf_event(
        self,
        *,
        attachments: CadfAttachments,
        event_type: str = "activity",
        action: str = "create",
        outcome: str = "success",
        target: dict[str, Any] | None = None,
        observer: dict[str, Any] | None = None,
        initiator: CadfInitiator | None = None,
        event_id: str | None = None,
        event_time: datetime | None = None,
    ) -> CadfEvent:
        """
        Build a CADF event with the given parameters.
        """
        event_time = event_time or datetime.now(timezone.utc)
        event_time_iso = event_time.astimezone().isoformat()
        initiator = initiator or CadfInitiator(name=SRE_INITIATOR_NAME)

        return CadfEvent(
            id=event_id or str(uuid.uuid4().hex),
            eventType=event_type,
            eventTime=event_time_iso,
            action=action,
            outcome=outcome,
            initiator=initiator,
            target=target or {},
            observer=observer or {},
            attachments=attachments,
        )

    @overload
    def send_event(
        self,
        *,
        tenant: Tenant,
        cadf_event: CadfEvent,
        command_name: str = DEFAULT_COMMAND_NAME,
    ) -> int | None: ...

    @overload
    def send_event(
        self,
        *,
        tenant: Tenant,
        attachments: CadfAttachments,
        command_name: str = DEFAULT_COMMAND_NAME,
        event_time: datetime | None = None,
        event_id: str | None = None,
    ) -> int | None: ...

    def send_event(
        self,
        *,
        tenant: Tenant,
        attachments: CadfAttachments | None = None,
        cadf_event: CadfEvent | None = None,
        command_name: str = DEFAULT_COMMAND_NAME,
        event_time: datetime | None = None,
        event_id: str | None = None,
    ) -> int | None:
        """
        Send a recommendation collection CADF event for the given tenant.
        """
        if attachments and cadf_event:
            raise ValueError("Only one of attachments or event can be provided")
        if not attachments and not cadf_event:
            raise ValueError("One of attachments or event must be provided")

        customer = tenant.customer_name
        transport = self._get_or_build_transport(customer)
        if not transport:
            _LOG.warning(f"No transport found for customer {customer}")
            return None

        if attachments:
            cadf_event = self.build_cadf_event(
                attachments=attachments,
                event_time=event_time,
                event_id=event_id,
            )
        elif cadf_event:
            cadf_event = cadf_event
        else:
            raise ValueError("One of attachments or cadf_event must be provided")

        code, status, response = transport.send_sync(
            command_name=command_name,
            parameters={
                "event": cadf_event,
                "qualifier": SRE_QUALIFIER_NAME,
            },
            is_flat_request=False,
            async_request=False,
            secure_parameters=None,
            compressed=True,
        )
        _LOG.debug(f"Response code: {code}, response message: {response}")
        return code

    def clear_cache(self) -> None:
        """Clear the customer rabbit mapping cache."""
        self._customer_rabbit_mapping.clear()

    def _get_or_build_transport(self, customer: str) -> MaestroRabbitMQTransport | None:
        """
        Get cached RabbitMQ transport for customer or build a new one.
        Returns None if no RabbitMQ application found.
        """
        if customer in self._customer_rabbit_mapping:
            return self._customer_rabbit_mapping[customer]

        application = self.rabbitmq_service.get_rabbitmq_application(customer)
        if not application:
            _LOG.warning(f"No application with type {RABBITMQ_TYPE} found")
            return None

        transport = self.rabbitmq_service.build_maestro_mq_transport(application)
        self._customer_rabbit_mapping[customer] = transport
        return transport
