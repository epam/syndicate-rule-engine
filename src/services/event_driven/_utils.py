from typing import Any

from models.event import EventRecordAttribute

from helpers.constants import Cloud
from pydantic import BaseModel as PydanticBaseModel, model_validator


class EventRecord(PydanticBaseModel):
    """
    EventRecord — internal model for validating and converting event records to EventRecordAttribute.

    Description:
        - Used as an intermediate structure for ingesting, processing, and storing events received from external systems.

    Event processing flow:
        1. Incoming request (e.g., via API handler)
        2. Data is converted to EventRecord (adapter layer)
        3. Event is stored in the database as an Event object

    Attributes:
        cloud (Cloud): Cloud provider.
        region_name (str | None): Cloud region name (if applicable).
        source_name (str): Name of the event source (e.g., service or system name).
        event_name (str): Event name (type or action).
        account_id (str | None): AWS account ID (if applicable).
        tenant_name (str | None): Tenant name (if applicable).

    Example:
        {
            "cloud": "AWS",
            "region_name": "eu-west-1",
            "source_name": "aws.ec2",
            "event_name": "RunInstances",
            "account_id": "1234567890",  # or None
            "tenant_name": None,  # or "test-tenant"
        }
    """

    cloud: Cloud
    region_name: str | None
    source_name: str
    event_name: str

    # Tenant identification — at least one must be present.
    # EventBridge events carry account_id; Maestro events carry tenant_name.
    # Resolution to a concrete Tenant happens downstream.
    account_id: str | None
    tenant_name: str | None

    @model_validator(mode="before")
    def at_least_one_tenant_identifier(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            raise ValueError(f"Expected dict, got {type(values)}")
        account_id = values.get("account_id")
        tenant_name = values.get("tenant_name")
        if not account_id and not tenant_name:
            raise ValueError(
                "At least one of 'account_id' or 'tenant_name' must be provided"
            )
        return values

    def to_event_record_attribute(self) -> EventRecordAttribute:
        return EventRecordAttribute(
            cloud=self.cloud,
            region_name=self.region_name,
            source_name=self.source_name,
            event_name=self.event_name,
            account_id=self.account_id,
            tenant_name=self.tenant_name,
        )
