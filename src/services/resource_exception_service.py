from uuid import uuid4
from time import time
from datetime import datetime, timezone

from pynamodb.pagination import ResultIterator

from helpers import get_path
from helpers.constants import Cloud
from helpers.log_helper import get_logger
from models.resource_exception import ResourceException
from services.base_data_service import BaseDataService
from services.resources import load_manager, prepare_resource_type

_LOG = get_logger(__name__)


class ResourcesExceptionService(BaseDataService[ResourceException]):
    def create(
        self,
        resource_id: str | None,
        location: str | None,
        resource_type: str | None,
        tenant_name: str | None,
        customer_name: str,
        arn: str | None,
        tags_filters: list[str] | None,
        expire_at: float,
    ) -> ResourceException:
        return ResourceException(
            id=str(uuid4()),
            resource_id=resource_id,
            location=location,
            resource_type=resource_type,
            tenant_name=tenant_name,
            customer_name=customer_name,
            arn=arn,
            tags_filters=tags_filters,
            created_at=time(),
            updated_at=time(),
            expire_at=datetime.fromtimestamp(expire_at, tz=timezone.utc),
        )

    def delete_by_id(self, id: str) -> None:
        """
        Delete a resource exception by its ID.
        """
        resource_exception = ResourceException.get_nullable(id)
        if resource_exception:
            resource_exception.delete()
        else:
            raise ValueError(f'Resource exception with ID {id} not found')

    def get_resource_exception_by_id(
        self, id: str
    ) -> ResourceException | None:
        """
        Get a resource exception by its ID.
        """
        return ResourceException.get_nullable(id)

    def get_resources_exceptions(
        self,
        resource_id: str | None = None,
        location: str | None = None,
        resource_type: str | None = None,
        tenant_name: str | None = None,
        customer_name: str | None = None,
        arn: str | None = None,
        tags_filters: list[str] | None = None,
        limit: int = 100,
        last_evaluated_key: dict | None = None,
    ) -> ResultIterator[ResourceException]:
        filter_condition = None
        if resource_id:
            filter_condition &= ResourceException.resource_id == resource_id
        if location:
            filter_condition &= ResourceException.location == location
        if resource_type:
            filter_condition &= (
                ResourceException.resource_type == resource_type
            )
        if tenant_name:
            filter_condition &= ResourceException.tenant_name == tenant_name
        if customer_name:
            filter_condition &= (
                ResourceException.customer_name == customer_name
            )
        if arn:
            filter_condition &= ResourceException.arn == arn

        kwargs = {'limit': limit}
        if last_evaluated_key:
            kwargs['last_evaluated_key'] = last_evaluated_key

        # NOTE: it's not actual scan, we use compound index in mongo
        # that is not supported in modular SDK
        if filter_condition is not None:
            return ResourceException.scan(filter_condition, **kwargs)
        else:
            return ResourceException.scan(**kwargs)

    def update_resource_exception_by_id(
        self,
        id: str,
        expire_at: float,
        resource_id: str | None = None,
        location: str | None = None,
        resource_type: str | None = None,
        tenant_name: str | None = None,
        customer_name: str | None = None,
        arn: str | None = None,
        tags_filters: list[str] | None = None,
    ) -> ResourceException:
        """
        Update a resource exception by its ID.
        """
        resource_exception = self.get_resource_exception_by_id(id)
        if not resource_exception:
            raise ValueError(f'Resource exception with ID {id} not found')

        resource_exception.resource_id = resource_id
        resource_exception.location = location
        resource_exception.resource_type = resource_type
        resource_exception.tenant_name = tenant_name
        resource_exception.customer_name = customer_name
        resource_exception.arn = arn
        resource_exception.tags_filters = tags_filters
        resource_exception.updated_at = time()
        resource_exception.expire_at = datetime.fromtimestamp(expire_at, tz=timezone.utc)

        resource_exception.save()

        return resource_exception
