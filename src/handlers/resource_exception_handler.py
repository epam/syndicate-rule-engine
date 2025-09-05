from datetime import datetime, date
from http import HTTPStatus

from modular_sdk.models.tenant import Tenant
from modular_sdk.modular import ModularServiceProvider

from handlers import AbstractHandler, Mapping
from helpers import NextToken
from helpers.constants import Cloud, Endpoint, HTTPMethod
from helpers.lambda_response import ResponseFactory
from helpers.log_helper import get_logger
from helpers.regions import (
    AllRegionsWithGlobal,
    get_region_by_cloud_with_global,
)
from models.resource_exception import ResourceException
from services import SP
from services.resources_service import ResourcesService
from services.resource_exception_service import ResourceExceptionsService
from validators.swagger_request_models import (
    BaseModel,
    ResourcesExceptionsGetModel,
    ResourcesExceptionsPostModel
)
from validators.utils import validate_kwargs

_LOG = get_logger(__name__)


class ResourceExceptionHandler(AbstractHandler):
    def __init__(
        self,
        modular_service: ModularServiceProvider,
        resources_service: ResourcesService,
        resource_exception_service: ResourceExceptionsService,
    ):
        self._ms = modular_service
        self._rs = resources_service
        self._res = resource_exception_service

    @classmethod
    def build(cls):
        return cls(
            modular_service=SP.modular_client,
            resources_service=SP.resources_service,
            resource_exception_service=SP.resource_exception_service,
        )

    @property
    def mapping(self) -> Mapping:
        return {
            Endpoint.RESOURCES_EXCEPTIONS: {
                HTTPMethod.GET: self.get_resource_exceptions,
                HTTPMethod.POST: self.create_resource_exception,
            },
            Endpoint.RESOURCES_EXCEPTIONS_ID: {
                HTTPMethod.GET: self.get_resource_exception_by_id,
                HTTPMethod.PUT: self.update_resource_exception,
                HTTPMethod.DELETE: self.delete_resource_exception,
            },
        }
    
    def _to_timestamp(self, dt: datetime | date) -> float:
        if isinstance(dt, datetime):
            return dt.timestamp()
        elif isinstance(dt, date):
            return datetime.combine(dt, datetime.min.time()).timestamp()
        else:
            _LOG.error(f'Unsupported type: {type(dt)}')
            raise TypeError(f'Unsupported type: {type(dt)}')

    def _validate_tenant(
        self,
        tenant_name: str | None,
        customer: str | None = None,
    ) -> Tenant | None:
        if not tenant_name:
            return None

        tenant = self._ms.tenant_service().get(tenant_name)
        if not tenant or not tenant.is_active:
            raise ValueError(f'Tenant {tenant_name} does not exist')

        if customer and tenant.customer_name != customer:
            raise ValueError(
                f'Tenant {tenant_name} does not belong to customer {customer}'
            )

        return tenant

    def _validate_resource_type(
        self, resource_type: str | None, tenant: Tenant | None = None
    ) -> tuple[str, Cloud] | None:
        if not resource_type:
            return None

        resource_cloud = None
        if '.' in resource_type:
            resource_cloud = resource_type.split('.')[0].upper()
        tenant_cloud = tenant.cloud if tenant else None
        if resource_cloud and tenant_cloud and resource_cloud != tenant_cloud:
            raise ValueError(
                f'Resource type {resource_type} does not match tenant cloud {tenant_cloud}'
            )

        if resource_cloud:
            cloud = Cloud[resource_cloud]
        elif tenant_cloud:
            cloud = Cloud[tenant_cloud.upper()]
        else:
            cloud = Cloud.AWS

        if not resource_cloud:
            resource_type = (
                f'{self._rs.cloud_to_prefix(cloud)}.{resource_type}'
            )

        resources_types = self._rs.get_resource_types_by_cloud(cloud)

        if resource_type not in resources_types:
            raise ValueError(
                f'Resource type {resource_type} is not supported for cloud {cloud}'
            )

        return resource_type, cloud

    def _validate_location(
        self, location: str | None, cloud: Cloud | None = None
    ):
        if not location:
            return None

        if cloud:
            locations = get_region_by_cloud_with_global(cloud)
        else:
            locations = AllRegionsWithGlobal

        if location not in locations:
            raise ValueError(
                f'Location {location} is not supported for cloud {cloud}'
            )
    
    def _validate_arn(self, arn: str | None, tenant: Tenant | None = None) -> None:
        if not arn:
            return None

        if not tenant:
            return None

        if tenant.project and tenant.project not in arn:
            raise ValueError(
                f'ARN {arn} does not match tenant account id {tenant.project}'
            )

    def _validate_resource_exception(
        self,
        customer_id: str | None = None,
        tenant_name: str | None = None,
        resource_type: str | None = None,
        location: str | None = None,
        arn: str | None = None
    ):
        """
        Validate the event's parameters. Tries to add cloud provider prefix
        to resource_type if it is not specified.
        """
        try:
            tenant = self._validate_tenant(tenant_name, customer_id)

            pair = self._validate_resource_type(resource_type, tenant)

            resource_type = pair[0] if pair else None
            cloud = (pair[1] if pair else None) or (
                Cloud[tenant.cloud] if tenant else None
            )

            self._validate_location(location, cloud)

            self._validate_arn(arn, tenant)

            return resource_type
        except ValueError as e:
            _LOG.warning(str(e))
            raise ResponseFactory(HTTPStatus.UNPROCESSABLE_ENTITY).message(str(e)).exc()

    def _build_resource_exception_dto(self, resource_exc: ResourceException) -> dict:
        dto = {
            'id': resource_exc.id,
            'type': resource_exc.type.value,
            'tenant_name': resource_exc.tenant_name,
            'customer_name': resource_exc.customer_name,
            'created_at': resource_exc.created_at,
            'updated_at': resource_exc.updated_at,
            # we store datetime object in mongodb
            # this because we need for it to be TTL
            'expire_at': self._to_timestamp(resource_exc.expire_at),
        }
        if resource_exc.resource_id:
            dto['resource_id'] = resource_exc.resource_id
        if resource_exc.location:
            dto['location'] = resource_exc.location 
        if resource_exc.resource_type:
            dto['resource_type'] = resource_exc.resource_type
        if resource_exc.arn:
            dto['arn'] = resource_exc.arn
        if resource_exc.tags_filters:
            dto['tags_filters'] = resource_exc.tags_filters

        return dto

    @validate_kwargs
    def get_resource_exceptions(self, event: ResourcesExceptionsGetModel):
        """
        Get resources with optional filtering and pagination.
        """
        _LOG.debug('Getting resources')

        resource_type = self._validate_resource_exception(
            customer_id=event.customer_id,
            tenant_name=event.tenant_name,
            resource_type=event.resource_type,
            location=event.location,
            arn=event.arn
        )

        resources_iterator = self._res.get_resources_exceptions(
            resource_id=event.resource_id,
            location=event.location,
            resource_type=resource_type,
            tenant_name=event.tenant_name,
            customer_name=event.customer_id,
            arn=event.arn,
            tags_filters=event.tags_filters,
            limit=event.limit,
            last_evaluated_key=NextToken.deserialize(event.next_token).value,
        )

        resources_exc_dtos = [
            self._build_resource_exception_dto(resource)
            for resource in resources_iterator
        ]

        return (
            ResponseFactory()
            .items(
                it=resources_exc_dtos,
                next_token=NextToken(resources_iterator.last_evaluated_key),
            )
            .build()
        )

    @validate_kwargs
    def get_resource_exception_by_id(self, event: BaseModel, id:str):
        """
        Get a resource exception by its ID.
        """
        _LOG.debug(f'Getting resource exception by ID: {id}')

        resource_exception = self._res.get_resource_exception_by_id(id)
        if not resource_exception:
            raise (
                ResponseFactory(HTTPStatus.NOT_FOUND)
                .message(f'Resource exception with ID {id} not found')
                .exc()
            )

        return (
            ResponseFactory()
            .data(data=self._build_resource_exception_dto(resource_exception))
            .build()
        )

    @validate_kwargs
    def create_resource_exception(self, event: ResourcesExceptionsPostModel):
        """
        Create a new resource exception.
        """
        _LOG.debug('Creating resource exception')

        resource_type =self._validate_resource_exception(
            customer_id=event.customer_id,
            tenant_name=event.tenant_name,
            resource_type=event.resource_type,
            location=event.location,
            arn=event.arn
        )

        resource_exception = self._res.create(
            resource_id=event.resource_id,
            location=event.location,
            resource_type=resource_type,
            tenant_name=event.tenant_name,
            customer_name=event.customer_id,
            arn=event.arn,
            tags_filters=event.tags_filters,
            expire_at=self._to_timestamp(event.expire_at)
        )
        self._res.save(resource_exception)

        return (
            ResponseFactory()
            .data(data=self._build_resource_exception_dto(resource_exception))
            .build()
        )

    @validate_kwargs
    def update_resource_exception(
        self, event: ResourcesExceptionsPostModel, id: str
    ):
        """
        Update a resource exception by its ID.
        """

        resource_type = self._validate_resource_exception(
            customer_id=event.customer_id,
            tenant_name=event.tenant_name,
            resource_type=event.resource_type,
            location=event.location,
            arn=event.arn
        )

        try:
            resource_exception = self._res.update_resource_exception_by_id(
                id=id,
                expire_at=self._to_timestamp(event.expire_at),
                resource_id=event.resource_id,
                location=event.location,
                resource_type=resource_type,
                tenant_name=event.tenant_name,
                customer_name=event.customer_id,
                arn=event.arn,
                tags_filters=event.tags_filters,
            )
        except ValueError as e:
            _LOG.warning(str(e))
            raise (
                ResponseFactory(HTTPStatus.NOT_FOUND)
                .message(str(e))
                .exc()
            )

        return (
            ResponseFactory()
            .data(data=self._build_resource_exception_dto(resource_exception))
            .build()
        )

    @validate_kwargs
    def delete_resource_exception(self, event: BaseModel, id: str):
        """
        Delete a resource exception by its ID.
        """
        _LOG.debug(f'Deleting resource exception by ID: {id}')

        try:
            self._res.delete_by_id(id)
        except ValueError as e:
            _LOG.warning(str(e))
            raise (
                ResponseFactory(HTTPStatus.NOT_FOUND)
                .message(str(e))
                .exc()
            )

        return (
            ResponseFactory()
            .message(f'Resource exception with ID {id} deleted')
            .build()
        )
