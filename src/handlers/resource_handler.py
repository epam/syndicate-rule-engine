from datetime import datetime
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
from services import SP
from services.resources_service import ResourcesService
from validators.swagger_request_models import (
    ResourcesArnGetModel,
    ResourcesGetModel,
)
from validators.utils import validate_kwargs

_LOG = get_logger(__name__)


class ResourceHandler(AbstractHandler):
    def __init__(
        self,
        modular_service: ModularServiceProvider,
        resources_service: ResourcesService,
    ):
        self._ms = modular_service
        self._rs = resources_service

    @classmethod
    def build(cls):
        return cls(
            modular_service=SP.modular_client,
            resources_service=SP.resources_service,
        )

    @property
    def mapping(self) -> Mapping:
        return {
            Endpoint.RESOURCES: {HTTPMethod.GET: self.get_resources},
            Endpoint.RESOURCES_ARN: {HTTPMethod.GET: self.get_resource_by_arn},
        }

    def _validate_tenant(
        self, tenant_name: str | None, customer: str | None = None
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
        tenant_cloud = tenant.cloud.upper() if tenant else None
        if resource_cloud and tenant_cloud and resource_cloud != tenant_cloud:
            raise ValueError(
                f'Resource type {resource_type} does not match tenant cloud {tenant_cloud}'
            )

        if resource_cloud:
            cloud = Cloud[resource_cloud]
        elif tenant_cloud:
            cloud = Cloud[tenant_cloud]
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

    def _validate_event(self, event: ResourcesGetModel):
        """
        Validate the event's parameters. Tries to add cloud provider prefix
        to resource_type if it is not specified.
        """
        try:
            tenant = self._validate_tenant(
                event.tenant_name, event.customer_id
            )

            pair = self._validate_resource_type(
                event.resource_type, tenant=tenant
            )

            event.resource_type = pair[0] if pair else None
            cloud = (pair[1] if pair else None) or (
                Cloud[tenant.cloud] if tenant else None
            )

            self._validate_location(event.location, cloud=cloud)
        except ValueError as e:
            raise (
                ResponseFactory(HTTPStatus.UNPROCESSABLE_ENTITY)
                .message(str(e))
                .exc()
            )

    def _build_resource_dto(self, resource):
        dto = {
            'id': resource.id,
            'name': resource.name,
            'location': resource.location,
            'resource_type': resource.resource_type,
            'tenant_name': resource.tenant_name,
            'customer_name': resource.customer_name,
            'data': resource.data,
            'sync_date': datetime.fromtimestamp(resource.sync_date),
        }
        if resource.arn:
            dto['arn'] = resource.arn
        
        return dto

    @validate_kwargs
    def get_resources(self, event: ResourcesGetModel):
        """
        Get resources with optional filtering and pagination.
        """
        _LOG.debug('Getting resources')

        self._validate_event(event)

        resources_iterator = self._rs.get_resources(
            id=event.id,
            name=event.name,
            location=event.location,
            resource_type=event.resource_type,
            tenant_name=event.tenant_name,
            customer_name=event.customer_id,
            limit=event.limit,
            last_evaluated_key=NextToken.deserialize(event.next_token).value,
        )

        resource_dtos = [
            self._build_resource_dto(resource)
            for resource in resources_iterator
        ]

        return (
            ResponseFactory()
            .items(
                it=resource_dtos,
                next_token=NextToken(resources_iterator.last_evaluated_key),
            )
            .build()
        )

    @validate_kwargs
    def get_resource_by_arn(self, event: ResourcesArnGetModel):
        """
        Get a resource by its ARN.
        """
        _LOG.debug(f'Getting resource by ARN: {event.arn}')

        resource = self._rs.get_resource_by_arn(event.arn)
        if not resource or resource.customer_name != event.customer_id:
            raise (
                ResponseFactory(HTTPStatus.NOT_FOUND)
                .message(f'Resource with ARN {event.arn} not found')
                .exc()
            )

        return (
            ResponseFactory()
            .data(data=self._build_resource_dto(resource))
            .build()
        )
