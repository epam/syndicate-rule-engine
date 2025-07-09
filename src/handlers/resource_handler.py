from handlers import AbstractHandler, Mapping
from helpers import NextToken
from helpers.constants import CustodianEndpoint, HTTPMethod
from helpers.lambda_response import ResponseFactory
from helpers.log_helper import get_logger
from services import SP
from services.resources_service import ResourcesService
from validators.swagger_request_models import ResourcesGetModel
from validators.utils import validate_kwargs

_LOG = get_logger(__name__)


class ResourceHandler(AbstractHandler):
    
    def __init__(self, resources_service: ResourcesService):
        self._resources_service = resources_service

    @classmethod
    def build(cls):
        return cls(
            resources_service=SP.resources_service
        )

    @property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.RESOURCES: {
                HTTPMethod.GET: self.get_resources,
            }
        }

    @validate_kwargs
    def get_resources(self, event: ResourcesGetModel):
        """
        Get resources with optional filtering and pagination.
        """
        _LOG.debug('Getting resources')
        
        resources_iterator = self._resources_service.get_resources(
            id=event.id,
            name=event.name,
            location=event.location,
            resource_type=event.resource_type,
            tenant_name=event.tenant_name,
            customer_name=event.customer_name,
            limit=event.limit,
            last_evaluated_key=NextToken.deserialize(event.next_token).value
        )
        
        resources = list(resources_iterator)
        
        resource_dtos = []
        for resource in resources:
            resource_dtos.append({
                'id': resource.id,
                'name': resource.name,
                'location': resource.location,
                'resource_type': resource.resource_type,
                'tenant_name': resource.tenant_name,
                'customer_name': resource.customer_name,
                'data': resource.data,
                'sync_date': resource.sync_date,
            })
        
        return ResponseFactory().items(
            it=resource_dtos,
            next_token=NextToken(resources_iterator.last_evaluated_key)
        ).build()