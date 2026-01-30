from typing import Generator

from helpers.constants import Cloud, ResourcesCollectorType
from models.resource import Resource
from services.metadata import EMPTY_RULE_METADATA
from services.resources import (
    to_aws_resources,
    to_azure_resources,
    to_google_resources,
    to_k8s_resources,
)
from services.resources_service import ResourcesService
from services.sharding import ShardPart

from .base import ResourceIteratorStrategy


class AwsResourceIterator(ResourceIteratorStrategy):
    """Strategy for iterating AWS resources."""

    def iterate(
        self,
        part: ShardPart,
        account_id: str,
        location: str,
        resource_type: str,
        customer_name: str,
        tenant_name: str,
        resources_service: ResourcesService,
        collector_type: ResourcesCollectorType,
    ) -> Generator[Resource, None, None]:
        for res in to_aws_resources(
            part, resource_type, EMPTY_RULE_METADATA, account_id
        ):
            yield resources_service.create(
                account_id=account_id,
                location=location,
                resource_type=resource_type,
                id=res.id,
                name=res.name,
                arn=res.arn,
                data=res.data,
                sync_date=part.timestamp,
                collector_type=collector_type,
                tenant_name=tenant_name,
                customer_name=customer_name,
            )


class AzureResourceIterator(ResourceIteratorStrategy):
    """Strategy for iterating Azure resources."""

    def iterate(
        self,
        part: ShardPart,
        account_id: str,
        location: str,
        resource_type: str,
        customer_name: str,
        tenant_name: str,
        resources_service: ResourcesService,
        collector_type: ResourcesCollectorType,
    ) -> Generator[Resource, None, None]:
        for res in to_azure_resources(part, resource_type):
            yield resources_service.create(
                account_id=account_id,
                location=location,
                resource_type=resource_type,
                id=res.id,
                name=res.name,
                arn=res.id,
                data=res.data,
                sync_date=part.timestamp,
                collector_type=collector_type,
                tenant_name=tenant_name,
                customer_name=customer_name,
            )


class GoogleResourceIterator(ResourceIteratorStrategy):
    """Strategy for iterating Google Cloud resources."""

    def iterate(
        self,
        part: ShardPart,
        account_id: str,
        location: str,
        resource_type: str,
        customer_name: str,
        tenant_name: str,
        resources_service: ResourcesService,
        collector_type: ResourcesCollectorType,
    ) -> Generator[Resource, None, None]:
        for res in to_google_resources(
            part, resource_type, EMPTY_RULE_METADATA, account_id
        ):
            yield resources_service.create(
                account_id=account_id,
                location=location,
                resource_type=resource_type,
                id=res.id,
                name=res.name,
                arn=res.urn,
                data=res.data,
                sync_date=part.timestamp,
                collector_type=collector_type,
                tenant_name=tenant_name,
                customer_name=customer_name,
            )


class K8sResourceIterator(ResourceIteratorStrategy):
    """Strategy for iterating Kubernetes resources."""

    def iterate(
        self,
        part: ShardPart,
        account_id: str,
        location: str,
        resource_type: str,
        customer_name: str,
        tenant_name: str,
        resources_service: ResourcesService,
        collector_type: ResourcesCollectorType,
    ) -> Generator[Resource, None, None]:
        for res in to_k8s_resources(part, resource_type):
            yield resources_service.create(
                account_id=account_id,
                location=location,
                resource_type=resource_type,
                id=res.id,
                name=res.name,
                arn=res.id,
                data=res.data,
                sync_date=part.timestamp,
                collector_type=collector_type,
                tenant_name=tenant_name,
                customer_name=customer_name,
            )


_RESOURCE_ITERATOR_REGISTRY: dict[Cloud, ResourceIteratorStrategy] = {
    Cloud.AWS: AwsResourceIterator(),
    Cloud.AZURE: AzureResourceIterator(),
    Cloud.GOOGLE: GoogleResourceIterator(),
    Cloud.GCP: GoogleResourceIterator(),
    Cloud.KUBERNETES: K8sResourceIterator(),
    Cloud.K8S: K8sResourceIterator(),
}


def get_resource_iterator(cloud: Cloud) -> ResourceIteratorStrategy | None:
    """Get the appropriate resource iterator strategy for the given cloud."""
    return _RESOURCE_ITERATOR_REGISTRY.get(cloud)

