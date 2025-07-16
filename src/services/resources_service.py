from pynamodb.pagination import ResultIterator

from helpers.constants import (
    COMPOUND_KEYS_SEPARATOR,
    Cloud,
    ResourcesCollectorType,
)
from helpers.log_helper import get_logger
from models.resource import Resource
from services.base_data_service import BaseDataService

_LOG = get_logger(__name__)

try:
    from c7n.resources.resource_map import ResourceMap as AWSResourceMap
    from c7n_azure.resources.resource_map import (
        ResourceMap as AzureResourceMap,
    )
    from c7n_gcp.resources.resource_map import ResourceMap as GCPResourceMap
    from c7n_kube.resources.resource_map import ResourceMap as K8sResourceMap
except ImportError:
    _LOG.warning(
        'c7n resources are not available. '
        'Ensure that c7n, c7n-azure, and c7n-gcp packages are installed.'
    )
    AWSResourceMap = None
    AzureResourceMap = None
    GCPResourceMap = None
    K8sResourceMap = None


class ResourcesService(BaseDataService[Resource]):
    def create(
        self,
        id: str,
        name: str,
        location: str,
        resource_type: str,
        account_id: str,
        tenant_name: str,
        customer_name: str,
        data: dict,
        sync_date: float,
        collector_type: ResourcesCollectorType,
        arn: str | None = None,
    ):
        return Resource(
            id=id,
            name=name,
            location=location,
            resource_type=resource_type,
            account_id=account_id,
            tenant_name=tenant_name,
            customer_name=customer_name,
            _data=data,
            sync_date=sync_date,
            _collector_type=collector_type.value,
            arn=arn,
        )

    def update(
        self,
        resource: Resource,
        data: dict | None = None,
        sync_date: float | None = None,
    ):
        actions = []
        if data:
            actions.append(Resource._data.set(data))
            actions.append(Resource._hash.set(Resource._compute_hash(data)))
        if sync_date:
            actions.append(Resource.sync_date.set(sync_date))

        if actions:
            resource.update(actions=actions)

    def get_resource_by_id(
        self, id: str, location: str, resource_type: str, account_id: str
    ) -> Resource | None:
        res = Resource.get_nullable(
            hash_key=COMPOUND_KEYS_SEPARATOR.join(
                [account_id, location, resource_type, id]
            )
        )
        return res

    def get_resource_by_arn(self, arn: str) -> Resource | None:
        # NOTE: it's not actual scan, we use sparse index in mongo
        res = list(Resource.scan(Resource.arn == arn, limit=1))

        return res[0] if res else None

    def get_resource(
        self,
        id: str,
        name: str,
        location: str,
        resource_type: str,
        tenant_name: str,
        customer_name: str,
    ) -> Resource | None:
        filter_condition = (
            (Resource.id == id)
            & (Resource.name == name)
            & (Resource.location == location)
            & (Resource.resource_type == resource_type)
            & (Resource.tenant_name == tenant_name)
            & (Resource.customer_name == customer_name)
        )

        # NOTE: it's not actual scan, we use compound index in mongo
        # that is not supported in modular SDK
        res = list(Resource.scan(filter_condition, limit=1))
        return res[0] if res else None

    def get_resources(
        self,
        id: str | None = None,
        name: str | None = None,
        location: str | None = None,
        resource_type: str | None = None,
        tenant_name: str | None = None,
        customer_name: str | None = None,
        limit: int = 50,
        last_evaluated_key: dict | None = None,
    ) -> ResultIterator[Resource]:
        """
        Retrieves a list of resources based on the provided filters with pagination support.
        """

        filter_condition = None
        if id:
            filter_condition &= Resource.id == id
        if name:
            filter_condition &= Resource.name == name
        if location:
            filter_condition &= Resource.location == location
        if resource_type:
            filter_condition &= Resource.resource_type == resource_type
        if tenant_name:
            filter_condition &= Resource.tenant_name == tenant_name
        if customer_name:
            filter_condition &= Resource.customer_name == customer_name

        kwargs = {'limit': limit}
        if last_evaluated_key:
            kwargs['last_evaluated_key'] = last_evaluated_key

        # NOTE: it's not actual scan, we use compound index in mongo
        # that is not supported in modular SDK
        if filter_condition is not None:
            return Resource.scan(filter_condition, **kwargs)
        else:
            return Resource.scan(**kwargs)

    @staticmethod
    def get_resource_types_by_cloud(cloud: Cloud) -> list[str]:
        """
        Returns a list of resource types for the specified cloud.
        """
        if cloud == Cloud.AWS and AWSResourceMap:
            return list(AWSResourceMap.keys())
        elif cloud == Cloud.AZURE and AzureResourceMap:
            return list(AzureResourceMap.keys())
        elif cloud == Cloud.GCP and GCPResourceMap:
            return list(GCPResourceMap.keys())
        elif cloud == Cloud.K8S and K8sResourceMap:
            return list(K8sResourceMap.keys())
        else:
            _LOG.warning(f'Cannot get resource types for cloud: {cloud}')
            return []

    @staticmethod
    def cloud_to_prefix(cloud: Cloud) -> str:
        """
        Returns the cloud provider prefix for the specified cloud.
        """
        if cloud == Cloud.AWS:
            return 'aws'
        elif cloud == Cloud.AZURE:
            return 'azure'
        elif cloud == Cloud.GCP:
            return 'gcp'
        elif cloud == Cloud.K8S:
            return 'k8s'
        else:
            raise ValueError(f'Unsupported cloud: {cloud}')
