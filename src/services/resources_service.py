from datetime import date, datetime
from typing import Any

from pynamodb.pagination import ResultIterator
from modular_sdk.models.tenant import Tenant

from helpers.constants import (
    COMPOUND_KEYS_SEPARATOR,
    GLOBAL_REGION,
    Cloud,
    ResourcesCollectorType,
)
from helpers.log_helper import get_logger
from models.resource import Resource
from services.base_data_service import BaseDataService
from services.sharding import RuleMeta

_LOG = get_logger(__name__)


def _sanitize_data(data: Any) -> Any:
    """
    Recursively converts datetime objects to ISO format strings.
    PynamoDB's MapAttribute doesn't support datetime in undeclared attributes.
    """
    if isinstance(data, datetime):
        return data.isoformat()
    if isinstance(data, date):
        return data.isoformat()
    if isinstance(data, dict):
        return {k: _sanitize_data(v) for k, v in data.items()}
    if isinstance(data, (list, tuple)):
        return [_sanitize_data(item) for item in data]
    return data


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
    def remove_policy_resources(
        self, account_id: str, location: str, resource_type: str
    ):
        """
        Breaks DynamoDB's abstraction and just removes all resources using index
        """
        assert self.model_class.is_mongo_model(), 'only MongoDB is supported'
        col = self.model_class.mongo_adapter().get_collection(self.model_class)
        res = col.delete_many(
            {
                Resource.account_id.attr_name: account_id,
                Resource.location.attr_name: location,
                Resource.resource_type.attr_name: resource_type,
            }
        )
        _LOG.info(
            f'Removed {res.deleted_count} resources {account_id=}:{location=}:{resource_type=}'
        )

    def remove_platform_resources(
        self, platform_id: str, resource_type: str
    ) -> None:
        """
        Removes all the previously collected resources of the given type that
        belong to the given k8s platform
        """
        assert self.model_class.is_mongo_model(), 'only MongoDB is supported'
        col = self.model_class.mongo_adapter().get_collection(self.model_class)
        res = col.delete_many(
            {
                Resource.platform_id.attr_name: platform_id,
                Resource.resource_type.attr_name: resource_type,
            }
        )
        _LOG.info(
            f'Removed {res.deleted_count} resources '
            f'{platform_id=}:{resource_type=}'
        )

    def create(
        self,
        account_id: str,
        location: str,
        resource_type: str,
        id: str,
        name: str | None,
        arn: str | None,
        data: dict,
        sync_date: float,
        collector_type: ResourcesCollectorType,
        tenant_name: str,
        customer_name: str,
        platform_id: str | None = None,
        namespace: str | None = None,
    ) -> Resource:
        return Resource(
            account_id=account_id,
            location=location,
            resource_type=resource_type,
            id=id,
            name=name,
            arn=arn,
            _data=_sanitize_data(data),
            sync_date=sync_date,
            _collector_type=collector_type.value,
            tenant_name=tenant_name,
            customer_name=customer_name,
            platform_id=platform_id,
            namespace=namespace,
        )

    def get_resource_by_id(
        self, id: str, location: str, resource_type: str, account_id: str
    ) -> Resource | None:
        return Resource.get_nullable(
            hash_key=COMPOUND_KEYS_SEPARATOR.join(
                [account_id, location, resource_type, id]
            )
        )

    def get_resource_by_arn(self, arn: str) -> Resource | None:
        res = Resource.arn_index.query(arn, limit=1)
        return next(res, None)

    def get_resource_by_arn_and_tenant(
        self, arn: str, tenant_name: str
    ) -> Resource | None:
        """
        Retrieves a resource by ARN and tenant name.
        """
        filter_condition = Resource.tenant_name == tenant_name
        res = Resource.arn_index.query(
            arn, filter_condition=filter_condition, limit=1
        )
        return next(res, None)

    def get_resources(
        self,
        id: str | None = None,
        name: str | None = None,
        location: str | None = None,
        resource_type: str | None = None,
        tenant_name: str | None = None,
        customer_name: str | None = None,
        platform_id: str | None = None,
        namespace: str | None = None,
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
        if platform_id:
            filter_condition &= Resource.platform_id == platform_id
        if namespace:
            filter_condition &= Resource.namespace == namespace

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

    def count_type_resources_for_tenant(
        self, tenant: Tenant, metadata: dict[str, RuleMeta]
    ) -> dict[str, dict[str, int]]:
        """
        Returns the number of collected resources of the given tenant grouped
        by resource type and location:
        {'aws.ec2': {'eu-west-1': 10, 'global': 2}, ...}

        Only resource types that are mentioned in the given shards collection
        meta are taken into account. The whole thing is done within one
        aggregation that is fully covered by the `cn_1_tn_1_rt_1_l_1` index,
        so MongoDB does not have to fetch the documents themselves.
        """
        types = {
            res for rule in metadata.values() if (res := rule.get('resource'))
        }
        if not types:
            return {}

        assert self.model_class.is_mongo_model(), 'only MongoDB is supported'
        col = self.model_class.mongo_adapter().get_collection(self.model_class)

        rt = Resource.resource_type.attr_name
        loc = Resource.location.attr_name
        cursor = col.aggregate(
            [
                {
                    '$match': {
                        Resource.customer_name.attr_name: tenant.customer_name,
                        Resource.tenant_name.attr_name: tenant.name,
                        rt: {'$in': sorted(types)},
                    }
                },
                {
                    '$group': {
                        '_id': {'rt': f'${rt}', 'loc': f'${loc}'},
                        'count': {'$sum': 1},
                    }
                },
            ]
        )

        result: dict[str, dict[str, int]] = {}
        for item in cursor:
            _id = item['_id']
            result.setdefault(_id['rt'], {})[_id['loc']] = item['count']
        return result

    @staticmethod
    def resources_scanned_for_region(
        type_resources: dict[str, dict[str, int]],
        resource_type: str,
        region: str,
    ) -> int:
        """
        Number of collected resources of the given type that a policy
        executed against the given region could have scanned. Global resources
        are always included.
        """
        locations = type_resources.get(resource_type)
        if not locations:
            return 0
        count = locations.get(region, 0)
        if region != GLOBAL_REGION:
            count += locations.get(GLOBAL_REGION, 0)
        return count
