from abc import ABC, abstractmethod
from typing import Iterable
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from importlib import import_module
from functools import lru_cache
import time

from azure.mgmt.resourcegraph import ResourceGraphClient
from azure.mgmt.resourcegraph.models import QueryRequest, QueryRequestOptions

from c7n import utils
from c7n.version import version
from c7n.provider import get_resource_class
from c7n.policy import Policy, execution, PolicyExecutionMode
from c7n.exceptions import ResourceLimitExceeded
from c7n.resources.resource_map import ResourceMap as AWSResourceMap
from c7n_azure.resources.resource_map import ResourceMap as AzureResourceMap
from c7n_gcp.resources.resource_map import ResourceMap as GCPResourceMap

from modular_sdk.modular import ModularServiceProvider
from modular_sdk.models.tenant import Tenant

from helpers.constants import Cloud, EXCLUDE_RESOURCE_TYPES, ResourcesCollectorType
from helpers.log_helper import get_logger
from helpers.regions import get_region_by_cloud_with_global
from executor.job import (
    PolicyDict,
    job_initializer,
    PoliciesLoader,
    get_tenant_credentials,
)
from services import SP
from services.resources_service import ResourcesService

_LOG = get_logger(__name__)

def get_factory(class_path: str):
    """
    Returns a factory function that imports a class from the given path.
    """
    module_path, class_name = class_path.rsplit('.', 1)
    module = import_module(module_path)
    return getattr(module, class_name)

@lru_cache(maxsize=1)
def azure_custodian_resource_type_map() -> dict[str, str]:
    """
    Maps Azure resource types to Cloud Custodian resource types.
    """
    type_map = {}
    for resource_type, class_ in AzureResourceMap.items():
        type_map[get_factory(class_).resource_type.resource_type.lower()] = resource_type
    return type_map
        

# CC by default stores retrieved information on disk
# This mode turn off this behavior
# Though it stops storing scan's metadata
@execution.register('collect')
class CollectMode(PolicyExecutionMode):
    """
    Queries resources from cloud provider for filtering and actions.
    Do not store them on disk.
    """

    schema = utils.type_schema('pull')

    def run(self, *args, **kw):
        if not self.policy.is_runnable():
            return []

        self.policy.log.debug(
            'Running policy:%s resource:%s region:%s c7n:%s',
            self.policy.name,
            self.policy.resource_type,
            self.policy.options.region or 'default',
            version,
        )

        s = time.time()
        try:
            resources = self.policy.resource_manager.resources()
        except ResourceLimitExceeded as e:
            self.policy.log.error(str(e))
            raise

        rt = time.time() - s
        self.policy.log.info(
            'policy:%s resource:%s region:%s count:%d time:%0.2f',
            self.policy.name,
            self.policy.resource_type,
            self.policy.options.region,
            len(resources),
            rt,
        )

        if not resources:
            return []

        if self.policy.options.dryrun:
            self.policy.log.debug('dryrun: skipping actions')
            return resources

        for a in self.policy.resource_manager.actions:
            s = time.time()
            self.policy.log.info(
                'policy:%s action:%s'
                ' resources:%d'
                ' execution_time:%0.2f'
                % (self.policy.name, a.name, len(resources), time.time() - s)
            )
        return resources

class BaseResourceCollector(ABC):
    """
    Abstract base class for resource collectors.
    All resource collectors should inherit from this class and implement its methods.
    """

    @property
    @abstractmethod
    def collector_type(self) -> ResourcesCollectorType: ...

    @abstractmethod
    def collect_tenant_resources(
        self,
        tenant_name: str,
        regions: Iterable[str] | None = None,
        resource_types: Iterable[str] | None = None,
        workers: int = 10,
    ): ...

    @abstractmethod
    def collect_all_resources(
        self,
        regions: Iterable[str] | None = None,
        resource_types: Iterable[str] | None = None,
        workers: int = 10,
    ): ...

# TODO: add resource retrieval for Kubernetes
class CustodianResourceCollector(BaseResourceCollector):
    """
    Collects resources from cloud using Cloud Custodian policies.
    It first creates policies with no filters for specified resource types and
    regions, by default it collects all resources for the cloud.
    Then it runs the policies to collect resources and saves them in temp folder.
    After that, all the resource are saved in the mongodb.
    """

    collector_type = ResourcesCollectorType.CUSTODIAN

    def __init__(
        self,
        modular_service: ModularServiceProvider,
        resources_service: ResourcesService,
    ):
        self._ms = modular_service
        self._rs = resources_service

    @classmethod
    def build(cls) -> 'CustodianResourceCollector':
        """
        Builds a ResourceCollector instance.
        """
        return CustodianResourceCollector(
            modular_service=SP.modular_client,
            resources_service=SP.resources_service,
        )

    @staticmethod
    def _get_policies(
        resource_map: dict,
        cloud: Cloud,
        resource_types: Iterable[str] | None = None,
        regions: Iterable[str] | None = None,
    ) -> list[Policy]:
        if not resource_types:
            resource_types = set(resource_map.keys())
        else:
            if not all(rt in resource_map.keys() for rt in resource_types):
                raise ValueError(
                    f'Some resource types {resource_types} are not valid {cloud} resource types.'
                )
            resource_types = set(resource_types)

        if not regions:
            regions = (
                get_region_by_cloud_with_global(cloud)
                if cloud == Cloud.AWS
                else {'global'}
            )
        else:
            if not all(
                region in get_region_by_cloud_with_global(cloud)
                for region in regions
            ):
                raise ValueError(
                    f'Some regions {regions} are not valid {cloud} regions.'
                )
            regions = set(regions)

        policy_dicts = []
        for resource_type in resource_types - EXCLUDE_RESOURCE_TYPES:
            policy = {
                'name': f'retrieve-{resource_type}',
                'resource': resource_type,
                'description': f'policy to retrieve all {resource_type} resources',
                'mode': {'type': 'collect'},
            }
            policy_dicts.append(PolicyDict(**policy))
        
        # TODO: PolicyLoader ignores regions. We should get around that
        policy_loader = PoliciesLoader(cloud=cloud, regions=regions)

        return policy_loader.load_from_policies(policy_dicts)

    @staticmethod
    def get_aws_policies(
        resource_types: Iterable[str] | None = None,
        regions: Iterable[str] | None = None,
    ) -> list[Policy]:
        return CustodianResourceCollector._get_policies(
            resource_map=AWSResourceMap,
            cloud=Cloud.AWS,
            resource_types=resource_types,
            regions=regions,
        )

    @staticmethod
    def get_azure_policies(
        resource_types: Iterable[str] | None = None,
        regions: Iterable[str] | None = None,
    ) -> list[Policy]:
        return CustodianResourceCollector._get_policies(
            resource_map=AzureResourceMap,
            cloud=Cloud.AZURE,
            resource_types=resource_types,
            regions=regions,
        )

    @staticmethod
    def get_gcp_policies(
        resource_types: Iterable[str] | None = None,
        regions: Iterable[str] | None = None,
    ) -> list[Policy]:
        return CustodianResourceCollector._get_policies(
            resource_map=GCPResourceMap,
            cloud=Cloud.GCP,
            resource_types=resource_types,
            regions=regions,
        )

    # TODO: better error handling
    def _process_policy(
        self,
        policy: Policy,
        resource_type: str,
        region: str,
        account_id: str,
        tenant_name: str,
        customer_name: str,
    ) -> bool:
        try:
            resources = policy()
            for data in resources:
                self._load_scan(
                    data,
                    resource_type,
                    region,
                    account_id,
                    tenant_name,
                    customer_name,
                )
            return True
        except Exception as e:
            _LOG.error(f'Error processing policy {policy.name}: {e}')
            return False

    # TODO: add creds handling for Kube
    @staticmethod
    def _get_credentials(tenant: Tenant) -> dict:
        credentials = get_tenant_credentials(tenant)

        if credentials is None:
            raise ValueError(
                f'No credentials found for tenant {tenant.name}'
            )

        return credentials

    def _load_scan(
        self,
        data: dict,
        resource_type: str,
        region: str,
        account_id: str,
        tenant_name: str,
        customer_name: str,
    ):
        """
        Builds a CloudResource object from the data collected by the policy.
        """
        resource_class = get_resource_class(resource_type)

        resource_id = data[resource_class.resource_type.id]

        if hasattr(resource_class.resource_type, 'name'):
            resource_name = data.get(resource_class.resource_type.name, '')
        elif 'name' in data:
            resource_name = data['name']
        else:
            _LOG.warning(
                f"Resource {resource_type} does not have a 'name' field in data: {data}"
            )
            resource_name = ''
        
        # TODO: generate arn if not present
        if hasattr(resource_class.resource_type, 'arn'):
            arn = data.get(resource_class.resource_type.arn, None)
        else:
            arn = None

        self._rs.create(
            id=resource_id,
            name=resource_name,
            location=region,
            resource_type=resource_type,
            account_id=account_id,
            tenant_name=tenant_name,
            customer_name=customer_name,
            data=data,
            sync_date=datetime.now(timezone.utc).timestamp(),
            collector_type=self.collector_type,
            arn=arn,
        ).save()

    def collect_tenant_resources(
        self,
        tenant_name: str,
        regions: Iterable[str] | None = None,
        resource_types: Iterable[str] | None = None,
        workers: int = 10,
    ):
        tenant = self._ms.tenant_service().get(tenant_name)
        if not tenant:
            raise ValueError(f'Tenant {tenant_name} not found')

        cloud = Cloud(tenant.cloud)

        if cloud == Cloud.AWS:
            policies = self.get_aws_policies(
                resource_types=resource_types, regions=regions
            )
        elif cloud == Cloud.AZURE:
            policies = self.get_azure_policies(
                resource_types=resource_types, regions=regions
            )
        elif cloud == Cloud.GCP:
            policies = self.get_gcp_policies(
                resource_types=resource_types, regions=regions
            )
        else:
            raise ValueError(f'Unsupported cloud: {cloud}')

        _LOG.info(f'Starting resource collection for tenant: {tenant_name}')
        _LOG.info(f'Policies to run: {len(policies)}')

        credentials = CustodianResourceCollector._get_credentials(tenant)
        account_id = tenant.project if tenant.project is not None else ""
        with ThreadPoolExecutor(
            max_workers=workers,
            initializer=job_initializer,
            initargs=(credentials,),
        ) as executor:
            for policy in policies:
                executor.submit(
                    self._process_policy,
                    policy,
                    policy.resource_type,
                    policy.options.region,
                    account_id,
                    tenant.name,
                    tenant.customer_name,
                )

    def collect_all_resources(
        self,
        regions: Iterable[str] | None = None,
        resource_types: Iterable[str] | None = None,
        workers: int = 10,
    ):
        """
        Collects resources for all tenants.
        """
        tenants = self._ms.tenant_service().scan_tenants(only_active=True)
        for tenant in tenants:
            try:
                _LOG.info(f'Collecting resources for tenant: {tenant.name}')
                self.collect_tenant_resources(
                    tenant_name=tenant.name,
                    regions=regions,
                    resource_types=resource_types,
                    workers=workers,
                )
            except Exception as e:
                _LOG.error(
                    f'Error collecting resources for tenant {tenant.name}: {e}'
                )

class AzureGraphResourceCollector(BaseResourceCollector):
    """ 
    Collect resources from Azure using Resource Management API.
    """

    collector_type = ResourcesCollectorType.AZURE_RESOURCE_GRAPH

    def __init__(
        self,
        modular_service: ModularServiceProvider,
        resources_service: ResourcesService,
    ):
        self._ms = modular_service
        self._rs = resources_service

    def _get_credentials(self, tenant: Tenant) -> dict:
        credentials = get_tenant_credentials(tenant)
        if not credentials:
            raise ValueError(f'No credentials found for tenant {tenant.name}')

        return credentials

    def _load_scan(
        self,
        data: dict,
        account_id: str,
        tenant_name: str,
        customer_name: str,
    ):
        resource_id = data['id']
        resource_name = data.get('name') or resource_id
        location = data['location']
        
        resource_type = azure_custodian_resource_type_map().get(
            data['type'].lower()
        )
        if not resource_type:
            _LOG.warning(
                f"Resource type {data['type']} not found in Azure resource map. "
                "Skipping resource."
            )
            return

        self._rs.create(
            id=resource_id,
            name=resource_name,
            location=location,
            resource_type=resource_type,
            account_id=account_id,
            tenant_name=tenant_name,
            customer_name=customer_name,
            data=data,
            sync_date=datetime.now(timezone.utc).timestamp(),
            collector_type=self.collector_type,
        ).save()
    
    def collect_tenant_resources(
        self,
        tenant_name: str,
        regions: Iterable[str] | None = None,
        resource_types: Iterable[str] | None = None,
        workers: int = 10,
    ):
        tenant = self._ms.tenant_service().get(tenant_name)
        if not tenant:
            raise ValueError(f'Tenant {tenant_name} not found')
        account_id = tenant.project if tenant.project else ""


        resource_graph_client = ResourceGraphClient(
            credential=self._get_credentials(tenant)
        )
        
        # NOTE: we can use just `resources` query to get all resources
        # but it will return all the attributes of resources that can be too much
        query = """
        resources
        | project id, name, type, location, tags
        """
        if regions:
            regions = [region.lower() for region in regions]
            query += ' | where location in ' + ', '.join(f"'{r}'" for r in regions)
        if resource_types:
            resource_types = [rt.lower() for rt in resource_types]
            query += ' | where type in ' + ', '.join(f"'{rt}'" for rt in resource_types)

        resources = []
        skip = 0
        top = 1000
        
        try:
            while True:
                query_options = QueryRequestOptions(
                    skip=skip,
                    top=top,
                    result_format='objectArray'
                )
                
                query_request = QueryRequest(
                    subscriptions=[account_id],
                    query=query,
                    options=query_options
                )
                
                response = resource_graph_client.resources(query_request).as_dict()
                resources.extend(response['data'])

                skip += top
                if skip >= response['total_records']:
                    break
                
        except Exception as e:
            _LOG.error(f"Failed to collect Azure resources using Resource Graph: {e}")
            raise
            
        for resource in resources:
            self._load_scan(
                data=resource,
                account_id=account_id,
                tenant_name=tenant.name,
                customer_name=tenant.customer_name,
            )

    def collect_all_resources(
        self,
        regions: Iterable[str] | None = None,
        resource_types: Iterable[str] | None = None,
        workers: int = 10,
    ):
        """
        Collect all Azure resources in the subscription using Resource Graph.
        """
        tenants = self._ms.tenant_service().scan_tenants(only_active=True)
        for tenant in tenants:
            if tenant.cloud != Cloud.AZURE:
                continue
            try:
                _LOG.info(f'Collecting resources for tenant: {tenant.name}')
                self.collect_tenant_resources(
                    tenant_name=tenant.name,
                    regions=regions,
                    resource_types=resource_types,
                    workers=workers,
                )
            except Exception as e:
                _LOG.error(
                    f'Error collecting resources for tenant {tenant.name}: {e}'
                )