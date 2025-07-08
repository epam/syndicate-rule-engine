from typing import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from hashlib import sha256
import time

from msgspec.json import encode


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

from executor.helpers.constants import ExecutorError
from helpers.constants import Cloud
from helpers.log_helper import get_logger
from helpers.regions import CLOUD_REGIONS
from executor.job import (
    ExecutorException,
    PolicyDict, 
    job_initializer, 
    PoliciesLoader,
    get_tenant_credentials
)
from models.resource import Resource
from services import SP

_LOG = get_logger(__name__)

# CC by default stores retrieved information on disk
# This mode turn off this behavior
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
            "Running policy:%s resource:%s region:%s c7n:%s",
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
            "policy:%s resource:%s region:%s count:%d time:%0.2f",
            self.policy.name,
            self.policy.resource_type,
            self.policy.options.region,
            len(resources),
            rt,
        )

        if not resources:
            return []

        if self.policy.options.dryrun:
            self.policy.log.debug("dryrun: skipping actions")
            return resources

        for a in self.policy.resource_manager.actions:
            s = time.time()
            self.policy.log.info(
                "policy:%s action:%s"
                " resources:%d"
                " execution_time:%0.2f"
                % (self.policy.name, a.name, len(resources), time.time() - s)
            )
        return resources

# TODO: add resource retrieval for Kubernetes
class ResourceCollector:
    """
    Collects resources from cloud using Cloud Custodian policies.
    It first creates policies with no filters for specified resource types and 
    regions, by default it collects all resources for the cloud.
    Then it runs the policies to collect resources and saves them in temp folder.
    After that, all the resource are saved in the mongodb.
    """

    def __init__(self, modular_service: ModularServiceProvider):
        self._ms = modular_service

    @classmethod
    def build(cls) -> 'ResourceCollector':
        """
        Builds a ResourceCollector instance.
        """
        return ResourceCollector(
            modular_service=SP.modular_client
        )

    def _get_policies(
            self,
            resource_map: dict,
            cloud: Cloud,
            resource_types: Iterable[str] | None = None,
            regions: Iterable[str] | None = None,
    ) -> list[Policy]:
        if not resource_types:
            resource_types = list(resource_map.keys())
        else:
            if not all(rt in resource_map.keys() for rt in resource_types):
                raise ValueError(f"Some resource types {resource_types} are not valid {cloud} resource types.")

        if not regions:
            regions = CLOUD_REGIONS[cloud] | {'global'}
        else:
            if not all(region in CLOUD_REGIONS[cloud] | {'global'} for region in regions):
                raise ValueError(f"Some regions {regions} are not valid {cloud} regions.")
            regions = set(regions)

        policy_dicts = []
        for resource_type in resource_types:
            policy = {
                'name': f'retrieve-{resource_type}',
                'resource': resource_type,
                'description': f'policy to retrieve all {resource_type} resources',
                'mode': {
                    'type': 'collect'
                }
            }
            policy_dicts.append(PolicyDict(**policy))

        policy_loader = PoliciesLoader(
            cloud=cloud,
            regions=regions
        )

        return policy_loader.load_from_policies(policy_dicts)

    def get_aws_policies(
            self, 
            resource_types: Iterable[str]|None = None, 
            regions: Iterable[str]|None = None
    ) -> list[Policy]:
        return self._get_policies(
            resource_map=AWSResourceMap,
            cloud=Cloud.AWS,
            resource_types=resource_types,
            regions=regions,
        )

    def get_azure_policies(
            self, 
            resource_types: Iterable[str]|None = None, 
            regions: Iterable[str]|None = None
    ) -> list[Policy]:
        return self._get_policies(
            resource_map=AzureResourceMap,
            cloud=Cloud.AZURE,
            resource_types=resource_types,
            regions=regions,
        )
    
    def get_gcp_policies(
            self, 
            resource_types: Iterable[str]|None = None, 
            regions: Iterable[str]|None = None
        ) -> list[Policy]:
        return self._get_policies(
            resource_map=GCPResourceMap,
            cloud=Cloud.GCP,
            resource_types=resource_types,
            regions=regions,
        )

    # TODO: better error handling
    def _process_policy(self, policy: Policy, resource_type: str, region: str) -> tuple[list, str, str] | None:
        try:
            return policy(), resource_type, region
        except Exception as e:
            _LOG.error(f"Error processing policy {policy.name}: {e}")
            return None
    
    @staticmethod
    def _compute_hash(data: dict) -> str:
        return sha256(encode(data)).hexdigest()
    
    # TODO: add creds handling for Kube
    def _get_credentials(self, tenant: Tenant) -> dict:
        credentials = get_tenant_credentials(tenant)

        if credentials is None:
            raise ExecutorException(ExecutorError.NO_CREDENTIALS)

        return credentials    
        
    def _build_resource(self, data: dict, resource_type: str, region: str) -> Resource:
        """
        Builds a CloudResource object from the data collected by the policy.
        """
        resource_class = get_resource_class(resource_type)

        if hasattr(resource_class.resource_type, 'id'):
            resource_id = data.get(resource_class.resource_type.id, '')
        elif 'id' in data:
            resource_id = data['id']
        else:
            _LOG.warning(f"Resource {resource_type} does not have an 'id' field in data: {data}")
            resource_id = ''
        
        if hasattr(resource_class.resource_type, 'name'):
            resource_name = data.get(resource_class.resource_type.name, '')
        elif 'name' in data:
            resource_name = data['name']
        else:
            _LOG.warning(f"Resource {resource_type} does not have a 'name' field in data: {data}")
            resource_name = ''
        
        resource = Resource(
            id=resource_id,
            name=resource_name,
            location=region,
            resource_type=resource_type,
            data=data,
            sync_date=datetime.now(timezone.utc).isoformat(),
            hash=self._compute_hash(data)
        )
        return resource

    def collect_resources(
        self,
        tenant_name: str,
        regions: Iterable[str] | None = None,
        resource_types: Iterable[str] | None = None,
        workers: int = 10
    ):
        tenant = self._ms.tenant_service().get(tenant_name)
        if not tenant:
            raise ValueError(f"Tenant {tenant_name} not found")

        cloud = Cloud(tenant.cloud)

        if cloud == Cloud.AWS:
            policies = self.get_aws_policies(
                resource_types=resource_types,
                regions=regions
            )
        elif cloud == Cloud.AZURE:
            policies = self.get_azure_policies(
                resource_types=resource_types,
                regions=regions
            )
        elif cloud == Cloud.GCP:
            policies = self.get_gcp_policies(
                resource_types=resource_types,
                regions=regions
            )
        else:
            raise ValueError(f"Unsupported cloud: {cloud}")
        
        credentials = self._get_credentials(tenant)
        with ThreadPoolExecutor(
            max_workers=workers,
            initializer=job_initializer,
            initargs=(credentials,)
        ) as executor:
            results = []
            for policy in policies:
                result = executor.submit(
                    self._process_policy, 
                    policy, policy.resource_type, 
                    policy.options.region
                )
                results.append(result)

            for result in as_completed(results):
                res, resource_type, region = result.result()
                if res:
                    for data in res:
                        self._build_resource(data, resource_type, region).save()
