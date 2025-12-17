import os
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import Generator, TYPE_CHECKING

from c7n import utils
from c7n.exceptions import ResourceLimitExceeded
from c7n.policy import Policy, PolicyExecutionMode, execution
from c7n.resources.resource_map import ResourceMap as AWSResourceMap
from c7n.version import version
from c7n_azure.resources.resource_map import ResourceMap as AzureResourceMap
from c7n_gcp.resources.resource_map import ResourceMap as GCPResourceMap
from c7n_kube.resources.resource_map import ResourceMap as K8sResourceMap
from modular_sdk.models.tenant import Tenant
from modular_sdk.modular import ModularServiceProvider

from executor.helpers.constants import AWS_DEFAULT_REGION
from executor.job import (
    PoliciesLoader,
    get_tenant_credentials,
)
from helpers.constants import (
    EXCLUDE_RESOURCE_TYPES,
    Cloud,
    ResourcesCollectorType, GLOBAL_REGION,
)
from helpers.log_helper import get_logger
from models.resource import Resource
from services import SP, modular_helpers
from services.license_service import LicenseService
from services.metadata import EMPTY_RULE_METADATA
from services.reports import ActivatedTenantsIterator
from services.resources import (
    to_aws_resources,
    to_azure_resources,
    to_google_resources,
    to_k8s_resources,
)
from services.resources_service import ResourcesService
from services.sharding import ShardPart

if TYPE_CHECKING:
    from modular_sdk.services.tenant_settings_service import TenantSettingsService

_LOG = get_logger(__name__)


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
        return resources or []


class BaseResourceCollector(ABC):
    """
    Abstract base class for resource collectors.
    All resource collectors should inherit from this class and implement its methods.
    """

    collector_type: ResourcesCollectorType

    @abstractmethod
    def collect_tenant_resources(
        self,
        tenant_name: str,
        regions: set[str] | None = None,
        resource_types: set[str] | None = None,
        **kwargs,
    ): ...

    @abstractmethod
    def collect_all_resources(
        self,
        regions: set[str] | None = None,
        resource_types: set[str] | None = None,
        **kwargs,
    ): ...


class CredentialsContext:
    def __init__(self, creds: dict):
        # Convert Path objects to strings for os.environ compatibility
        self._c = {
            k: str(v) 
            if v is not None else '' 
            for k, v in creds.items()
        }

    def __enter__(self):
        os.environ.update(self._c)
        os.environ.setdefault('AWS_DEFAULT_REGION', AWS_DEFAULT_REGION)

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Clear credentials from environment variables
        for key in self._c:
            os.environ.pop(key, None)
        os.environ.setdefault('AWS_DEFAULT_REGION', AWS_DEFAULT_REGION)


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
        license_service: LicenseService,
        tenant_settings_service: 'TenantSettingsService',
    ):
        self._ms = modular_service
        self._rs = resources_service
        self._ls = license_service
        self._tss = tenant_settings_service

    @classmethod
    def build(cls) -> 'CustodianResourceCollector':
        """
        Builds a ResourceCollector instance.
        """
        return CustodianResourceCollector(
            modular_service=SP.modular_client,
            resources_service=SP.resources_service,
            license_service=SP.license_service,
            tenant_settings_service=
            SP.modular_client.tenant_settings_service(),
        )

    def load_policies(
        self,
        cloud: Cloud,
        resource_types: set[str],
        regions: set[str] | None = None,
    ) -> tuple[Policy, ...]:
        """
        Specify regions including 'global'. Policies for all regions will be
        loaded if not specified.
        Make sure to validate cloud and resource types before calling this method.
        Make sure resource types contain clouds
        """
        if not resource_types:
            return ()

        loader = PoliciesLoader(
            cloud=cloud, output_dir=None, regions=regions, cache=None
        )
        policies = [
            {
                'name': f'collect-{t}',
                'resource': t,
                'mode': {'type': 'collect'},
            }
            for t in resource_types
        ]
        return tuple(loader.load_from_policies(policies))

    @staticmethod
    def _resolve_resource_types(
        cloud: Cloud,
        scope: set[str],
        included: set[str] | None = None,
        excluded: set[str] | None = None,
    ) -> set[str]:
        """
        Scope and excluded must contain resource cloud
        """
        p = PoliciesLoader.cc_provider_name(cloud)
        if included is None:
            base = scope
        else:
            base = set()
            for item in included:
                if not item.startswith(p):
                    item = f'{p}.{item}'
                base.add(item)
        if excluded is not None:
            base.difference_update(excluded)
        if invalid := (scope - base):
            raise ValueError(f'Not supported: {invalid}')
        return base

    def load_aws_policies(
        self,
        resource_types: set[str] | None = None,
        regions: set[str] | None = None,
    ) -> tuple[Policy, ...]:
        return self.load_policies(
            cloud=Cloud.AWS,
            resource_types=self._resolve_resource_types(
                cloud=Cloud.AWS,
                scope=set(AWSResourceMap),
                included=resource_types,
                excluded=EXCLUDE_RESOURCE_TYPES,
            ),
            regions=regions,
        )

    def load_azure_policies(
        self, resource_types: set[str] | None = None, **kwargs
    ) -> tuple[Policy, ...]:
        """
        Loads Azure policies for all resource types.
        """

        # regions are not specified because azure supports only global scan
        #  and there is no sense in scanning nothing
        return self.load_policies(
            cloud=Cloud.AZURE,
            resource_types=self._resolve_resource_types(
                cloud=Cloud.AZURE,
                scope=set(AzureResourceMap),
                included=resource_types,
                excluded=EXCLUDE_RESOURCE_TYPES,
            ),
        )

    def load_google_policies(
        self, resource_types: set[str] | None = None, **kwargs
    ) -> tuple[Policy, ...]:
        return self.load_policies(
            cloud=Cloud.GOOGLE,
            resource_types=self._resolve_resource_types(
                cloud=Cloud.GOOGLE,
                scope=set(GCPResourceMap),
                included=resource_types,
                excluded=EXCLUDE_RESOURCE_TYPES,
            ),
        )

    def load_k8s_policies(
        self, resource_types: set[str] | None = None, **kwargs
    ) -> tuple[Policy, ...]:
        return self.load_policies(
            cloud=Cloud.KUBERNETES,
            resource_types=self._resolve_resource_types(
                cloud=Cloud.KUBERNETES,
                scope=set(K8sResourceMap),
                included=resource_types,
                excluded=EXCLUDE_RESOURCE_TYPES,
            ),
        )

    @staticmethod
    def _invoke_policy(policy: Policy) -> list[dict] | None:
        try:
            return policy()
        except Exception:
            _LOG.exception('Error invoking policy')
            return None

    def iter_aws_resources(
        self,
        part: ShardPart,
        account_id: str,
        location: str,
        resource_type: str,
        customer_name: str,
        tenant_name: str,
    ) -> Generator[Resource, None, None]:
        for res in to_aws_resources(
            part, resource_type, EMPTY_RULE_METADATA, account_id
        ):
            yield self._rs.create(
                account_id=account_id,
                location=location,  # important, not resolved region but policy location
                resource_type=resource_type,
                id=res.id,
                name=res.name,
                arn=res.arn,
                data=res.data,
                sync_date=part.timestamp,
                collector_type=self.collector_type,
                tenant_name=tenant_name,
                customer_name=customer_name,
            )

    def iter_azure_resources(
        self,
        part: ShardPart,
        account_id: str,
        location: str,
        resource_type: str,
        customer_name: str,
        tenant_name: str,
    ) -> Generator[Resource, None, None]:
        for res in to_azure_resources(part, resource_type):
            yield self._rs.create(
                account_id=account_id,
                location=location,
                resource_type=resource_type,
                id=res.id,
                name=res.name,
                arn=res.id,
                data=res.data,
                sync_date=part.timestamp,
                collector_type=self.collector_type,
                tenant_name=tenant_name,
                customer_name=customer_name,
            )

    def iter_google_resources(
        self,
        part: ShardPart,
        account_id: str,
        location: str,
        resource_type: str,
        customer_name: str,
        tenant_name: str,
    ) -> Generator[Resource, None, None]:
        for res in to_google_resources(
            part, resource_type, EMPTY_RULE_METADATA, account_id
        ):
            yield self._rs.create(
                account_id=account_id,
                location=location,
                resource_type=resource_type,
                id=res.id,
                name=res.name,
                arn=res.urn,
                data=res.data,
                sync_date=part.timestamp,
                collector_type=self.collector_type,
                tenant_name=tenant_name,
                customer_name=customer_name,
            )

    def iter_k8s_resources(
        self,
        part: ShardPart,
        account_id: str,
        location: str,
        resource_type: str,
        customer_name: str,
        tenant_name: str,
    ) -> Generator[Resource, None, None]:
        for res in to_k8s_resources(part, resource_type):
            yield self._rs.create(
                account_id=account_id,
                location=location,
                resource_type=resource_type,
                id=res.id,
                name=res.name,
                arn=res.id,
                data=res.data,
                sync_date=part.timestamp,
                collector_type=self.collector_type,
                tenant_name=tenant_name,
                customer_name=customer_name,
            )

    def _process_policy(
        self,
        policy: Policy,
        cloud: Cloud,
        customer_name: str,
        tenant_name: str,
        account_id: str,
    ) -> None:
        """
        Invokes one policy and saves its result to the database
        """
        resources = self._invoke_policy(policy)
        if resources is None:
            # error
            return
        location = PoliciesLoader.get_policy_region(policy)
        rt = policy.resource_type
        # THIS is the location that is stored in the database. It does not
        # necessarily match the region of the resource

        # NOTE: here the returns resources are the actual ones while
        # those in DB are obsolete. We should basically remove all
        # resources return by such policy from DN and save all these new.
        # Second option: we can iterate over those from DB and process
        # one by one, check if one was removed, if not update it, them save
        # all that left. Not sure what will be more efficient, but the first
        # one is definitely easier to implement.
        self._rs.remove_policy_resources(
            account_id=account_id, location=location, resource_type=rt
        )
        part = ShardPart(
            policy=policy.name,  # does not matter here
            location=location,
            timestamp=time.time(),
            resources=resources,
        )
        match cloud:
            case Cloud.AWS:
                it = self.iter_aws_resources(
                    part, account_id, location, rt, customer_name, tenant_name
                )
            case Cloud.AZURE:
                it = self.iter_azure_resources(
                    part, account_id, location, rt, customer_name, tenant_name
                )
            case Cloud.GOOGLE | Cloud.GCP:
                it = self.iter_google_resources(
                    part, account_id, location, rt, customer_name, tenant_name
                )
            case Cloud.KUBERNETES | Cloud.K8S:
                it = self.iter_k8s_resources(
                    part, account_id, location, rt, customer_name, tenant_name
                )
        self._rs.batch_save(it)

    # TODO: add creds handling for Kube
    @staticmethod
    def _get_credentials(tenant: Tenant) -> dict:
        credentials = get_tenant_credentials(tenant)

        if credentials is None:
            raise ValueError(f'No credentials found for tenant {tenant.name}')
        return credentials

    def collect_tenant_resources(
        self,
        tenant_name: str,
        regions: set[str] | None = None,
        resource_types: set[str] | None = None,
        workers: int | None = None,
        **kwargs,
    ):
        tenant = self._ms.tenant_service().get(tenant_name)
        if not tenant:
            raise ValueError(f'Tenant {tenant_name} not found')
        if regions is None:
            regions = modular_helpers.get_tenant_regions(
                tenant,
                self._tss,
            ) | {GLOBAL_REGION}

        cloud = modular_helpers.tenant_cloud(tenant)
        credentials = CustodianResourceCollector._get_credentials(tenant)
        with CredentialsContext(credentials):
            match cloud:
                case Cloud.AWS:
                    policies = self.load_aws_policies(
                        resource_types=resource_types,
                        regions=regions
                    )
                case Cloud.AZURE:
                    policies = self.load_azure_policies(
                        resource_types=resource_types
                    )
                case Cloud.GCP | Cloud.GOOGLE:
                    policies = self.load_google_policies(
                        resource_types=resource_types
                    )
                case Cloud.KUBERNETES | Cloud.K8S:
                    policies = self.load_k8s_policies(
                        resource_types=resource_types
                    )
                case _:
                    raise ValueError(f'Unsupported cloud {cloud}')

            _LOG.info(f'Starting resource collection for tenant: {tenant_name}')
            _LOG.info(f'Policies to run: {len(policies)}')

            # TODO: check process pool executor vs Billiard pool
            with ThreadPoolExecutor(
                max_workers=workers,
            ) as executor:
                for policy in policies:
                    executor.submit(
                        self._process_policy,
                        policy,
                        cloud,
                        tenant.customer_name,
                        tenant.name,
                        str(tenant.project)
                    )

    def collect_all_resources(
        self,
        regions: set[str] | None = None,
        resource_types: set[str] | None = None,
        workers: int | None = None,
        **kwargs,
    ):
        """
        Collects resources for all tenants. If regions are not specified
        all active regions will be collected
        """
        it = ActivatedTenantsIterator(mc=self._ms, ls=self._ls)
        for _, tenant, _ in it:
            if regions is None:
                tenant_regions = modular_helpers.get_tenant_regions(
                    tenant,
                    self._tss,
                ) | {GLOBAL_REGION}
            else:
                tenant_regions = regions
            try:
                _LOG.info(f'Collecting resources for tenant: {tenant.name}')
                self.collect_tenant_resources(
                    tenant_name=tenant.name,
                    regions=tenant_regions,
                    resource_types=resource_types,
                    workers=workers,
                )
            except Exception as e:
                _LOG.error(
                    f'Error collecting resources for tenant {tenant.name}: {e}'
                )
