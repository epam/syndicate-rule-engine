import os
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

import billiard as multiprocessing
from c7n import utils
from c7n.exceptions import ResourceLimitExceeded
from c7n.policy import PolicyExecutionMode, execution
from c7n.resources.resource_map import ResourceMap as AWSResourceMap
from c7n.version import version
from c7n_azure.resources.resource_map import ResourceMap as AzureResourceMap
from c7n_gcp.resources.resource_map import ResourceMap as GCPResourceMap
from c7n_kube.resources.resource_map import ResourceMap as K8sResourceMap
from modular_sdk.models.tenant import Tenant
from modular_sdk.modular import ModularServiceProvider

from executor.job import PoliciesLoader, get_tenant_credentials
from helpers.constants import (
    EXCLUDE_RESOURCE_TYPES,
    GLOBAL_REGION,
    Cloud,
    ResourcesCollectorType,
)
from helpers.log_helper import get_logger
from services import SP, modular_helpers
from services.license_service import LicenseService
from services.reports import ActivatedTenantsIterator
from services.resources_service import ResourcesService
from services.sharding import ShardPart

from .base import BaseResourceCollector
from .constants import BATCH_SAVE_CHUNK_SIZE
from .strategies import get_resource_iterator


if TYPE_CHECKING:
    from modular_sdk.services.tenant_settings_service import (
        TenantSettingsService,
    )

_LOG = get_logger(__name__)


# CC by default stores retrieved information on disk
# This mode turns off this behavior
# Though it stops storing scan's metadata
@execution.register("collect")
class CollectMode(PolicyExecutionMode):
    """
    Queries resources from cloud provider for filtering and actions.
    Does not store them on disk.
    """

    schema = utils.type_schema("pull")

    def run(self, *args, **kw):
        if not self.policy.is_runnable():
            return []

        self.policy.log.debug(
            "Running policy:%s resource:%s region:%s c7n:%s",
            self.policy.name,
            self.policy.resource_type,
            self.policy.options.region or "default",
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
        return resources or []


@dataclass(frozen=True, slots=True)
class TenantTask:
    """Serializable task data for subprocess."""

    tenant_name: str
    account_id: str
    customer_name: str
    cloud: str
    regions: tuple[str, ...]
    resource_types: tuple[str, ...] | None
    credentials: dict


def _subprocess_initializer(creds: dict) -> None:
    """Initialize subprocess environment with credentials."""
    from executor.helpers.constants import AWS_DEFAULT_REGION

    os.environ.update({k: str(v) if v else "" for k, v in creds.items()})
    os.environ.setdefault("AWS_DEFAULT_REGION", AWS_DEFAULT_REGION)


def _collect_in_subprocess(task: TenantTask) -> tuple[int, list[str]]:
    """
    Run resource collection in subprocess.
    Returns (saved_count, failed_policies).
    All memory is freed when this process exits.
    """
    from services import SP

    cloud = Cloud(task.cloud)
    rs = SP.resources_service
    saved_total = 0
    failed_policies: list[str] = []

    resource_types = _get_resource_types(
        cloud=cloud,
        included=task.resource_types,
    )

    for rt in resource_types:
        try:
            saved = _process_single_resource_type(
                cloud=cloud,
                resource_type=rt,
                regions=set(task.regions),
                account_id=task.account_id,
                customer_name=task.customer_name,
                tenant_name=task.tenant_name,
                rs=rs,
            )
            saved_total += saved
        except Exception as e:
            _LOG.error(f"Failed {rt}: {e}")
            failed_policies.append(rt)

    return saved_total, failed_policies


def _resolve_resource_types(
    cloud: Cloud,
    scope: set[str],
    included: set[str] | None = None,
    excluded: set[str] | None = None,
) -> set[str]:
    """Resolve resource types for a cloud provider."""
    p = PoliciesLoader.cc_provider_name(cloud)
    if included is None:
        base = scope.copy()
    else:
        base = set()
        for item in included:
            if not item.startswith(p):
                item = f"{p}.{item}"
            base.add(item)
    if excluded is not None:
        base.difference_update(excluded)
    return base


def _get_resource_types(cloud: Cloud, included: tuple[str, ...] | None) -> set[str]:
    """Get valid resource types for cloud."""
    match cloud:
        case Cloud.AWS:
            scope = set(AWSResourceMap)
        case Cloud.AZURE:
            scope = set(AzureResourceMap)
        case Cloud.GOOGLE | Cloud.GCP:
            scope = set(GCPResourceMap)
        case Cloud.KUBERNETES | Cloud.K8S:
            scope = set(K8sResourceMap)
        case _:
            return set()

    return _resolve_resource_types(
        cloud=cloud,
        scope=scope,
        included=set(included) if included else None,
        excluded=EXCLUDE_RESOURCE_TYPES,
    )


def _process_single_resource_type(
    cloud: Cloud,
    resource_type: str,
    regions: set[str],
    account_id: str,
    customer_name: str,
    tenant_name: str,
    rs: ResourcesService,
) -> int:
    """Process one resource type across regions. Returns saved count."""
    loader = PoliciesLoader(
        cloud=cloud,
        output_dir=None,
        regions=regions if cloud == Cloud.AWS else None,
        cache=None,
    )

    policy_dict: dict = {
        "name": f"collect-{resource_type}",
        "resource": resource_type,
        "mode": {"type": "collect"},
    }

    policies = loader.load_from_policies([policy_dict])  # type: ignore[arg-type]
    saved_count = 0

    for policy in policies:
        try:
            resources = policy()
            if resources is None:
                continue

            location = PoliciesLoader.get_policy_region(policy)
            rt = policy.resource_type

            rs.remove_policy_resources(
                account_id=account_id,
                location=location,
                resource_type=rt,
            )

            if not resources:
                continue

            timestamp = time.time()
            part = ShardPart(
                policy=policy.name,
                location=location,
                timestamp=timestamp,
                resources=resources,
            )

            iterator_strategy = get_resource_iterator(cloud)
            if not iterator_strategy:
                continue

            it = iterator_strategy.iterate(
                part=part,
                account_id=account_id,
                location=location,
                resource_type=rt,
                customer_name=customer_name,
                tenant_name=tenant_name,
                resources_service=rs,
                collector_type=ResourcesCollectorType.CUSTODIAN,
            )

            for chunk in utils.chunks(it, BATCH_SAVE_CHUNK_SIZE):
                rs.batch_save(chunk)
                saved_count += len(chunk)

        except Exception:
            _LOG.exception(f"Error in policy {policy.name}")

    return saved_count


# TODO: add resource retrieval for Kubernetes
class CustodianResourceCollector(BaseResourceCollector):
    """
    Resource collector with process isolation using multiprocessing.

    Uses same pattern as job.py to prevent Cloud Custodian memory leaks.
    Each tenant runs in separate subprocess - when it exits, all memory is freed.

    Key optimizations:
    1. Process isolation per tenant - prevents Cloud Custodian memory leaks
    2. Streaming policy processing - one resource type at a time
    3. Aggressive garbage collection
    4. multiprocessing.Pool compatible with Celery daemon processes
    """

    collector_type = ResourcesCollectorType.CUSTODIAN

    def __init__(
        self,
        modular_service: ModularServiceProvider,
        resources_service: ResourcesService,
        license_service: LicenseService,
        tenant_settings_service: "TenantSettingsService",
    ):
        self._ms = modular_service
        self._rs = resources_service
        self._ls = license_service
        self._tss = tenant_settings_service

    @classmethod
    def build(cls) -> "CustodianResourceCollector":
        """Builds a ResourceCollector instance."""
        return CustodianResourceCollector(
            modular_service=SP.modular_client,
            resources_service=SP.resources_service,
            license_service=SP.license_service,
            tenant_settings_service=SP.modular_client.tenant_settings_service(),
        )

    def collect_all_resources(
        self,
        regions: set[str] | None = None,
        resource_types: set[str] | None = None,
    ) -> None:
        """
        Collect resources for all activated tenants.

        Each tenant runs in separate subprocess using multiprocessing.Pool.
        Process isolation ensures memory doesn't accumulate across tenants.
        When each subprocess exits, all Cloud Custodian memory leaks are freed.
        """
        _LOG.info("Starting resource collection for all tenants")

        it = ActivatedTenantsIterator(mc=self._ms, ls=self._ls)
        processed = 0
        failed_tenants: list[str] = []
        total_resources = 0

        for _, tenant, _ in it:
            tenant_regions = regions
            if tenant_regions is None:
                tenant_regions = modular_helpers.get_tenant_regions(
                    tenant, self._tss
                ) | {GLOBAL_REGION}

            try:
                task = self._create_task(
                    tenant=tenant,
                    regions=tenant_regions,
                    resource_types=resource_types,
                )

                _LOG.info(f"Processing tenant: {tenant.name}")

                # Each tenant in separate subprocess
                # Memory is freed when subprocess exits
                with multiprocessing.Pool(  # type: ignore[attr-defined]
                    processes=1,
                    initializer=_subprocess_initializer,
                    initargs=(task.credentials,),
                ) as pool:
                    saved, failed = pool.apply(_collect_in_subprocess, (task,))

                total_resources += saved
                processed += 1

                _LOG.info(f"Completed {tenant.name}: {saved} resources")
                if failed:
                    _LOG.warning(f"Failed policies for {tenant.name}: {len(failed)}")

            except Exception as e:
                _LOG.error(f"Error processing {tenant.name}: {e}")
                failed_tenants.append(tenant.name)

        _LOG.info(
            f"Collection complete: {processed} tenants, "
            f"{total_resources} resources, {len(failed_tenants)} failed"
        )

        if failed_tenants:
            _LOG.warning(f"Failed tenants: {failed_tenants}")

    def _create_task(
        self,
        tenant: Tenant,
        regions: set[str] | None,
        resource_types: set[str] | None,
    ) -> TenantTask:
        """Create serializable task for subprocess."""
        if regions is None:
            regions = modular_helpers.get_tenant_regions(tenant, self._tss) | {
                GLOBAL_REGION
            }

        cloud = modular_helpers.tenant_cloud(tenant)
        credentials = get_tenant_credentials(tenant)
        if credentials is None:
            raise ValueError(f"No credentials found for tenant {tenant.name}")

        return TenantTask(
            tenant_name=tenant.name,
            account_id=str(tenant.project),
            customer_name=tenant.customer_name,
            cloud=cloud.value,
            regions=tuple(regions),
            resource_types=tuple(resource_types) if resource_types else None,
            credentials=credentials,
        )
