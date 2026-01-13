"""
Resource collector using Cloud Custodian with subprocess isolation.

Architecture:
1. SUBPROCESS: Runs CC scans, saves results to files (no MongoDB)
2. MAIN PROCESS: Reads files, saves to MongoDB (no fork issues)

This separation avoids PyMongo fork issues since MongoDB connections
are only used in the main process.
"""

from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING, Generator
from typing_extensions import Self
from dataclasses import dataclass

from billiard.pool import ApplyResult, Pool
import msgspec
from c7n import utils
from modular_sdk.models.tenant import Tenant
from modular_sdk.modular import ModularServiceProvider

from helpers.constants import (
    Env,
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
    from modular_sdk.services.tenant_settings_service import TenantSettingsService

_LOG = get_logger(__name__)


@dataclass
class ScanRegionResult:
    region: str
    successful: int
    failed_types: list[str]


def _subprocess_initializer(creds: dict) -> None:
    """Initialize subprocess environment with credentials."""
    from executor.helpers.constants import AWS_DEFAULT_REGION

    pid = os.getpid()
    _LOG.debug(f"Initializing subprocess: pid={pid}")
    os.environ.update({k: str(v) if v else "" for k, v in creds.items()})
    os.environ.setdefault("AWS_DEFAULT_REGION", AWS_DEFAULT_REGION)
    _LOG.debug(f"Initialized subprocess: pid={pid}")


def _scan_region_in_subprocess(
    cloud: Cloud,
    region: str,
    resource_types_tuple: tuple[str, ...] | None,
    work_dir: str,
) -> ScanRegionResult:
    """
    Run CC scan for ONE region in subprocess, save results to files.
    NO MongoDB operations here - only file I/O.

    Args is a tuple: (cloud_value, region, resource_types_tuple, work_dir)
    Returns (region, successful_count, failed_resource_types).
    """
    from c7n.resources.resource_map import ResourceMap as AWSResourceMap
    from c7n_azure.resources.resource_map import ResourceMap as AzureResourceMap
    from c7n_gcp.resources.resource_map import ResourceMap as GCPResourceMap
    from c7n_kube.resources.resource_map import ResourceMap as K8sResourceMap
    from executor.job import PoliciesLoader, PolicyDict, Runner

    work_path = Path(work_dir)
    successful = 0
    failed_types: list[str] = []

    # Get resource types for this cloud
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
            return ScanRegionResult(
                region=region,
                successful=0,
                failed_types=[],
            )

    prefix = PoliciesLoader.cc_provider_name(cloud)
    if resource_types_tuple:
        resource_types = {
            rt if rt.startswith(prefix) else f"{prefix}.{rt}"
            for rt in resource_types_tuple
        }
    else:
        resource_types = scope.copy()

    resource_types -= EXCLUDE_RESOURCE_TYPES

    _LOG.info(f"Scanning region {region} ({len(resource_types)} resource types)")

    # Create policies for each resource type
    policies_dicts: list[PolicyDict] = [
        {"name": f"collect-{rt}", "resource": rt} for rt in resource_types
    ]

    loader = PoliciesLoader(
        cloud=cloud,
        output_dir=work_path,
        regions={region},
        cache=None,
    )

    try:
        policies = loader.load_from_policies(policies_dicts)
    except Exception:
        _LOG.exception(f"Failed to load policies for region {region}")
        return ScanRegionResult(
            region=region,
            successful=0,
            failed_types=list(resource_types),
        )

    _LOG.info(f"Loaded {len(policies)} policies for region {region}")

    # Run policies using Runner (saves to files)
    runner = Runner.factory(cloud, policies)
    runner.start()

    successful = runner.n_successful
    for (reg, policy_name), _ in runner.failed.items():
        # Extract resource type from policy name
        if policy_name.startswith("collect-"):
            failed_types.append(policy_name[8:])

    _LOG.info(f"Completed scan for region {region}: {successful} successful")
    return ScanRegionResult(
        region=region,
        successful=successful,
        failed_types=failed_types,
    )


class ScanResult:
    """Reads scan results from work_dir and yields resources."""

    def __init__(self, work_dir: Path, cloud: Cloud):
        self._work_dir = work_dir
        self._cloud = cloud
        self._res_decoder = msgspec.json.Decoder(type=list[dict])

    def iter_resources(self) -> Generator[tuple[str, str, list[dict]], None, None]:
        """
        Yields (region, resource_type, resources) tuples.
        Skips policies with no resources.
        """
        if not self._work_dir.exists():
            return

        for region_dir in filter(Path.is_dir, self._work_dir.iterdir()):
            region = region_dir.name
            for policy_dir in filter(Path.is_dir, region_dir.iterdir()):
                resources_file = policy_dir / "resources.json"
                if not resources_file.exists():
                    continue

                try:
                    with open(resources_file, "rb") as f:
                        resources = self._res_decoder.decode(f.read())
                except Exception:
                    _LOG.exception(f"Failed to read {resources_file}")
                    continue

                if not resources:
                    continue

                # Extract resource type from policy name (collect-{resource_type})
                policy_name = policy_dir.name
                if policy_name.startswith("collect-"):
                    resource_type = policy_name[8:]
                else:
                    resource_type = policy_name

                yield region, resource_type, resources


class CustodianResourceCollector(BaseResourceCollector):
    """
    Custodian resource collector.
    """

    collector_type = ResourcesCollectorType.CUSTODIAN

    def __init__(
        self,
        modular_service: ModularServiceProvider,
        resources_service: ResourcesService,
        license_service: LicenseService,
        tenant_settings_service: TenantSettingsService,
    ):
        self._ms = modular_service
        self._rs = resources_service
        self._ls = license_service
        self._tss = tenant_settings_service

    @classmethod
    def build(cls) -> Self:
        return cls(
            modular_service=SP.modular_client,
            resources_service=SP.resources_service,
            license_service=SP.license_service,
            tenant_settings_service=SP.modular_client.tenant_settings_service(),
        )

    def _scan_all_regions(
        self,
        cloud: Cloud,
        regions: set[str],
        resource_types: tuple[str, ...] | None,
        credentials: dict,
        work_dir: Path,
    ) -> list[str]:
        """
        Run CC scans for all regions in parallel subprocesses.

        Uses configurable number of workers (SCAN_RESOURCES_PROCESSORS env var).
        Each worker processes one region and exits (maxtasksperchild=1) to free
        Cloud Custodian memory leaks.

        Returns list of failed regions.
        """
        failed_regions: list[str] = []
        sorted_regions = sorted(regions)

        if not sorted_regions:
            return []

        # Prepare tasks for parallel processing
        tasks = [
            (cloud, region, resource_types, str(work_dir)) for region in sorted_regions
        ]

        max_processes = Env.SCAN_RESOURCES_PROCESSORS.as_int()
        processes_count = min(max_processes, len(tasks))

        _LOG.info(
            f"Scanning {len(tasks)} regions with {max_processes} parallel processes"
        )

        try:
            with Pool(  # type: ignore[attr-defined]
                processes=processes_count,
                initializer=_subprocess_initializer,
                initargs=(credentials,),
                maxtasksperchild=1,  # Worker exits after each region to free memory
            ) as pool:
                async_results: list[ApplyResult | None] = [
                    pool.apply_async(
                        _scan_region_in_subprocess,
                        args=task,
                    )
                    for task in tasks
                ]

                for region, ar in zip(regions, async_results):
                    try:
                        if ar:
                            pair: ScanRegionResult = ar.get()

                            if pair.successful == 0:
                                failed_regions.append(region)
                            else:
                                _LOG.info(
                                    f"Region {region}: {pair.successful} policies successful"
                                )
                            if pair.failed_types:
                                _LOG.warning(
                                    f"Failed types in {region}: {pair.failed_types}"
                                )
                                failed_regions.append(region)
                    except Exception:
                        _LOG.exception(f"Error in async result for region {region}")
                        failed_regions.append(region)

        except Exception as e:
            _LOG.error(f"Error in parallel region scanning: {e}")
            # Mark all regions as failed
            failed_regions = sorted_regions

        return failed_regions

    def _save_resources_to_db(
        self,
        tenant: Tenant,
        cloud: Cloud,
        work_dir: Path,
    ) -> int:
        """
        Read scan results from files and save to MongoDB.
        Runs in MAIN process - safe MongoDB operations.
        Returns count of saved resources.
        """
        account_id = str(tenant.project)
        saved_total = 0

        scan_result = ScanResult(work_dir, cloud)
        iterator_strategy = get_resource_iterator(cloud)

        if not iterator_strategy:
            _LOG.warning(f"No resource iterator for cloud {cloud}")
            return 0

        for region, resource_type, resources in scan_result.iter_resources():
            try:
                # Remove old resources
                self._rs.remove_policy_resources(
                    account_id=account_id,
                    location=region,
                    resource_type=resource_type,
                )

                # Create ShardPart for the iterator
                timestamp = time.time()
                part = ShardPart(
                    policy=f"collect-{resource_type}",
                    location=region,
                    timestamp=timestamp,
                    resources=resources,
                )

                # Create and save resource objects
                it = iterator_strategy.iterate(
                    part=part,
                    account_id=account_id,
                    location=region,
                    resource_type=resource_type,
                    customer_name=tenant.customer_name,
                    tenant_name=tenant.name,
                    resources_service=self._rs,
                    collector_type=ResourcesCollectorType.CUSTODIAN,
                )

                for chunk in utils.chunks(it, BATCH_SAVE_CHUNK_SIZE):
                    self._rs.batch_save(chunk)
                    saved_total += len(chunk)

            except Exception:
                _LOG.exception(f"Failed to save {resource_type} in {region}")

        return saved_total

    def _collect_tenant(
        self,
        tenant: Tenant,
        regions: set[str],
        resource_types: set[str] | None,
        credentials: dict,
    ) -> tuple[int, list[str]]:
        """
        Collect resources for one tenant:
        1. Scan all regions (subprocesses)
        2. Save to DB (main process)
        """
        from services.resources import load_cc_providers

        load_cc_providers()

        cloud = modular_helpers.tenant_cloud(tenant)
        resource_types_tuple = tuple(resource_types) if resource_types else None

        with tempfile.TemporaryDirectory() as work_dir:
            work_path = Path(work_dir)

            _LOG.info(f"Phase 1: Scanning {len(regions)} regions")
            failed_regions = self._scan_all_regions(
                cloud=cloud,
                regions=regions,
                resource_types=resource_types_tuple,
                credentials=credentials,
                work_dir=work_path,
            )

            _LOG.info("Phase 2: Saving resources to database")
            saved = self._save_resources_to_db(
                tenant=tenant,
                cloud=cloud,
                work_dir=work_path,
            )

        return saved, failed_regions

    def collect_all_resources(
        self,
        regions: set[str] | None = None,
        resource_types: set[str] | None = None,
    ) -> None:
        """Collect resources for all activated tenants."""
        from executor.job import get_tenant_credentials

        _LOG.info("Starting resource collection for all tenants")

        it = ActivatedTenantsIterator(mc=self._ms, ls=self._ls)
        processed_tenants = 0
        failed_tenants: list[str] = []
        total_resources = 0

        for _, tenant, _ in it:
            tenant_regions = regions
            if tenant_regions is None:
                tenant_regions = modular_helpers.get_tenant_regions(
                    tenant, self._tss
                ) | {GLOBAL_REGION}

            try:
                credentials = get_tenant_credentials(tenant)
                if credentials is None:
                    raise ValueError(f"No credentials for tenant {tenant.name}")
                _LOG.info(
                    f"Processing tenant {tenant.name} ({len(tenant_regions)} regions)"
                )

                saved, failed_regions = self._collect_tenant(
                    tenant=tenant,
                    regions=tenant_regions,
                    resource_types=resource_types,
                    credentials=credentials,
                )

                total_resources += saved
                processed_tenants += 1

                _LOG.info(f"Completed tenant {tenant.name}: {saved} resources")
                if failed_regions:
                    _LOG.warning(f"Failed regions: {failed_regions}")

            except Exception as e:
                _LOG.error(f"Error processing tenant {tenant.name}: {e}")
                failed_tenants.append(tenant.name)

        _LOG.info(
            f"Collection complete: {processed_tenants} tenants, "
            f"{total_resources} resources, {len(failed_tenants)} failed"
        )
