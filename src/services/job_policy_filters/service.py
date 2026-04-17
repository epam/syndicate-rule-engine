from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, overload
from typing_extensions import Self

from helpers.constants import Env
from services.reports_bucket import (
    PlatformReportsBucketKeysBuilder,
    TenantReportsBucketKeysBuilder,
    ReportsBucketKeysBuilder,
)
from .builders.k8s import ResourceUidFilterBuilder, K8sQueryBuilder
from .keys import JobPolicyFiltersKeysBuilder
from .types import APPEND_TYPE, BundleFilters, PolicyName, PolicyScanEntry

if TYPE_CHECKING:
    from services.clients.s3 import S3Client
    from services.job_service import Job
    from modular_sdk.models.tenant import Tenant
    from services.platform_service import Platform


@dataclass(frozen=True)
class K8sBuildData:
    namespace: str | None
    name: str | None
    uids: list[str] | str


@dataclass(frozen=True)
class K8sBuildRequest:
    """Per-policy (rule) list of scan inputs (namespace/name scope + UIDs)."""

    policies: dict[PolicyName, list[K8sBuildData]]

    @classmethod
    def one_entry_per_policy(
        cls,
        policy_names: Iterable[PolicyName],
        uids: str | list[str],
        *,
        namespace: str | None = None,
        name: str | None = None,
    ) -> K8sBuildRequest:
        """Same UID set and optional name/namespace scope for every policy name."""
        entry = K8sBuildData(namespace=namespace, name=name, uids=uids)
        return cls(policies={n: [entry] for n in policy_names})


class PolicyFiltersBundleBuilder:
    """
    Builds a bundle of filters.
    """

    def build_k8s_bundle(
        self,
        req: K8sBuildRequest,
    ) -> BundleFilters:
        """
        Build the S3 policy-filters bundle.

        Args:
            req: For each policy, a list of :class:`K8sBuildData` rows — each row is one
                Custodian run (several rows => several runs, e.g. different namespaces).

        Returns:
            Map policy name -> list of scan entries (``query``, ``filters_merge``, ``filters``).
        """
        by_policy: dict[str, list[PolicyScanEntry]] = {}
        for policy_name, entries in req.policies.items():
            scan_entries: list[PolicyScanEntry] = []
            for entry in entries:
                query = K8sQueryBuilder(
                    namespace=entry.namespace,
                    name=entry.name,
                ).build()
                filters = ResourceUidFilterBuilder(entry.uids).build()
                scan_entries.append(
                    PolicyScanEntry(
                        query_merge=APPEND_TYPE,
                        query=query,
                        filters_merge=APPEND_TYPE,
                        filters=filters,
                    )
                )
            by_policy[policy_name] = scan_entries

        return BundleFilters.from_policy_map(by_policy)


class JobPolicyBundleService:
    def __init__(self, s3_client: S3Client) -> None:
        self._s3_client = s3_client

    @classmethod
    def build(cls) -> Self:
        from services import SP

        return cls(s3_client=SP.s3)

    @overload
    def save_bundle(
        self,
        *,
        tenant: Tenant,
        job: Job,
        bundle: BundleFilters,
    ) -> None: ...

    @overload
    def save_bundle(
        self,
        *,
        platform: Platform,
        job: Job,
        bundle: BundleFilters,
    ) -> None: ...

    def save_bundle(
        self,
        *,
        tenant: Tenant | None = None,
        platform: Platform | None = None,
        job: Job,
        bundle: BundleFilters,
    ) -> None:
        builder = self._get_builder(tenant, platform)

        self._s3_client.gz_put_json(
            bucket=Env.REPORTS_BUCKET_NAME.as_str(),
            key=builder.job_filters_bundle(job),
            obj=bundle.to_dict(),
        )

    @overload
    def load_bundle(
        self,
        *,
        tenant: Tenant,
        job: Job,
    ) -> BundleFilters | None: ...

    @overload
    def load_bundle(
        self,
        *,
        platform: Platform,
        job: Job,
    ) -> BundleFilters | None: ...

    def load_bundle(
        self,
        *,
        tenant: Tenant | None = None,
        platform: Platform | None = None,
        job: Job,
    ) -> BundleFilters | None:
        builder = self._get_builder(tenant, platform)
        data = self._s3_client.gz_get_json(
            bucket=Env.REPORTS_BUCKET_NAME.as_str(),
            key=builder.job_filters_bundle(job),
        )
        if not isinstance(data, dict):
            return None
        return BundleFilters.from_dict(data)

    def _get_builder(
        self,
        tenant: Tenant | None = None,
        platform: Platform | None = None,
    ) -> JobPolicyFiltersKeysBuilder:
        if tenant is not None:
            builder = TenantReportsBucketKeysBuilder(tenant)
        elif platform is not None:
            builder = PlatformReportsBucketKeysBuilder(platform)
        else:
            raise ValueError('Either tenant or platform must be provided')
        return JobPolicyFiltersKeysBuilder(builder)
