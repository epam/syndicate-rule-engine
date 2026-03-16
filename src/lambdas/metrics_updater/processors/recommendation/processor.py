import io
import json
from datetime import datetime, timezone
from typing import MutableMapping, Optional, Sequence

from modular_sdk.commons.constants import ParentType
from modular_sdk.models.tenant import Tenant
from modular_sdk.modular import ModularServiceProvider

from helpers import RequestContext, get_logger
from helpers.constants import (
    Cloud,
)
from helpers.time_helper import as_milliseconds
from lambdas.metrics_updater.processors.base import (
    BaseProcessor,
    NextLambdaEvent,
)
from models.metrics import ReportMetrics
from services import SP
from services.cadf_event_sender import CadfAttachment, CadfEventSender
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService
from services.license_service import LicenseService
from services.metadata import Metadata, MetadataProvider
from services.platform_service import Platform, PlatformService
from services.report_service import ReportService
from services.reports_bucket import (
    PlatformReportsBucketKeysBuilder,
    ReportsBucketKeysBuilder,
    TenantReportsBucketKeysBuilder,
)

from ._builder import (
    CloudRecommendationBuilder,
    K8SRecommendationBuilder,
    K8SRecommendationItem,
    K8SRecommendationsMapping,
    RecommendationItem,
    RecommendationsMapping,
    build_bucket_key,
)


_LOG = get_logger(__name__)


class RecommendationProcessor(BaseProcessor):
    """
    Processor for collecting recommendations
    """

    processor_name = "recommendations"
    _cloud_recommendation_builder_cls: type[CloudRecommendationBuilder] = (
        CloudRecommendationBuilder
    )
    _k8s_recommendation_builder_cls: type[K8SRecommendationBuilder] = (
        K8SRecommendationBuilder
    )

    def __init__(
        self,
        environment_service: EnvironmentService,
        s3: S3Client,
        assume_role_s3: S3Client,
        modular_client: ModularServiceProvider,
        cadf_event_sender: CadfEventSender,
        license_service: LicenseService,
        metadata_provider: MetadataProvider,
        report_service: ReportService,
        platform_service: PlatformService,
    ) -> None:
        self._environment_service = environment_service
        self._s3 = s3
        self._assume_role_s3 = assume_role_s3
        self._modular_client = modular_client
        self._cadf_event_sender = cadf_event_sender
        self._license_service = license_service
        self._metadata_provider = metadata_provider
        self._report_service = report_service
        self._platform_service = platform_service

        self._reports_bucket = self._environment_service.default_reports_bucket_name()
        self._recommendations_bucket = (
            self._environment_service.get_recommendation_bucket()
        )

    def __call__(
        self,
        event: Optional[MutableMapping] = None,
        context: Optional[RequestContext] = None,
    ) -> Optional[NextLambdaEvent]:
        self._process_data()

    @classmethod
    def build(cls) -> "RecommendationProcessor":
        return RecommendationProcessor(
            environment_service=SP.environment_service,
            s3=SP.s3,
            assume_role_s3=SP.assume_role_s3,  # For saving recommendations in another account or environment
            modular_client=SP.modular_client,
            cadf_event_sender=SP.cadf_event_sender,
            license_service=SP.license_service,
            metadata_provider=SP.metadata_provider,
            report_service=SP.report_service,
            platform_service=SP.platform_service,
        )

    @staticmethod
    def _json_to_jsonl(
        recommendations: Sequence[RecommendationItem | K8SRecommendationItem],
    ) -> str:
        buffer = io.StringIO()
        for item in recommendations:
            buffer.write(json.dumps(item, separators=(",", ":")))
            buffer.write("\n")
        content = buffer.getvalue()
        buffer.close()
        return content

    def _process_data(self) -> None:
        now = datetime.now(timezone.utc)
        timestamp = as_milliseconds(now.timestamp())
        cluster_platform_mapping = self._get_cluster_parents()
        tenant_service = self._modular_client.tenant_service()
        tenant_obj_mapping: dict[str, Tenant] = {}

        _LOG.info(
            f"Starting platform-level processing. "
            f"Found {len(cluster_platform_mapping)} K8s platforms"
        )

        for _, platform in cluster_platform_mapping.items():
            _LOG.info(f"Processing K8s platform: {platform.name}")
            tenant = tenant_service.get(platform.tenant_name)
            if not tenant or not tenant.project:
                _LOG.warning(
                    f"Skipping platform {platform.name}: "
                    f"no tenant or project found for tenant_name={platform.tenant_name}"
                )
                continue

            _LOG.debug(
                f"Platform {platform.name} -> tenant: {tenant.name}, "
                f"project: {tenant.project}, customer: {tenant.customer_name}"
            )

            metadata = self._license_service.get_customer_metadata(tenant.customer_name)
            cloud = Cloud.parse(tenant.cloud)
            if not cloud:
                _LOG.warning(
                    f"Skipping platform {platform.name}: "
                    f"cannot parse cloud {tenant.cloud!r} for tenant {tenant.name}"
                )
                continue
            tenant_obj_mapping[tenant.project] = tenant

            key_builder = PlatformReportsBucketKeysBuilder(platform)
            file = key_builder.latest_key()
            _LOG.debug(f"Get file {file} content")
            k8s_recommendations = self._get_platform_k8s_recommendations(
                platform=platform,
                metadata=metadata,
            )
            if not k8s_recommendations:
                _LOG.debug(f"No k8s recommendations based on findings {file}")

            tenant_key_builder = TenantReportsBucketKeysBuilder(tenant)
            latest_key = tenant_key_builder.latest_key()
            _LOG.debug(f"Get file {latest_key} content")
            recommendations = self._get_tenant_recommendations(
                tenant=tenant,
            )

            if not recommendations:
                _LOG.info(
                    f"Platform {platform.name}: no cloud recommendations, "
                    f"saving K8s-only recommendations for tenant {tenant.name}"
                )
                for region, recommend in k8s_recommendations.items():
                    content = self._json_to_jsonl(recommend)
                    self._save_recommendation(
                        region=region,
                        tenant=tenant,
                        content=content,
                        timestamp=timestamp,
                    )
                    _LOG.debug(
                        f"Saved {len(recommend)} K8s recommendations "
                        f"for tenant {tenant.name}, region {region}"
                    )
                self._send_event_to_maestro(
                    tenant=tenant,
                    timestamp=timestamp,
                    now=now,
                )
            else:
                _LOG.info(
                    f"Platform {platform.name}: merging K8s and cloud recommendations "
                    f"for tenant {tenant.name}"
                )
                all_regions = set(recommendations.keys()) | set(k8s_recommendations.keys())
                for region in all_regions:
                    cloud_recs = recommendations.get(region, [])
                    k8s_recs = k8s_recommendations.get(region, [])
                    merged = cloud_recs + k8s_recs
                    content = self._json_to_jsonl(merged)
                    self._save_recommendation(
                        region=region,
                        tenant=tenant,
                        content=content,
                        timestamp=timestamp,
                    )
                    _LOG.debug(
                        f"Saved {len(merged)} recommendations (cloud: {len(cloud_recs)}, "
                        f"k8s: {len(k8s_recs)}) for tenant {tenant.name}, region {region}"
                    )
                self._send_event_to_maestro(
                    tenant=tenant,
                    timestamp=timestamp,
                    now=now,
                )

        _LOG.info(
            f"Platform-level processing complete. "
            f"Processed {len(tenant_obj_mapping)} tenants"
        )

        # get tenant recommendations
        _LOG.info("Starting tenant-level processing")
        prefixes = list(
            self._s3.common_prefixes(
                bucket=self._reports_bucket,
                delimiter=ReportsBucketKeysBuilder.latest,
                prefix=ReportsBucketKeysBuilder.prefix,
            )
        )
        _LOG.debug(f"Found {len(prefixes)} prefixes in reports bucket")
        for prefix in prefixes:
            if Cloud.KUBERNETES in prefix:
                _LOG.debug(
                    f"Skipping K8s prefix {prefix}: already processed on platform level"
                )
                continue

            _LOG.debug(f"Processing prefix: {prefix}")
            objects: list[ReportMetrics] = [
                o
                for o in self._s3.list_objects(
                    bucket=self._reports_bucket, prefix=prefix
                )
                if o.key.endswith("json.gz") or o.key.endswith("json")
            ]

            for obj in objects:
                project_id = obj.key.split("/")[3]
                tenant = tenant_obj_mapping.get(project_id)
                if tenant:
                    _LOG.debug(
                        f"Skipping project {project_id}: "
                        f"tenant {tenant.name} already processed on platform level"
                    )
                    continue

                tenant: Tenant | None = next(
                    self._modular_client.tenant_service().i_get_by_acc(
                        project_id, active=True
                    ),
                    None,
                )
                if not tenant:
                    _LOG.warning(
                        f"Cannot find active tenant for project_id {project_id}, "
                        f"key: {obj.key}"
                    )
                    continue
                tenant_obj_mapping[project_id] = tenant
                tenant_name: str = tenant.name

                _LOG.info(f"Processing tenant {tenant_name} (project: {project_id})")
                recommendations = self._get_tenant_recommendations(
                    tenant=tenant,
                )
                # path to store /customer/cloud/tenant/timestamp/region.jsonl
                for region, recommend in recommendations.items():
                    content = self._json_to_jsonl(recommend)
                    self._save_recommendation(
                        region=region,
                        tenant=tenant,
                        content=content,
                        timestamp=timestamp,
                    )
                    _LOG.debug(
                        f"Saved {len(recommend)} recommendations "
                        f"for tenant {tenant_name}, region {region}"
                    )
                if not recommendations:
                    _LOG.debug(f"No recommendations based on findings {obj.key}")
                    continue

                self._send_event_to_maestro(
                    tenant=tenant,
                    timestamp=timestamp,
                    now=now,
                )

        _LOG.info("Tenant-level processing complete")

    def _send_event_to_maestro(
        self,
        tenant: Tenant,
        timestamp: int,
        now: datetime,
    ) -> int | None:
        """Send recommendation collection event for the tenant."""
        attachment = CadfAttachment(
            contentType="map",
            content={"tenant": tenant.name, "timestamp": timestamp},
            name="collectSyndicateRuleEngineRecommendations",
        )
        return self._cadf_event_sender.send_event(
            tenant=tenant,
            attachments=[attachment],
            event_time=now,
        )

    def _get_cluster_parents(self) -> dict[str, Platform]:
        """Get all parents and applications related to k8s clusters"""
        cluster_platform_mapping = {}
        for cust in self._modular_client.customer_service().i_get_customer():
            parents = self._modular_client.parent_service().i_get_parent_by_customer(
                customer_id=cust.name,
                # TODO: Need additional investigation why parent_type is not working via string/enum value
                # For now using list of parent_type to filter parents by type and is_deleted
                parent_type=[ParentType.PLATFORM_K8S],
                is_deleted=False,
            )
            for parent in parents:
                _LOG.debug(f"Found parent {parent.parent_id} for customer {cust.name}")
                platform = Platform(parent)
                self._platform_service.fetch_application(platform)
                cluster_platform_mapping[platform.name] = platform
        return cluster_platform_mapping

    def _get_platform_cloud_recommendations(
        self,
        platform: Platform,
        metadata: Metadata,
        cloud: Cloud,
    ) -> RecommendationsMapping:
        collection = self._report_service.platform_latest_collection(platform)
        collection.fetch_all()
        collection.fetch_meta()
        builder = self._cloud_recommendation_builder_cls(
            collection=collection,
            metadata=metadata,
            cloud=cloud,
        )
        return builder.build()

    def _get_platform_k8s_recommendations(
        self,
        platform: Platform,
        metadata: Metadata,
    ) -> K8SRecommendationsMapping:
        collection = self._report_service.platform_latest_collection(platform)
        collection.fetch_all()
        collection.fetch_meta()
        builder = self._k8s_recommendation_builder_cls(
            collection=collection,
            metadata=metadata,
            application_uuid=platform.id,
            region=platform.region,
        )
        return builder.build()

    def _get_tenant_recommendations(self, tenant: Tenant) -> RecommendationsMapping:
        collection = self._report_service.tenant_latest_collection(tenant)
        metadata = self._license_service.get_customer_metadata(tenant.customer_name)
        collection.fetch_all()
        collection.fetch_meta()

        cloud = Cloud.parse(tenant.cloud)
        if not cloud:
            _LOG.warning(f"Cannot find cloud for tenant {tenant.name}")
            return {}

        builder = self._cloud_recommendation_builder_cls(
            collection=collection,
            metadata=metadata,
            cloud=cloud,
        )
        return builder.build()

    def _save_recommendation(
        self,
        region: str,
        tenant: Tenant,
        content: str,
        timestamp: int,
    ) -> None:
        file_path = build_bucket_key(tenant, timestamp, region)
        _LOG.debug(f"Saving file {file_path}, region {region}")
        self._assume_role_s3.put_object(
            bucket=self._recommendations_bucket,
            key=file_path,
            body=content.encode(),
        )
