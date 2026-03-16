from abc import ABC, abstractmethod
from datetime import datetime
from typing import Generic, Iterator, Optional, TypedDict, TypeVar
from modular_sdk.models.tenant import Tenant

from helpers import get_logger
from helpers.constants import Cloud, RemediationComplexity
from services.metadata import Metadata, RuleMetadata
from services.resources import iter_rule_region_resources
from services.sharding import ShardPart, ShardsCollection
from ._constants import (
    SOURCE,
    RESOURCE_TYPE_MAPPING,
    MCCResourceType,
    K8SMCCResourceType,
    DEFAULT_DESCRIPTION,
)


_LOG = get_logger(__name__)

T = TypeVar("T", bound=dict)


class RecommendationStats(TypedDict):
    scan_date: Optional[str]
    status: str
    message: str


class Recommendation(TypedDict, total=False):
    article: str
    impact: str
    description: str
    remediation_complexity: RemediationComplexity


class K8SRecommendation(Recommendation):
    resource_id: Optional[str]
    resource_type: str


class RecommendationItem(TypedDict):
    resource_id: str
    resource_type: str
    source: str
    severity: str
    stats: RecommendationStats
    general_actions: list[str]
    recommendation: Recommendation


class K8SRecommendationItem(TypedDict):
    resource_id: str
    resource_type: str
    source: str
    severity: str
    stats: RecommendationStats
    general_actions: list[str]
    recommendation: K8SRecommendation


RecommendationsMapping = dict[str, list[RecommendationItem]]
K8SRecommendationsMapping = dict[str, list[K8SRecommendationItem]]


def build_bucket_key(tenant: Tenant, timestamp: int, region: str) -> str:
    """Build S3 bucket key for recommendations."""
    return (
        f"{tenant.customer_name}/{tenant.cloud}/{tenant.name}/"
        f"{timestamp}/{region}.jsonl"
    )


class BaseRecommendationBuilder(ABC, Generic[T]):
    """Abstract base class for recommendation builders."""

    def __init__(self, collection: ShardsCollection, metadata: Metadata) -> None:
        self._collection = collection
        self._metadata = metadata

    @abstractmethod
    def build(self) -> T:
        """Build recommendations mapping."""

    def _get_description(self, policy: str) -> str:
        return self._collection.meta.get(policy, {}).get(
            "description", DEFAULT_DESCRIPTION
        )

    def _iter_parts_with_metadata(self) -> Iterator[tuple[ShardPart, RuleMetadata]]:
        """Iterate over shard parts that have valid rule metadata."""
        for _, shard in self._collection:
            for part in shard:
                rule_meta = self._metadata.rules.get(part.policy)
                if not rule_meta:
                    _LOG.warning(
                        f"Rule metadata not found for policy: {part.policy}, skipping..."
                    )
                    continue
                yield part, rule_meta


class CloudRecommendationBuilder(BaseRecommendationBuilder[RecommendationsMapping]):
    """Builder for cloud (AWS/GCP/Azure) recommendations."""

    def __init__(
        self,
        collection: ShardsCollection,
        metadata: Metadata,
        cloud: Cloud,
    ) -> None:
        super().__init__(collection, metadata)
        self._cloud = cloud

    def build(self) -> RecommendationsMapping:
        recommendations: RecommendationsMapping = {}

        # Use iter_rule_region_resources to get CloudResource objects with proper IDs
        for policy, location, resources in iter_rule_region_resources(
            collection=self._collection,
            cloud=self._cloud,
            metadata=self._metadata,
        ):
            rule_meta = self._metadata.rules.get(policy)
            if not rule_meta:
                _LOG.warning(
                    f"Rule metadata not found for policy: {policy}, skipping..."
                )
                continue
            cloud = Cloud.parse(rule_meta.cloud)
            if not cloud:
                _LOG.warning(
                    f"Cloud not found for rule metadata: {rule_meta.cloud}, skipping..."
                )
                continue
            description = self._get_description(policy)
            resource_type_mapping = RESOURCE_TYPE_MAPPING.get(cloud, {})
            for resource in resources:
                resource_type = resource_type_mapping.get(
                    resource.resource_type, MCCResourceType.UNKNOWN
                )
                item = RecommendationItem(
                    resource_id=resource.id or "unknown",
                    resource_type=resource_type,
                    source=SOURCE,
                    severity=rule_meta.severity,
                    stats=RecommendationStats(
                        scan_date=datetime.fromtimestamp(
                            resource.sync_date
                        ).isoformat(),
                        status="OK",
                        message="Processed successfully",
                    ),
                    general_actions=[resource_type],
                    recommendation=Recommendation(
                        article=rule_meta.article,
                        impact=rule_meta.impact,
                        description=description,
                        remediation_complexity=rule_meta.remediation_complexity,
                    ),
                )
                recommendations.setdefault(location, []).append(item)

        return recommendations


class K8SRecommendationBuilder(BaseRecommendationBuilder[K8SRecommendationsMapping]):
    """Builder for Kubernetes recommendations."""

    _cloud = Cloud.K8S

    def __init__(
        self,
        collection: ShardsCollection,
        metadata: Metadata,
        application_uuid: str,
        region: Optional[str] = None,
    ) -> None:
        super().__init__(collection, metadata)
        self._application_uuid = application_uuid
        self._region = region

    def build(self) -> K8SRecommendationsMapping:
        recommendations: K8SRecommendationsMapping = {}

        for part, rule_meta in self._iter_parts_with_metadata():
            scan_date = datetime.fromtimestamp(part.timestamp).isoformat()
            location = self._region or part.location
            description = self._get_description(part.policy)
            cloud = Cloud.parse(rule_meta.cloud)
            if not cloud:
                _LOG.warning(
                    f"Cloud not found for rule metadata: {rule_meta.cloud}, skipping..."
                )
                continue
            resource_type_mapping = RESOURCE_TYPE_MAPPING.get(cloud, {})
            for resource in part.resources:
                resource_type = resource_type_mapping.get(
                    resource["resource_type"], K8SMCCResourceType.UNKNOWN
                )
                item = K8SRecommendationItem(
                    resource_id=self._application_uuid,
                    resource_type=MCCResourceType.K8S_CLUSTER,
                    source=SOURCE,
                    severity=rule_meta.severity,
                    stats=RecommendationStats(
                        scan_date=scan_date,
                        status="OK",
                        message="Processed successfully",
                    ),
                    general_actions=[resource_type],
                    recommendation=K8SRecommendation(
                        resource_id=resource.get("id", "unknown"),
                        resource_type=resource_type,
                        article=rule_meta.article,
                        impact=rule_meta.impact,
                        description=description,
                        remediation_complexity=rule_meta.remediation_complexity,
                    ),
                )
                recommendations.setdefault(location, []).append(item)

        return recommendations
