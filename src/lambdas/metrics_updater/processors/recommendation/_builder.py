from abc import ABC, abstractmethod
from copy import deepcopy
from datetime import datetime
from typing import Any, Generic, Iterator, Optional, TypedDict, TypeVar, cast

from modular_sdk.models.tenant import Tenant

from helpers import get_logger
from helpers.constants import Cloud, ARTICLE_ATTR, IMPACT_ATTR, RESOURCE_TYPE_ATTR
from services.metadata import Metadata, RuleMetadata
from services.resources import iter_rule_region_resources, service_to_resource_type
from services.sharding import ShardPart, ShardsCollection
from ._constants import RULE_RECOMMENDATION_MAPPING, SOURCE, K8S_RECOMMENDATION_MODEL


_LOG = get_logger(__name__)

T = TypeVar("T", bound=dict)


class RecommendationStats(TypedDict):
    scan_date: Optional[str]
    status: str
    message: str


class BaseRecommendation(TypedDict, total=False):
    article: str
    impact: str
    description: str
    protocol: str  # from old solution
    action: str  # from old solution
    port: list[str]  # from old solution


class K8SRecommendation(BaseRecommendation):
    resource_id: Optional[str]
    resource_type: str


class RecommendationItem(TypedDict):
    resource_id: str
    resource_type: str
    source: str
    severity: str
    stats: RecommendationStats
    meta: Optional[dict[str, Any]]
    general_actions: list[str]
    recommendation: BaseRecommendation


class K8SRecommendationItem(TypedDict):
    resource_id: str
    resource_type: str
    source: str
    severity: str
    stats: RecommendationStats
    meta: Optional[dict[str, Any]]
    general_actions: list[str]
    recommendation: K8SRecommendation


RecommendationsMapping = dict[str, list[RecommendationItem]]
K8SRecommendationsMapping = dict[str, list[K8SRecommendationItem]]


K8S_RESOURCE_TO_ACTION: dict[str, str] = {
    "ConfigMap": "CONFIG",
    "ClusterRole": "ROLE",
    "Role": "ROLE",
    "Deployment": "DEPLOYMENT",
    "Secret": "SECRET",
    "ServiceAccount": "SERVICE_ACCOUNT",
    "Namespace": "NAMESPACE",
}


DEFAULT_K8S_ACTION = "POD"
DEFAULT_DESCRIPTION = "Description"


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

            resource_type = service_to_resource_type(rule_meta.service, self._cloud)
            description = self._get_description(policy)
            for resource in resources:
                item = RecommendationItem(
                    resource_id=resource.id or "unknown",
                    resource_type=resource_type,
                    source=SOURCE,
                    severity=rule_meta.severity,
                    stats={
                        "scan_date": datetime.fromtimestamp(
                            resource.sync_date
                        ).isoformat(),
                        "status": "OK",
                        "message": "Processed successfully",
                    },
                    meta=None,
                    general_actions=[],
                    recommendation={
                        "article": rule_meta.article,
                        "impact": rule_meta.impact,
                        "description": description,
                    },
                )
                recommendations.setdefault(location, []).append(item)

        return recommendations


class OldCloudRecommendationBuilder(CloudRecommendationBuilder):
    """Old version of builder for cloud (AWS/GCP/Azure) recommendations."""

    def build(self) -> RecommendationsMapping:
        recommendations: RecommendationsMapping = {}

        for part, rule_meta in self._iter_parts_with_metadata():
            policy = part.policy
            recommendation = cast(
                RecommendationItem,
                RULE_RECOMMENDATION_MAPPING.get(policy),
            )
            if not recommendation:
                _LOG.warning(
                    f"Recommendation not found for policy: {policy}, skipping..."
                )
                continue

            item = deepcopy(recommendation)
            scan_date = datetime.fromtimestamp(part.timestamp).isoformat()
            description = self._get_description(policy)

            item["recommendation"][ARTICLE_ATTR] = rule_meta.article
            item["recommendation"][IMPACT_ATTR] = rule_meta.impact
            item["recommendation"]["description"] = description
            item["stats"]["scan_date"] = scan_date

            template_id = item["resource_id"]
            for res in part.resources:
                item_copy = item.copy()
                item_copy["resource_id"] = template_id.format(**res)
                recommendations.setdefault(part.location, []).append(item_copy)

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
            resource_type = service_to_resource_type(rule_meta.service, self._cloud)
            action = K8S_RESOURCE_TO_ACTION.get(resource_type, DEFAULT_K8S_ACTION)
            description = self._get_description(part.policy)

            for resource in part.resources:
                item = K8SRecommendationItem(
                    resource_id=self._application_uuid,
                    resource_type=resource_type,
                    source=SOURCE,
                    severity=rule_meta.severity,
                    stats={
                        "scan_date": scan_date,
                        "status": "OK",
                        "message": "Processed successfully",
                    },
                    meta=None,
                    general_actions=[action],
                    recommendation={
                        "resource_id": resource.get("id"),
                        "resource_type": resource_type,
                        "article": rule_meta.article,
                        "impact": rule_meta.impact,
                        "description": description,
                    },
                )
                recommendations.setdefault(location, []).append(item)

        return recommendations


class OldK8SRecommendationBuilder(K8SRecommendationBuilder):
    """Old version of builder for Kubernetes recommendations."""

    def build(self) -> K8SRecommendationsMapping:
        recommendations: K8SRecommendationsMapping = {}

        for part, rule_meta in self._iter_parts_with_metadata():
            item = cast(
                K8SRecommendationItem,
                deepcopy(K8S_RECOMMENDATION_MODEL),
            )
            scan_date = datetime.fromtimestamp(part.timestamp).isoformat()
            resource_type = rule_meta.service
            action = K8S_RESOURCE_TO_ACTION.get(resource_type, DEFAULT_K8S_ACTION)
            description = self._get_description(part.policy)

            item["recommendation"][ARTICLE_ATTR] = rule_meta.article
            item["recommendation"][IMPACT_ATTR] = rule_meta.impact
            item["recommendation"]["description"] = description
            item["recommendation"]["resource_type"] = resource_type
            item["stats"]["scan_date"] = scan_date
            item["general_actions"] = [action]
            item["resource_id"] = self._application_uuid

            template_id = item["resource_id"]
            for res in part.resources:
                item_copy = item.copy()
                item_copy["resource_id"] = template_id.format(**res)
                location = self._region or part.location
                recommendations.setdefault(location, []).append(item_copy)

        return recommendations
