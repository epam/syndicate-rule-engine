from abc import ABC, abstractmethod
from copy import deepcopy
from functools import cached_property
from typing import Any, Generic, Iterator, Optional, TypedDict, TypeVar

from modular_sdk.models.tenant import Tenant

from helpers import get_logger
from helpers.constants import (
    ARTICLE_ATTR,
    IMPACT_ATTR,
    RESOURCE_TYPE_ATTR,
    SEVERITY_ATTR,
)
from services.metadata import Metadata
from services.sharding import ShardPart, ShardsCollection


_LOG = get_logger(__name__)

T = TypeVar("T", bound=dict)


class RecommendationStats(TypedDict):
    scan_date: Optional[str]
    status: str
    message: str


class BaseRecommendation(TypedDict):
    article: str
    impact: str
    description: str


class K8SRecommendation(BaseRecommendation):
    resource_id: Optional[str]
    resource_type: str


class RecommendationItem(TypedDict):
    resource_id: Optional[str]
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

RESOURCE_ID_KEYS: tuple[str, ...] = ("GroupId", "InstanceId", "selfLink", "id")

DEFAULT_K8S_ACTION = "POD"
DEFAULT_DESCRIPTION = "Description"


class PolicyMetadataCache:
    """Cache for policy metadata to avoid repeated lookups."""

    def __init__(self, metadata: Metadata) -> None:
        self._metadata = metadata
        self._cache: dict[str, dict[str, str]] = {}

    def get(self, policy: str) -> dict[str, str]:
        if policy not in self._cache:
            rule_meta = self._metadata.rules.get(policy)
            self._cache[policy] = {
                ARTICLE_ATTR: rule_meta.article if rule_meta else "",
                IMPACT_ATTR: rule_meta.impact if rule_meta else "",
                RESOURCE_TYPE_ATTR: rule_meta.service if rule_meta else "",
                SEVERITY_ATTR: rule_meta.severity if rule_meta else "",
            }
        return self._cache[policy]


class BucketKeyBuilder:
    """Builds S3 bucket keys for recommendations."""

    @staticmethod
    def build(tenant: Tenant, timestamp: int, region: str) -> str:
        return (
            f"{tenant.customer_name}/{tenant.cloud}/{tenant.name}/"
            f"{timestamp}/{region}.jsonl"
        )


class BaseRecommendationBuilder(ABC, Generic[T]):
    """Abstract base class for recommendation builders."""

    SOURCE = "SYNDICATE_RULE_ENGINE"

    def __init__(self, collection: ShardsCollection, metadata: Metadata) -> None:
        self._collection = collection
        self._metadata = metadata

    @abstractmethod
    def build(self) -> T:
        """Build recommendations mapping."""

    @cached_property
    def _policy_cache(self) -> PolicyMetadataCache:
        return PolicyMetadataCache(self._metadata)

    def _iter_shard_parts(self) -> Iterator[ShardPart]:
        """Iterate over all shard parts in the collection."""
        for _, shard in self._collection:
            yield from shard

    def _get_description(self, policy: str) -> str:
        return self._collection.meta.get(policy, {}).get(
            "description", DEFAULT_DESCRIPTION
        )

    @staticmethod
    def _create_stats() -> RecommendationStats:
        return {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully",
        }

    @staticmethod
    def _extract_resource_id(resource: dict[str, Any]) -> Optional[str]:
        """Extract resource ID from resource dict using known keys."""
        for key in RESOURCE_ID_KEYS:
            if key in resource:
                return str(resource[key])
        return None


class CloudRecommendationBuilder(BaseRecommendationBuilder[RecommendationsMapping]):
    """Builder for cloud (AWS/GCP/Azure) recommendations."""

    def build(self) -> RecommendationsMapping:
        recommendations: RecommendationsMapping = {}

        for part in self._iter_shard_parts():
            policy_meta = self._policy_cache.get(part.policy)
            template = self._create_item_template(part.policy, policy_meta)

            for resource in part.resources:
                item = deepcopy(template)
                item["resource_id"] = self._extract_resource_id(resource)
                recommendations.setdefault(part.location, []).append(item)

        return recommendations

    def _create_item_template(
        self, policy: str, policy_meta: dict[str, str]
    ) -> RecommendationItem:
        return RecommendationItem(
            resource_id=None,
            resource_type=policy_meta[RESOURCE_TYPE_ATTR],
            source=self.SOURCE,
            severity=policy_meta[SEVERITY_ATTR],
            stats=self._create_stats(),
            meta=None,
            general_actions=[],
            recommendation=BaseRecommendation(
                article=policy_meta[ARTICLE_ATTR],
                impact=policy_meta[IMPACT_ATTR],
                description=self._get_description(policy),
            ),
        )


class K8SRecommendationBuilder(BaseRecommendationBuilder[K8SRecommendationsMapping]):
    """Builder for Kubernetes recommendations."""

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

        for part in self._iter_shard_parts():
            policy_meta = self._policy_cache.get(part.policy)
            template = self._create_item_template(part.policy, policy_meta)
            location = self._region or part.location

            for resource in part.resources:
                item = deepcopy(template)
                item["recommendation"]["resource_id"] = resource.get("id")
                recommendations.setdefault(location, []).append(item)

        return recommendations

    def _create_item_template(
        self, policy: str, policy_meta: dict[str, str]
    ) -> K8SRecommendationItem:
        resource_type = policy_meta[RESOURCE_TYPE_ATTR]
        action = K8S_RESOURCE_TO_ACTION.get(resource_type, DEFAULT_K8S_ACTION)

        return K8SRecommendationItem(
            resource_id=self._application_uuid,
            resource_type=resource_type,
            source=self.SOURCE,
            severity=policy_meta[SEVERITY_ATTR],
            stats=self._create_stats(),
            meta=None,
            general_actions=[action],
            recommendation=K8SRecommendation(
                resource_id=None,
                resource_type=resource_type,
                article=policy_meta[ARTICLE_ATTR],
                impact=policy_meta[IMPACT_ATTR],
                description=self._get_description(policy),
            ),
        )
