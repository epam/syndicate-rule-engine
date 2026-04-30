"""Pluggable strategies for resource extraction and policy-bundle persistence."""

from typing_extensions import Self

from helpers.constants import Cloud
from models.job import Job
from services.event_driven.assembly.strategies.base import (
    NullPolicyBundleStrategy,
    NullResourceRefStrategy,
    PolicyBundlePersistenceStrategy,
    ResourceRefExtractionStrategy,
)
from services.event_driven.assembly.strategies.k8s import (
    KubernetesPlatformPolicyBundleStrategy,
    KubernetesResourceRefStrategy,
)

__all__ = (
    'PolicyBundlePersistenceStrategy',
    'ResourceRefExtractionStrategy',
    'DefaultPolicyBundleStrategyRouter',
    'ResourceRefStrategyCatalog',
)


class DefaultPolicyBundleStrategyRouter:
    """
    Routes to the correct bundle strategy.

    Today every ``job.platform_id`` job is a K8s cluster; when other platform
    kinds appear, resolve via ``platform`` (or job metadata) and return e.g.
    :class:`AwsPlatformPolicyBundleStrategy`.
    """

    __slots__ = ('_k8s', '_null')

    def __init__(
        self,
        *,
        k8s: KubernetesPlatformPolicyBundleStrategy,
    ) -> None:
        self._k8s = k8s
        self._null = NullPolicyBundleStrategy()

    @classmethod
    def default(cls) -> Self:
        return cls(
            k8s=KubernetesPlatformPolicyBundleStrategy.build(),
        )

    def strategy_for_job(self, job: Job) -> PolicyBundlePersistenceStrategy:
        if job.platform_id:
            return self._k8s
        return self._null


class ResourceRefStrategyCatalog:
    """
    Select extraction strategy by event ``cloud``.

    Register new clouds here (or inject a custom catalog in tests).
    """

    __slots__ = ('_by_cloud', '_fallback')

    def __init__(
        self,
        *,
        by_cloud: dict[str, ResourceRefExtractionStrategy] | None = None,
        fallback: ResourceRefExtractionStrategy | None = None,
    ) -> None:
        self._fallback = fallback or NullResourceRefStrategy()
        self._by_cloud = dict(by_cloud or {})

    @classmethod
    def default(cls) -> Self:
        k8s = KubernetesResourceRefStrategy()
        noop = NullResourceRefStrategy()
        return cls(
            by_cloud={
                Cloud.KUBERNETES.value: k8s,
                Cloud.AWS.value: noop,
                Cloud.AZURE.value: noop,
                Cloud.GOOGLE.value: noop,
                Cloud.GCP.value: noop,
            },
            fallback=noop,
        )

    def for_cloud(self, cloud: str) -> ResourceRefExtractionStrategy:
        return self._by_cloud.get(cloud, self._fallback)
