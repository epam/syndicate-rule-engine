"""Resource identity references for narrow-scan assembly (extensible by cloud)."""

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass

from services.event_driven.domain.models import KubernetesMetadata


class ResourceRef(ABC):
    """Base type for cloud-specific resource handles attached to rules."""


@dataclass(frozen=True)
class K8sResourceRef(ResourceRef):
    """Kubernetes involved-object identity for policy-filters narrow scan."""

    metadata: KubernetesMetadata
