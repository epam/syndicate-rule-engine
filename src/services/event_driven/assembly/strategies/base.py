"""Strategy-based persistence of policy-filters bundles (K8s today, clouds later)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, ClassVar

from helpers.log_helper import get_logger
from models.event import EventRecordAttribute
from services.event_driven.assembly.job_rule_refs import JobRuleRefs
from services.event_driven.assembly.resource_refs import (
    ResourceRef,
)

if TYPE_CHECKING:
    from models.job import Job
    from services.platform_service import Platform

_LOG = get_logger(__name__)


class PolicyBundlePersistenceStrategy(ABC):
    """Persist narrow-scan bundle for a saved job when applicable."""

    @abstractmethod
    def maybe_persist(
        self,
        *,
        job: Job,
        rule_refs: JobRuleRefs | None,
        platform: Platform | None,
    ) -> None:
        raise NotImplementedError


class NullPolicyBundleStrategy(PolicyBundlePersistenceStrategy):
    """No bundle (tenant-scoped jobs or unsupported platform type)."""

    _instance: ClassVar[NullPolicyBundleStrategy | None] = None

    def __new__(cls) -> NullPolicyBundleStrategy:
        if cls is not NullPolicyBundleStrategy:
            return super().__new__(cls)
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def maybe_persist(
        self,
        *,
        job: Job,
        rule_refs: JobRuleRefs | None,
        platform: Platform | None,
    ) -> None:
        return None


class ResourceRefExtractionStrategy(ABC):
    """Extract a resource handle for narrow scan, if the event carries one."""

    @abstractmethod
    def try_extract(
        self,
        event_record: EventRecordAttribute,
    ) -> ResourceRef | None:
        raise NotImplementedError


class NullResourceRefStrategy(ResourceRefExtractionStrategy):
    """Public clouds (until narrow-scan metadata exists)."""

    def try_extract(
        self,
        event_record: EventRecordAttribute,
    ) -> ResourceRef | None:
        return None
