"""Strategy-based persistence of policy-filters bundles (K8s today, clouds later)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, ClassVar

from typing_extensions import override

from helpers.constants import Cloud
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
    ) -> None:
        return None


class ResourceRefExtractionStrategy(ABC):
    """Extract a resource handle for narrow scan, if the event carries one."""

    def try_extract(
        self,
        event_record: EventRecordAttribute,
    ) -> ResourceRef | None:
        if not self.can_be_extracted(event_record):
            _LOG.debug('Event record %s is not supported', event_record)
            return None
        return self._try_extract(event_record)

    def can_be_extracted(self, event_record: EventRecordAttribute) -> bool:
        return event_record.cloud == self._cloud

    @property
    @abstractmethod
    def _cloud(self) -> Cloud:
        raise NotImplementedError

    @abstractmethod
    def _try_extract(
        self,
        event_record: EventRecordAttribute,
    ) -> ResourceRef | None:
        raise NotImplementedError


class NullResourceRefStrategy(ResourceRefExtractionStrategy):
    """Public clouds (until narrow-scan metadata exists)."""

    _instance: ClassVar[NullResourceRefStrategy | None] = None

    def __new__(cls) -> NullResourceRefStrategy:
        if cls is not NullResourceRefStrategy:
            return super().__new__(cls)
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @property
    def _cloud(self) -> Cloud:
        raise NotImplementedError

    def _try_extract(
        self,
        event_record: EventRecordAttribute,
    ) -> ResourceRef | None:
        return None

    @override
    def can_be_extracted(self, event_record: EventRecordAttribute) -> bool:
        return True
