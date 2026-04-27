"""Build K8s policy-filters scan plan from :class:`K8sResourceRef` rows."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping

from typing_extensions import Self

from helpers.log_helper import get_logger
from models.event import EventRecordAttribute
from models.job import Job
from services.event_driven.assembly.job_rule_refs import JobRuleRefs
from services.event_driven.assembly.resource_refs import (
    K8sResourceRef,
    ResourceRef,
)
from services.event_driven.domain import RuleNameType
from services.event_driven.domain.models import KubernetesMetadata
from services.job_policy_filters import K8sBuildData, K8sBuildRequest
from services.job_policy_filters.service import (
    JobPolicyBundleService,
    PolicyFiltersBundleBuilder,
)
from services.platform_service import Platform

from .base import (
    PolicyBundlePersistenceStrategy,
    ResourceRefExtractionStrategy,
)

_LOG = get_logger(__name__)


class KubernetesResourceRefStrategy(ResourceRefExtractionStrategy):
    """Involved object from watcher / agent ingest."""

    def try_extract(
        self,
        event_record: EventRecordAttribute,
    ) -> ResourceRef | None:
        _LOG.debug('K8s involved object: %s', event_record)
        md = event_record.metadata
        if md is None:
            return None
        mapping = md.as_dict() if hasattr(md, 'as_dict') else md
        if not isinstance(mapping, dict):
            return None
        return K8sResourceRef(
            metadata=KubernetesMetadata.model_validate(mapping)
        )


class KubernetesPlatformPolicyBundleStrategy(PolicyBundlePersistenceStrategy):
    """S3 policy-filters bundle for Kubernetes platform-scoped reactive jobs."""

    def __init__(
        self,
        *,
        bundle_service: JobPolicyBundleService,
        filters_builder: PolicyFiltersBundleBuilder,
    ) -> None:
        self._bundle_service = bundle_service
        self._filters_builder = filters_builder

    @classmethod
    def build(cls) -> Self:
        return cls(
            bundle_service=JobPolicyBundleService.build(),
            filters_builder=PolicyFiltersBundleBuilder.build(),
        )

    def maybe_persist(
        self,
        *,
        job: Job,
        rule_refs: JobRuleRefs | None,
        platform: Platform | None,
    ) -> None:
        if not job.platform_id:
            return
        if rule_refs is None or rule_refs.is_effectively_empty():
            _LOG.warning(
                'Kubernetes event-driven job %s has no involvedObject '
                'UIDs in aggregated events; skipping policy filters bundle',
                job.id,
            )
            return
        if not platform:
            _LOG.error(
                'Cannot save policy filters bundle: platform %s not found',
                job.platform_id,
            )
            return
        bundle = self._filters_builder.build_k8s_bundle(
            self._build_request_for_scanned_rules(
                job.rules_to_scan,
                rule_refs.by_rule,
            ),
        )
        self._bundle_service.save_bundle(
            platform=platform,
            job=job,
            bundle=bundle,
        )

    @classmethod
    def _build_request_for_scanned_rules(
        cls,
        rules_to_scan: Iterable[RuleNameType],
        by_rule: Mapping[RuleNameType, Iterable[ResourceRef]],
    ) -> K8sBuildRequest:
        """One policy entry per rule in ``rules_to_scan``; missing rules → empty rows."""
        return K8sBuildRequest(
            policies={
                rule: cls._scan_rows_for_rule_refs(
                    r
                    for r in by_rule.get(rule, ())
                    if isinstance(r, K8sResourceRef)
                )
                for rule in rules_to_scan
            }
        )

    @staticmethod
    def _scan_rows_for_rule_refs(
        refs: Iterable[K8sResourceRef],
    ) -> list[K8sBuildData]:
        """Group by namespace; one resource → narrow query; several → namespace + ``uid in``."""
        by_uid: dict[str, tuple[str, str | None]] = {}
        for ref in refs:
            meta = ref.metadata
            uid = meta.resource_uid
            if not uid:
                continue
            if uid not in by_uid:
                by_uid[uid] = (meta.name, meta.namespace)

        ns_map: dict[str | None, list[tuple[str, str]]] = defaultdict(list)
        for uid, (name, ns) in by_uid.items():
            ns_map[ns].append((name, uid))

        entries: list[K8sBuildData] = []
        for ns in sorted(ns_map.keys(), key=lambda x: (x is None, x or '')):
            pairs = sorted(ns_map[ns], key=lambda t: t[1])
            one = len(pairs) == 1
            uids: str | list[str] = (
                pairs[0][1] if one else [u for _, u in pairs]
            )
            name = (pairs[0][0] or None) if one else None
            entries.append(K8sBuildData(namespace=ns, name=name, uids=uids))

        return entries
