"""
Report delivery service for event-driven (REACTIVE) jobs.

Handles:
- Job completion notification: enqueue report generation for immediate mode
- Attacks report delivery via Maestro
- Interval mode: aggregated attacks report per interval window
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any, Iterable, cast, NamedTuple

from helpers.constants import (
    Cloud,
    DEPRECATED_RULE_SUFFIX,
    JobState,
    JobType,
    RabbitCommand,
    ReportType,
    TS_EXCLUDED_RULES_KEY,
    SRE_REPORTS_TYPE_TO_M3_MAPPING,
)
from helpers.log_helper import get_logger
from helpers.reports import service_from_resource_type
from helpers.time_helper import utc_datetime, utc_iso
from services.modular_helpers import get_tenant_regions, tenant_cloud
from services.platform_service import Platform, PlatformService
from services.reports import Report, ReportVisitor, strip_attacks_violations_for_maestro
from services.resources import MaestroReportResourceView, rule_resources_dict
from modular_sdk.models.tenant import Tenant
from typing_extensions import Self
from services.metadata import Metadata
from services.sharding import ShardsCollection, ShardPart

if TYPE_CHECKING:
    from modular_sdk.modular import ModularServiceProvider
    from models.job import Job
    from services.job_service import JobService
    from services.license_service import License, LicenseService
    from services.rabbitmq_service import RabbitMQService
    from services.report_service import ReportService
    from services.setting_service import SettingsService

_LOG = get_logger(__name__)

REPORT_DELIVERY_MODE_IMMEDIATE = "immediate"
REPORT_DELIVERY_MODE_INTERVAL = "interval"
# Buffer for job completion: jobs may take up to ~4h
JOB_COMPLETION_BUFFER_MINUTES = 240


def create_rules_metadata(
    total: int = 0,
    disabled: list[Any] | None = None,
    deprecated: list[Any] | None = None,
    passed: list[Any] | None = None,
    failed: list[Any] | None = None,
    violated: list[Any] | None = None,
    not_executed: list[Any] | None = None,
) -> dict:
    return {
        "total": total,
        "disabled": disabled or [],
        "deprecated": deprecated or [],
        "passed": passed or [],
        "failed": failed or [],
        "violated": violated or [],
        "not_executed": not_executed or [],
    }


def build_attacks_report_payload(
    *,
    data: list[dict],
    customer: str,
    tenant_name: str,
    tenant_id: str,
    cloud: Cloud,
    receivers: list,
    report_from: str,
    report_to: str,
    created_at: str,
    tenant_metadata: dict | None = None,
    jobs_count: int | None = None,
) -> dict:
    """
    Build attacks report payload for Maestro.
    Minimal, self-contained — no MaestroModelBuilder overhead.
    """
    strip_attacks_violations_for_maestro(data)
    payload = {
        "receivers": receivers,
        "customer": customer,
        "metadata": {
            "type": ReportType.OPERATIONAL_ATTACKS.value,
            "description": ReportType.OPERATIONAL_ATTACKS.description,
            "version": "2.0.0",
            "created_at": created_at,
            "to": report_to,
            "from": report_from,
        },
        "externalData": False,
        "data": data,
        "exceptions_data": [],
        "tenant_name": tenant_name,
        "id": tenant_id,
        "cloud": cloud.value,
        "tenant_metadata": tenant_metadata or {},
    }
    if jobs_count is not None:
        payload["jobs_count"] = jobs_count
    return payload


def build_platform_report(
    *,
    tenant: Tenant,
    receivers: list[str],
    data: dict,
    rules_data: dict,
    platform: Platform,
    report_from: str,
    report_to: str,
) -> dict:
    """
    Build OPERATIONAL_KUBERNETES report payload for Maestro
    """

    return {
        "receivers": receivers,
        "customer": tenant.customer_name,
        "from": report_from,
        "to": report_to,
        "outdated_tenants": [],
        "externalData": False,
        "data": data,
        "tenant_name": tenant.name,
        "last_scan_date": report_to,
        "cluster_id": platform.id,
        "cloud": tenant.cloud.upper(),
        "region": platform.region,
        "cluster_metadata": {
            "rules": {
                "violated": list(rules_data.get("violated", [])),
            },
        },
    }


def build_attacks_data(
    metadata: Metadata,
    rule_resources: dict,
    collection_meta: dict,
    report_type: ReportType = ReportType.OPERATIONAL_ATTACKS,
) -> list[dict]:
    """
    Build attacks data for Maestro report payload.
    """
    report = Report.derive_report(report_type)
    view = MaestroReportResourceView()

    generator = ReportVisitor.derive_visitor(
        ReportType.OPERATIONAL_ATTACKS,
        metadata=metadata,
        view=view,
        scope=None,
    )

    report_it = report.accept(
        generator,
        rule_resources=rule_resources,
        meta=collection_meta,
    )
    return list(report_it)


class AggregatedResources(NamedTuple):
    all_rule_resources: dict[str, set]
    rules_data: dict[str, dict]
    collections_meta: dict


class ReportDeliveryService:
    """
    Service for delivering attacks reports for event-driven (REACTIVE) jobs.
    Supports immediate (per-job) and interval (aggregated) delivery modes.
    """

    def __init__(
        self,
        license_service: LicenseService,
        job_service: JobService,
        report_service: ReportService,
        rabbitmq_service: RabbitMQService,
        modular_client: ModularServiceProvider,
        settings_service: SettingsService,
        platform_service: PlatformService,
    ) -> None:
        self._license_service = license_service
        self._job_service = job_service
        self._report_service = report_service
        self._rabbitmq_service = rabbitmq_service
        self._modular_client = modular_client
        self._settings_service = settings_service
        self._platform_service = platform_service

    @classmethod
    def build(cls) -> Self:
        from services import SERVICE_PROVIDER

        return cls(
            license_service=SERVICE_PROVIDER.license_service,
            job_service=SERVICE_PROVIDER.job_service,
            report_service=SERVICE_PROVIDER.report_service,
            rabbitmq_service=SERVICE_PROVIDER.rabbitmq_service,
            modular_client=SERVICE_PROVIDER.modular_client,
            settings_service=SERVICE_PROVIDER.settings_service,
            platform_service=SERVICE_PROVIDER.platform_service,
        )

    def _get_report_delivery_config(self, license_obj: License) -> dict | None:
        """Extract report_delivery config from license event_driven."""
        ed = license_obj.event_driven
        if not ed:
            return None
        if not ed.get("active"):
            return None
        rd = ed.get("report_delivery")
        if not rd or not (rd.get("enabled") if isinstance(rd, dict) else False):
            return None
        return dict(rd) if isinstance(rd, dict) else None

    def _get_event_driven_license(self, tenant: Tenant) -> License | None:
        """Get event-driven license for tenant (active, applicable, not expired)."""
        lic = self._license_service.get_tenant_license(tenant)
        if not lic:
            return None
        if lic.is_expired():
            return None
        if not self._license_service.is_subject_applicable(
            lic=lic,
            customer=tenant.customer_name,
            tenant_name=tenant.name,
        ):
            return None
        if not lic.event_driven.get("active"):
            return None
        return lic

    def _build_tenant_metadata(
        self,
        tenant: Tenant,
        lic: License,
        *,
        last_scan_date: str,
        finished_scans: int,
        succeeded_scans: int,
        rules_data: dict | None = None,
    ) -> dict:
        """
        Build tenant_metadata dict for Maestro attacks report payload.
        Matches structure expected by Maestro (licenses, last_scan_date, rules, etc.).
        When rules_data is provided, rules and license total_rules are filled from it.
        """
        tss = self._modular_client.tenant_settings_service()
        activated_regions = tuple(sorted(get_tenant_regions(tenant, tss)))
        allowance = lic.allowance or {}
        rules = rules_data if rules_data is not None else create_rules_metadata()
        license_meta = {
            "id": lic.license_key,
            "rulesets": list(lic.ruleset_ids) if lic.ruleset_ids else [],
            "total_rules": rules.get("total", 0),
            "jobs": allowance.get("job_balance", 0),
            "per": allowance.get("time_range", "DAY"),
            "description": lic.description or "",
            "valid_until": utc_iso(lic.expiration) if lic.expiration else None,
            "valid_from": utc_iso(lic.valid_from) if lic.valid_from else None,
        }
        meta = {
            "licenses": [license_meta],
            "is_automatic_scans_enabled": True,
            "in_progress_scans": 0,
            "finished_scans": finished_scans,
            "succeeded_scans": succeeded_scans,
            "last_scan_date": last_scan_date,
            "activated_regions": list(activated_regions),
            "rules": rules,
        }
        return meta

    @staticmethod
    def _rule_check_to_dict(
        *,
        rule_id: str,
        description: str,
        remediation: str,
        remediation_complexity: str,
        severity: str,
        service: str,
        resource_type: str,
        when: float,
    ) -> dict:
        """One rule item for tenant_metadata.rules.violated (full metadata)."""
        return {
            "id": rule_id,
            "description": description,
            "remediation": remediation,
            "remediation_complexity": remediation_complexity,
            "severity": severity,
            "service": service,
            "resource_type": resource_type,
            "when": when,
        }

    @staticmethod
    def _passed_rule_to_dict(
        *,
        rule_id: str,
        description: str,
        region: str,
        when: float,
    ) -> dict:
        """One rule item for tenant_metadata.rules.passed (id, description, region, when)."""
        return {
            "id": rule_id,
            "description": description,
            "region": region,
            "when": when,
        }

    @staticmethod
    def _failed_rule_to_dict(
        *,
        rule_id: str,
        description: str,
        region: str,
        when: float,
        error_type: str | None,
        error: str | None,
    ) -> dict:
        """One rule item for tenant_metadata.rules.failed (+ error_type, error)."""
        out: dict = {
            "id": rule_id,
            "description": description,
            "region": region,
            "when": when,
        }
        if error_type is not None:
            out["error_type"] = error_type
        if error is not None:
            out["error"] = error
        return out

    def _get_tenant_disabled_rules(self, tenant: Tenant) -> set[str]:
        """Rule ids excluded for this tenant or its customer (CUSTODIAN_EXCLUDED_RULES)."""
        excluded: set[str] = set()
        tss = self._modular_client.tenant_settings_service()
        ts = tss.get(tenant_name=tenant.name, key=TS_EXCLUDED_RULES_KEY)
        if ts and ts.value:
            excluded.update(ts.value.as_dict().get("rules") or ())
        css = self._modular_client.customer_settings_service()
        cs = css.get_nullable(
            customer_name=tenant.customer_name, key=TS_EXCLUDED_RULES_KEY
        )
        if cs and cs.value:
            excluded.update(cs.value.get("rules") or ())
        return excluded

    def _iter_deprecated_rules_dict(self, meta: dict, metadata: Metadata) -> list[dict]:
        """Deprecated rules from meta keys ending with DEPRECATED_RULE_SUFFIX."""
        out: list[dict] = []
        for policy in meta:
            if not policy.endswith(DEPRECATED_RULE_SUFFIX):
                continue
            rule_meta = metadata.rule(policy)
            deprecation = rule_meta.deprecation
            description = meta.get(policy, {}).get("description", "") or (
                rule_meta.impact if rule_meta.impact else ""
            )
            deprecation_date = None
            if isinstance(deprecation.date, date):
                deprecation_date = deprecation.date.isoformat()
            deprecation_reason = ""
            if isinstance(deprecation.link, str) and deprecation.link:
                deprecation_reason = deprecation.link
            elif rule_meta.impact:
                deprecation_reason = rule_meta.impact
            out.append(
                {
                    "id": policy,
                    "description": description,
                    "deprecation_date": deprecation_date,
                    "deprecation_reason": deprecation_reason or None,
                }
            )
        return out

    def _build_rules_from_collection(
        self,
        collection: "ShardsCollection",
        metadata: Metadata,
        scope: set[str],
        tenant: Tenant,
    ) -> dict:
        """
        Build rules summary (total, disabled, deprecated, violated, passed, failed).
        Matches ReportRulesMetadata shape; used for immediate report delivery.
        """
        meta = collection.meta or {}
        disabled = self._get_tenant_disabled_rules(tenant)
        disabled_in_scope = sorted(scope & disabled)
        deprecated_all = self._iter_deprecated_rules_dict(meta, metadata)
        deprecated_in_scope = [d for d in deprecated_all if d["id"] in scope]
        violated: list[dict] = []
        yielded_violated: set[str] = set()
        for part in collection.iter_parts():
            policy = part.policy
            if (
                policy not in scope
                or len(part.resources) == 0
                or policy in yielded_violated
            ):
                continue
            yielded_violated.add(policy)
            rm = metadata.rule(policy, resource=meta.get(policy, {}).get("resource"))
            rt = meta.get(policy, {}).get("resource", "")
            violated.append(
                self._rule_check_to_dict(
                    rule_id=policy,
                    description=meta.get(policy, {}).get("description") or "",
                    remediation=rm.remediation or "",
                    remediation_complexity=rm.remediation_complexity.value,
                    severity=rm.severity.value,
                    service=rm.service or service_from_resource_type(rt),
                    resource_type=rt,
                    when=part.timestamp,
                )
            )
        passed: list[dict] = []
        for part in collection.iter_all_parts():
            if part.has_error() or len(part.resources) > 0 or part.policy not in scope:
                continue
            pmeta = meta.get(part.policy, {})
            passed.append(
                self._passed_rule_to_dict(
                    rule_id=part.policy,
                    description=pmeta.get("description") or "",
                    region=part.location,
                    when=part.timestamp,
                )
            )
        failed: list[dict] = []
        for part in collection.iter_error_parts():
            if part.policy not in scope:
                continue
            pmeta = meta.get(part.policy, {})
            error_type_str = part.error.split(":", 1)[0] if part.error else None
            failed.append(
                self._failed_rule_to_dict(
                    rule_id=part.policy,
                    description=pmeta.get("description") or "",
                    region=part.location,
                    when=part.timestamp,
                    error_type=error_type_str,
                    error=part.error,
                )
            )
        result = create_rules_metadata(
            total=len(scope),
            disabled=disabled_in_scope,
            deprecated=deprecated_in_scope,
            passed=passed,
            failed=failed,
            violated=violated,
        )
        return result

    def notify_job_completed(self, job: Job, tenant: Tenant) -> None:
        """
        Called after a REACTIVE job completes successfully.
        If report_delivery is enabled and mode is immediate, enqueues report generation.
        """
        if job.job_type != JobType.REACTIVE:
            return

        lic = self._get_event_driven_license(tenant)
        if not lic:
            _LOG.warning(
                f"No event-driven license for tenant {tenant.name}, "
                "skip report notification"
            )
            return

        config = self._get_report_delivery_config(lic)
        if not config:
            return

        mode = config.get("mode") or REPORT_DELIVERY_MODE_IMMEDIATE
        if mode != REPORT_DELIVERY_MODE_IMMEDIATE:
            _LOG.info(f"Report delivery mode is {mode!r}, not immediate; skip enqueue")
            return

        try:
            from onprem.tasks import generate_reactive_report

            generate_reactive_report.delay(job.id)
            _LOG.info(f"Enqueued generate_reactive_report for job {job.id}")
        except Exception:
            _LOG.exception(
                f"Failed to enqueue generate_reactive_report for job {job.id}"
            )

    def _open_job_report_collection(
        self,
        job: Job,
        tenant: Tenant,
        default_tenant_cloud: Cloud,
    ) -> tuple["ShardsCollection", Cloud, str] | None:
        """
        Resolve S3 shards for a job: tenant path (cloud accounts) or K8s platform path.
        Returns (collection, effective_cloud, account_id_for_rule_resources).
        """
        if not job.platform_id:
            collection = self._report_service.job_collection(tenant, job)
            collection.meta = self._report_service.fetch_meta(tenant)
            return collection, default_tenant_cloud, tenant.project or ""

        platform = self._platform_service.get_nullable(hash_key=job.platform_id)
        if not platform:
            _LOG.warning(
                f"Platform {job.platform_id!r} not found for job {job.id}, "
                "skip report data"
            )
            return None
        if platform.customer != job.customer_name:
            _LOG.warning(
                f"Platform customer mismatch for job {job.id}: "
                f"{platform.customer!r} vs {job.customer_name!r}"
            )
            return None

        collection = self._report_service.platform_job_collection(platform, job)
        collection.meta = self._report_service.fetch_meta(platform)
        return collection, Cloud.KUBERNETES, ""

    def _collect_attacks_for_job(
        self,
        job: Job,
        tenant: Tenant,
        lic: License,
    ) -> tuple[list[dict], dict, Cloud] | None:
        """
        Load collection and generate attacks report data plus rules summary.
        Returns None if collection cannot be resolved or no rule_resources;
        else (attacks_data, rules_data, effective_cloud for Maestro payload).
        """
        default_tenant_cloud = tenant_cloud(tenant)
        opened = self._open_job_report_collection(
            job, tenant, default_tenant_cloud
        )
        if not opened:
            return None
        collection, effective_cloud, account_id = opened
        collection.fetch_all()
        metadata = self._license_service.get_metadata_for_licenses([lic])
        rule_resources = rule_resources_dict(
            collection, effective_cloud, metadata, account_id
        )
        if not rule_resources:
            return None
        attacks_data = build_attacks_data(
            metadata=metadata,
            rule_resources=rule_resources,
            collection_meta=collection.meta,
        )
        if not attacks_data:
            return None
        scope = set(rule_resources.keys())
        rules_data = self._build_rules_from_collection(
            collection, metadata, scope, tenant
        )
        return attacks_data, rules_data, effective_cloud

    def _build_platform_data(
        self,
        metadata: Metadata,
        rule_resources: dict,
        collection_meta: dict,
        collection: ShardsCollection,
    ) -> dict | None:
        """
        Build KUBERNETES data for Maestro report payload.
        """

        view = MaestroReportResourceView()
        report = Report.derive_report(ReportType.OPERATIONAL_KUBERNETES)
        resources_gen = ReportVisitor.derive_visitor(
            ReportType.OPERATIONAL_RESOURCES,
            metadata=metadata,
            view=view,
        )

        policy_data = list(
            report.accept(
                resources_gen,
                rule_resources=rule_resources,
                meta=collection_meta,
            )
        )
        if not policy_data:
            return None

        mitre_data = build_attacks_data(
            metadata=metadata,
            rule_resources=rule_resources,
            collection_meta=collection_meta,
            report_type=ReportType.OPERATIONAL_KUBERNETES,
        )
        if not mitre_data:
            return None

        compliance = self._report_service.calculate_tenant_full_coverage(
            col=collection, metadata=metadata, cloud=Cloud.KUBERNETES
        )
        compliance_data = [
            {"name": st.full_name, "value": round(cov * 100, 2)}
            for st, cov in compliance.items()
        ]

        return {
            "policy_data": policy_data,
            "mitre_data": mitre_data,
            "compliance_data": compliance_data,
        }
    
    def _collect_platform_data_for_job(
        self,
        job: Job,
        tenant: Tenant,
        lic: License,
    ) -> tuple[dict, Platform, Metadata, dict] | None:
        """
        Load platform collection and derived rule data for immediate delivery.
        """
        platform = self._platform_service.get_nullable(hash_key=job.platform_id)
        if not platform:
            _LOG.warning(f"Platform {job.platform_id} not found for job {job.id}")
            return None

        default_tenant_cloud = tenant_cloud(tenant)
        opened = self._open_job_report_collection(
            job, tenant, default_tenant_cloud
        )
        if not opened:
            return None

        collection, effective_cloud, account_id = opened
        collection.fetch_all()

        metadata = self._license_service.get_metadata_for_licenses([lic])
        rule_resources = rule_resources_dict(
            collection, effective_cloud, metadata, account_id
        )
        if not rule_resources:
            return None
        
        platform_data = self._build_platform_data(
            metadata=metadata,
            rule_resources=rule_resources,
            collection_meta=collection.meta,
            collection=collection,
        )
        if not platform_data:
            return None
        scope = set(rule_resources.keys())
        rules_data = self._build_rules_from_collection(
            collection, metadata, scope, tenant
        )
        return platform_data, platform, metadata, rules_data

    def generate_and_send_report_immediate(self, job_id: str) -> bool:
        """
        Generate attacks report for a single job and send via RabbitMQ.
        Returns True if sent, False if skipped (no attacks, no config, etc).
        For platform jobs, generates OPERATIONAL_KUBERNETES; for tenant jobs, OPERATIONAL_ATTACKS.
        Delegates to separate methods for tenant and platform job handling.
        """
        job = self._job_service.get_nullable(job_id)
        if not job:
            _LOG.warning(f"Job {job_id} not found")
            return False
        if job.job_type != JobType.REACTIVE:
            _LOG.debug(f"Job {job_id} is not REACTIVE, skip")
            return False

        tenant = self._modular_client.tenant_service().get(job.tenant_name)
        if not tenant:
            _LOG.warning(f"Tenant {job.tenant_name} not found")
            return False

        lic = self._get_event_driven_license(tenant)
        if not lic:
            _LOG.debug(f"No event-driven license for tenant {job.tenant_name}")
            return False

        config = self._get_report_delivery_config(lic)
        if not config or config.get("mode") != REPORT_DELIVERY_MODE_IMMEDIATE:
            return False

        rabbitmq = self._rabbitmq_service.get_customer_rabbitmq(
            job.customer_name
        )
        if not rabbitmq:
            _LOG.warning(
                f"No RabbitMQ for customer {job.customer_name} "
                "for event-driven report delivery"
            )
            return False

        receivers = config.get("receivers") or []
        now = utc_datetime()
        report_end = (
            job.stopped_at and utc_iso(utc_datetime(job.stopped_at)) or utc_iso(now)
        )
        report_from = (
            job.submitted_at
            and utc_iso(utc_datetime(job.submitted_at))
            or utc_iso(now - timedelta(days=7))
        )

        if job.platform_id:
            report_type = ReportType.OPERATIONAL_KUBERNETES

            result = self._collect_platform_data_for_job(
                job, tenant, lic
            )
            if not result:
                _LOG.info(f"No k8s data for job {job_id}, skip report send")
                return False
            platform_data, platform, metadata, rules_data = result

            payload = build_platform_report(
                tenant=tenant,
                receivers=receivers,
                data=platform_data,
                rules_data=rules_data,
                platform=platform,
                report_from=report_from,
                report_to=report_end,
            )
        else:
            report_type = ReportType.OPERATIONAL_ATTACKS

            result = self._collect_attacks_for_job(job, tenant, lic)
            if not result:
                _LOG.info(f"No attacks for job {job_id}, skip report send")
                return False
            attacks_data, rules_data, cloud = result

            tenant_metadata = self._build_tenant_metadata(
                tenant,
                lic,
                last_scan_date=report_end,
                finished_scans=1,
                succeeded_scans=1,
                rules_data=rules_data,
            )
            payload = build_attacks_report_payload(
                data=attacks_data,
                customer=job.customer_name,
                tenant_name=job.tenant_name,
                tenant_id=tenant.project or "",
                cloud=cloud,
                receivers=receivers,
                report_from=report_from,
                report_to=report_end,
                created_at=utc_iso(now),
                tenant_metadata=tenant_metadata,
            )

        model = self._rabbitmq_service.build_m3_json_model(
            notification_type=SRE_REPORTS_TYPE_TO_M3_MAPPING[report_type],
            data=payload,
        )

        code = self._rabbitmq_service.send_to_m3(
            rabbitmq=rabbitmq,
            command=RabbitCommand.SEND_MAIL,
            models=[model],
        )
        if code != 200:
            _LOG.warning(f"RabbitMQ send returned {code}")
            return False
        _LOG.info(f"Sent {report_type.value} report for job {job_id}")
        return True

    def _update_last_report_sent_at(self, lic: License, now: datetime) -> None:
        """Update last_report_sent_at in license event_driven."""
        self._license_service.update_event_driven_last_report_sent_at(
            item=lic, last_report_sent_at=utc_iso(now)
        )

    def _aggregate_resources_for_jobs(
        self,
        jobs: list[Job],
        tenant: Tenant,
        metadata: Metadata,
    ) -> AggregatedResources | None:
        """
        Aggregate rules from multiple jobs.
        """
        if not metadata:
            _LOG.warning(f"No metadata for tenant {tenant.name}, skip aggregation")
            return None

        all_rule_resources: dict[str, set] = {}
        violated_by_id: dict[str, dict] = {}
        passed_by_id: dict[str, dict] = {}
        failed_by_id: dict[str, dict] = {}
        deprecated_by_id: dict[str, dict] = {}
        disabled_merged: set[str] = set()
        collections_meta = {}

        _LOG.debug(
            f"Processing {len(jobs)} jobs in "
            f"interval window for tenant {tenant.name}"
        )

        cloud = Cloud.parse(tenant.cloud, safe=False)
        for j in jobs:
            opened = self._open_job_report_collection(j, tenant, cloud)
            if not opened:
                continue

            col, job_cloud, account_id = opened
            col.fetch_all()

            rr = rule_resources_dict(col, job_cloud, metadata, account_id)
            for rule, resources in rr.items():
                all_rule_resources.setdefault(rule, set()).update(resources)
            if col.meta:
                collections_meta.update(col.meta)

            scope = {part.policy for part in col.iter_all_parts()}
            if scope:
                job_rules = self._build_rules_from_collection(
                    col, metadata, scope, tenant
                )
                for item in job_rules.get("violated", []):
                    violated_by_id[item["id"]] = item
                for item in job_rules.get("failed", []):
                    if item["id"] not in violated_by_id:
                        failed_by_id[item["id"]] = item
                for item in job_rules.get("passed", []):
                    rid = item["id"]
                    if rid not in violated_by_id and rid not in failed_by_id:
                        passed_by_id[rid] = item
                for rid in job_rules.get("disabled", []):
                    disabled_merged.add(rid)
                for d in job_rules.get("deprecated", []):
                    deprecated_by_id[d["id"]] = d

        all_rule_ids = (
            set(violated_by_id) | set(passed_by_id) | set(failed_by_id)
        )

        rules_data = create_rules_metadata(
            total=len(all_rule_ids) or len(all_rule_resources),
            disabled=sorted(disabled_merged),
            deprecated=list(deprecated_by_id.values()),
            violated=list(violated_by_id.values()),
            failed=list(failed_by_id.values()),
            passed=list(passed_by_id.values()),
        )

        return AggregatedResources(
            all_rule_resources=all_rule_resources,
            rules_data=rules_data,
            collections_meta=collections_meta,
        )

    def _send_interval_tenant_report(
        self,
        tenant_jobs: list[Job],
        tenant: Tenant,
        customer_name: str,
        lic: License,
        config: dict,
        metadata: Metadata,
        rabbitmq,
        fetch_start: datetime,
        now: datetime,
    ) -> bool:
        """
        Aggregate and send OPERATIONAL_ATTACKS report for tenant jobs in interval window.
        Returns True if sent, False if skipped (no data, no RabbitMQ, etc).
        """
        if not (tenant_jobs and metadata):
            return False

        aggregated = self._aggregate_resources_for_jobs(
            tenant_jobs, tenant, metadata
        )
        if not aggregated:
            _LOG.debug(
                f"No rule resources in interval window for tenant {tenant.name}"
            )
            return False
        
        all_rule_resources = aggregated.all_rule_resources
        collections_meta = aggregated.collections_meta
        rules_data = aggregated.rules_data

        if not (all_rule_resources and collections_meta):
            _LOG.debug(
                f"No rule resources in interval window for tenant {tenant.name}"
            )
            return False

        attacks_data = build_attacks_data(
            metadata=metadata,
            rule_resources=all_rule_resources,
            collection_meta=collections_meta,
        )
        if not attacks_data:
            _LOG.debug(f"No attacks data in interval window for tenant {tenant.name}")
            return False

        stopped_dts = [
            utc_datetime(j.stopped_at) for j in tenant_jobs if j.stopped_at
        ]
        last_stopped = max(stopped_dts, default=now)
        first_stopped = min(stopped_dts, default=fetch_start)

        tenant_metadata = self._build_tenant_metadata(
            tenant,
            lic,
            last_scan_date=utc_iso(last_stopped),
            finished_scans=len(tenant_jobs),
            succeeded_scans=len(tenant_jobs),
            rules_data=rules_data,
        )

        cloud = tenant_cloud(tenant)
        payload = build_attacks_report_payload(
            data=attacks_data,
            customer=customer_name,
            tenant_name=tenant.name,
            tenant_id=tenant.project or "",
            cloud=cloud,
            receivers=config.get("receivers") or [],
            report_from=utc_iso(first_stopped),
            report_to=utc_iso(last_stopped),
            created_at=utc_iso(now),
            tenant_metadata=tenant_metadata,
            jobs_count=len(tenant_jobs),
        )

        model = self._rabbitmq_service.build_m3_json_model(
            notification_type=SRE_REPORTS_TYPE_TO_M3_MAPPING[
                ReportType.OPERATIONAL_ATTACKS
            ],
            data=payload,
        )

        code = self._rabbitmq_service.send_to_m3(
            rabbitmq=rabbitmq,
            command=RabbitCommand.SEND_MAIL,
            models=[model],
        )
        if code == 200:
            _LOG.info(
                f"Sent interval attacks report for tenant {tenant.name}, "
                f"{len(tenant_jobs)} jobs"
            )
            return True

        _LOG.warning(
            f"Failed to send interval attacks report for tenant {tenant.name}, "
            f"{len(tenant_jobs)} jobs"
        )
        return False

    def _send_interval_platform_report(
        self,
        platform_jobs: list[Job],
        tenant: Tenant,
        receivers: list[str],
        metadata: Metadata,
        rabbitmq,
        fetch_start: datetime,
        now: datetime,
    ) -> bool:
        """
        Aggregate and send OPERATIONAL_KUBERNETES report for platform jobs in
        interval window.
        Returns True if sent, False if skipped (no data, no RabbitMQ, etc).
        """
        if not (platform_jobs and metadata):
            return False

        platform_id = platform_jobs[0].platform_id
        if not platform_id:
            return False

        platform = self._platform_service.get_nullable(hash_key=platform_id)
        if not platform:
            _LOG.warning(
                f"Platform {platform_id} not found for tenant {tenant.name}"
            )
            return False

        aggregated = self._aggregate_resources_for_jobs(
            platform_jobs, tenant, metadata
        )
        if not aggregated:
            _LOG.debug(
                f"No rule resources in interval window for "
                f"platform {platform_id} in tenant {tenant.name}"
            )
            return False

        all_rule_resources = aggregated.all_rule_resources
        collections_meta = aggregated.collections_meta
        rules_data = aggregated.rules_data

        if not (all_rule_resources and collections_meta):
            _LOG.debug(
                f"No rule resources in interval window for "
                f"platform {platform_id} in tenant {tenant.name}"
            )
            return False

        # collection to aggregate policy for coverage calculation
        collection = self._report_service.platform_latest_collection(
            platform=platform
        )

        collection.put_parts(
            ShardPart(policy=rule['id']) for rule in rules_data["passed"]
        )

        platform_data = self._build_platform_data(
            metadata=metadata,
            rule_resources=all_rule_resources,
            collection_meta=collections_meta,
            collection=collection,
        )
        if not platform_data:
            _LOG.debug(
                f"No platform data in interval window for "
                f"platform {platform_id} in tenant {tenant.name}"
            )
            return False
        
        stopped_dts = [
            utc_datetime(j.stopped_at) for j in platform_jobs if j.stopped_at
        ]
        last_stopped = max(stopped_dts, default=now)
        first_stopped = min(stopped_dts, default=fetch_start)

        k8s_payload = build_platform_report(
            tenant=tenant,
            receivers=receivers,
            data=platform_data,
            rules_data=rules_data,
            platform=platform,
            report_from=utc_iso(first_stopped),
            report_to=utc_iso(last_stopped),
        )

        model = self._rabbitmq_service.build_m3_json_model(
            notification_type=SRE_REPORTS_TYPE_TO_M3_MAPPING[
                ReportType.OPERATIONAL_KUBERNETES
            ],
            data=k8s_payload,
        )

        code = self._rabbitmq_service.send_to_m3(
            rabbitmq=rabbitmq,
            command=RabbitCommand.SEND_MAIL,
            models=[model],
        )
        if code == 200:
            _LOG.info(
                f"Sent interval kubernetes report for "
                f"platform {platform_id} in tenant {tenant.name}, "
                f"{len(platform_jobs)} jobs"
            )
            return True

        _LOG.warning(
            f"Failed to send interval kubernetes report for "
            f"platform {platform_id} in tenant {tenant.name}, "
            f"{len(platform_jobs)} jobs"
        )
        return False

    def process_interval_reports(self) -> None:
        """
        For each tenant with report_delivery mode=interval,
        check if interval has elapsed, aggregate attacks from jobs in window, send if any.
        Separates tenant jobs (OPERATIONAL_ATTACKS) from platform jobs (OPERATIONAL_KUBERNETES).
        Uses cursor for window: if 10-12 had nothing, next run checks 12-13, not 10-13.
        last_report_sent_at still used for throttle.
        """
        now = utc_datetime()
        sent_count = 0
        customer_service = self._modular_client.customer_service()
        tenant_service = self._modular_client.tenant_service()

        for customer in customer_service.i_get_customer():
            _LOG.debug(f"Processing customer {customer.name}")
            rabbitmq = self._rabbitmq_service.get_customer_rabbitmq(
                customer=customer.name,
            )
            if not rabbitmq:
                _LOG.warning(
                    f"No RabbitMQ for customer {customer.name} "
                    "for event-driven report delivery"
                )
                continue

            licenses = list(
                self._license_service.iter_customer_licenses(
                    customer=customer.name,
                )
            )
            metadata = self._license_service.get_metadata_for_licenses(
                licenses=licenses,
            )
            if not metadata:
                continue

            for lic in licenses:
                license_key = lic.license_key
                _LOG.debug(f"Customer {customer.name} has license {license_key}")

                config = self._get_report_delivery_config(lic)
                if not config or config.get("mode") != REPORT_DELIVERY_MODE_INTERVAL:
                    _LOG.debug(
                        f"No report delivery config for license {license_key} "
                        f"for customer {customer.name}"
                    )
                    continue
                receivers = config.get("receivers") or []

                interval_min = config.get("interval_minutes") or 60
                last_sent = lic.event_driven.get("last_report_sent_at")
                if last_sent:
                    last_dt = utc_datetime(last_sent)
                    if now < last_dt + timedelta(minutes=interval_min):
                        _LOG.debug(
                            f"Last report sent at {last_sent} is less than "
                            f"interval {interval_min} minutes ago, skip"
                        )
                        continue
                else:
                    last_dt = now - timedelta(minutes=interval_min)

                tenants = cast(
                    Iterable[Tenant],
                    tenant_service.i_get_tenant_by_customer(
                        customer_id=customer.name,
                        active=True,
                    ),
                )
                for tenant in tenants:
                    tenant_name = tenant.name
                    _LOG.debug(f"Processing tenant {tenant_name}")
                    if not self._license_service.is_subject_applicable(
                        lic=lic,
                        customer=customer.name,
                        tenant_name=tenant_name,
                    ):
                        _LOG.debug(
                            f"Tenant {tenant_name} is not applicable for license {license_key} "
                            f"for customer {customer.name}"
                        )
                        continue

                    cloud = tenant_cloud(tenant, safe=True)
                    if not cloud:
                        _LOG.debug(f"Tenant {tenant_name} has no supported cloud, skip")
                        continue
                    cursor_iso = self._settings_service.get_report_delivery_cursor(
                        customer=customer.name,
                        license_key=license_key,
                        tenant_name=tenant_name,
                    )
                    if cursor_iso:
                        try:
                            cursor_dt = utc_datetime(cursor_iso)
                        except Exception:
                            cursor_dt = last_dt
                    else:
                        cursor_dt = last_dt

                    fetch_start = cursor_dt - timedelta(
                        minutes=JOB_COMPLETION_BUFFER_MINUTES
                    )
                    _LOG.debug(
                        f"Fetching jobs for tenant {tenant_name} "
                        f"from {fetch_start} to {now}"
                    )

                    jobs = list(
                        self._job_service.get_by_tenant_name(
                            tenant_name=tenant_name,
                            job_types={JobType.REACTIVE},
                            status=JobState.SUCCEEDED,
                            start=fetch_start,
                            end=now,
                        )
                    )
                    jobs_in_window: list[Job] = []
                    for j in jobs:
                        if not j.stopped_at:
                            continue
                        try:
                            stopped_dt = utc_datetime(j.stopped_at)
                        except Exception:
                            continue
                        if cursor_dt < stopped_dt <= now:
                            jobs_in_window.append(j)

                    self._settings_service.save_report_delivery_cursor(
                        customer.name,
                        license_key,
                        tenant_name,
                        utc_iso(now),
                    )

                    if not jobs_in_window:
                        _LOG.debug(
                            f"No jobs in interval window for tenant " f"{tenant_name}"
                        )
                        continue

                    tenant_jobs = []
                    platform_jobs = []
                    for j in jobs_in_window:
                        if j.platform_id:
                            platform_jobs.append(j)
                        else:
                            tenant_jobs.append(j)

                    if tenant_jobs:
                        _LOG.debug(
                            f"Processing {len(tenant_jobs)} tenant jobs in "
                            f"interval window for tenant {tenant_name}"
                        )
                        sent = self._send_interval_tenant_report(
                            tenant_jobs=tenant_jobs,
                            tenant=tenant,
                            customer_name=customer.name,
                            lic=lic,
                            config=config,
                            metadata=metadata,
                            rabbitmq=rabbitmq,
                            fetch_start=fetch_start,
                            now=now,
                        )
                        if sent:
                            sent_count += 1

                    if platform_jobs:
                        platform_jobs_by_id = {}
                        for job in platform_jobs:
                            platform_jobs_by_id.setdefault(
                                job.platform_id, []
                            ).append(job)

                        for platform_id, jobs_group in platform_jobs_by_id.items():
                            _LOG.debug(
                                f"Processing {len(jobs_group)} platform jobs for "
                                f"platform {platform_id} in interval window "
                                f"for tenant {tenant_name}"
                            )
                            sent = self._send_interval_platform_report(
                                platform_jobs=jobs_group,
                                tenant=tenant,
                                receivers=receivers,
                                metadata=metadata,
                                rabbitmq=rabbitmq,
                                fetch_start=fetch_start,
                                now=now,
                            )
                            if sent:
                                sent_count += 1

                self._update_last_report_sent_at(lic, now)

        if sent_count:
            _LOG.info(f"Processed interval reports: {sent_count} sent")
        else:
            _LOG.info("No interval reports sent")
