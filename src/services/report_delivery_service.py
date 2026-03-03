"""
Report delivery service for event-driven (REACTIVE) jobs.

Handles:
- Job completion notification: enqueue report generation for immediate mode
- Attacks report delivery via Maestro
- Interval mode: aggregated attacks report per interval window
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any, Iterable, cast

from helpers.constants import (
    Cloud,
    DEPRECATED_RULE_SUFFIX,
    JobState,
    JobType,
    RabbitCommand,
    ReportType,
    TS_EXCLUDED_RULES_KEY,
)
from helpers.log_helper import get_logger
from helpers.reports import service_from_resource_type
from helpers.time_helper import utc_datetime, utc_iso
from services.modular_helpers import get_tenant_regions, tenant_cloud
from services.reports import Report, ReportVisitor, strip_attacks_violations_for_maestro
from services.resources import MaestroReportResourceView, rule_resources_dict
from modular_sdk.models.tenant import Tenant
from typing_extensions import Self
from services.metadata import Metadata
from handlers.reports.high_level_reports_handler import SRE_REPORTS_TYPE_TO_M3_MAPPING


if TYPE_CHECKING:
    from modular_sdk.modular import ModularServiceProvider
    from models.job import Job
    from services.job_service import JobService
    from services.license_service import License, LicenseService
    from services.rabbitmq_service import RabbitMQService
    from services.report_service import ReportService
    from services.setting_service import SettingsService
    from services.sharding import ShardsCollection

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
    ) -> None:
        self._license_service = license_service
        self._job_service = job_service
        self._report_service = report_service
        self._rabbitmq_service = rabbitmq_service
        self._modular_client = modular_client
        self._settings_service = settings_service

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
        return {
            "licenses": [license_meta],
            "is_automatic_scans_enabled": True,
            "in_progress_scans": 0,
            "finished_scans": finished_scans,
            "succeeded_scans": succeeded_scans,
            "last_scan_date": last_scan_date,
            "activated_regions": list(activated_regions),
            "rules": rules,
        }

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

    def _collect_attacks_for_job(
        self,
        job: Job,
        tenant: Tenant,
        cloud: Cloud,
        lic: License,
    ) -> tuple[list[dict], dict] | None:
        """
        Load collection and generate attacks report data plus rules summary.
        Returns None if platform job or no rule_resources; else (attacks_data, rules_data).
        """
        if job.is_platform_job:
            return None
        collection = self._report_service.job_collection(tenant, job)
        collection.meta = self._report_service.fetch_meta(tenant)
        collection.fetch_all()
        metadata = self._license_service.get_metadata_for_licenses([lic])
        rule_resources = rule_resources_dict(
            collection, cloud, metadata, tenant.project or ""
        )
        if not rule_resources:
            return None
        report = Report.derive_report(ReportType.OPERATIONAL_ATTACKS)
        view = MaestroReportResourceView()
        generator = ReportVisitor.derive_visitor(
            ReportType.OPERATIONAL_ATTACKS,
            metadata=metadata,
            view=view,
            scope=None,
        )
        attacks_data = list(
            report.accept(
                generator,
                rule_resources=rule_resources,
                meta=collection.meta,
            )
        )
        if not attacks_data:
            return None
        scope = set(rule_resources.keys())
        rules_data = self._build_rules_from_collection(
            collection, metadata, scope, tenant
        )
        return (attacks_data, rules_data)

    def generate_and_send_report_immediate(self, job_id: str) -> bool:
        """
        Generate attacks report for a single job and send via RabbitMQ.
        Returns True if sent, False if skipped (no attacks, no config, etc).
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

        cloud = tenant_cloud(tenant)
        result = self._collect_attacks_for_job(job, tenant, cloud, lic)
        if not result:
            _LOG.info(f"No attacks for job {job_id}, skip report send")
            return False
        attacks_data, rules_data = result

        rabbitmq = self._rabbitmq_service.get_customer_rabbitmq(job.customer_name)
        now = utc_datetime()
        report_end = (
            job.stopped_at and utc_iso(utc_datetime(job.stopped_at)) or utc_iso(now)
        )
        report_from = (
            job.submitted_at
            and utc_iso(utc_datetime(job.submitted_at))
            or utc_iso(now - timedelta(days=7))
        )
        tenant_metadata = self._build_tenant_metadata(
            tenant,
            lic,
            last_scan_date=report_end,
            finished_scans=1,
            succeeded_scans=1,
            rules_data=rules_data,
        )
        attacks_payload = build_attacks_report_payload(
            data=attacks_data,
            customer=job.customer_name,
            tenant_name=job.tenant_name,
            tenant_id=tenant.project or "",
            cloud=cloud,
            receivers=config.get("receivers") or [],
            report_from=report_from,
            report_to=report_end,
            created_at=utc_iso(now),
            tenant_metadata=tenant_metadata,
        )
        model = self._rabbitmq_service.build_m3_json_model(
            notification_type=SRE_REPORTS_TYPE_TO_M3_MAPPING[
                ReportType.OPERATIONAL_ATTACKS
            ],
            data=attacks_payload,
        )
        if not rabbitmq:
            _LOG.warning(
                f"No RabbitMQ for customer {job.customer_name} "
                "for event-driven report delivery"
            )
            return False

        code = self._rabbitmq_service.send_to_m3(
            rabbitmq=rabbitmq,
            command=RabbitCommand.SEND_MAIL,
            models=[model],
        )
        if code != 200:
            _LOG.warning(f"RabbitMQ send returned {code}")
            return False
        _LOG.info(f"Sent attacks report for job {job_id}")
        return True

    def _update_last_report_sent_at(self, lic: License, now: datetime) -> None:
        """Update last_report_sent_at in license event_driven."""
        self._license_service.update_event_driven_last_report_sent_at(
            item=lic, last_report_sent_at=utc_iso(now)
        )

    def process_interval_reports(self) -> None:
        """
        For each tenant with report_delivery mode=interval,
        check if interval has elapsed, aggregate attacks from jobs in window, send if any.
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

                    all_rule_resources: dict[str, set] = {}
                    collection_meta = {}
                    violated_by_id: dict[str, dict] = {}
                    passed_by_id: dict[str, dict] = {}
                    failed_by_id: dict[str, dict] = {}
                    deprecated_by_id: dict[str, dict] = {}
                    disabled_merged: set[str] = set()

                    _LOG.debug(
                        f"Processing {len(jobs_in_window)} jobs in "
                        f"interval window for tenant {tenant_name}"
                    )
                    for j in jobs_in_window:
                        if metadata and not j.is_platform_job:
                            col = self._report_service.job_collection(tenant, j)
                            col.meta = self._report_service.fetch_meta(tenant)
                            col.fetch_all()
                            rr = rule_resources_dict(
                                col, cloud, metadata, tenant.project or ""
                            )
                            for rule, resources in rr.items():
                                all_rule_resources.setdefault(rule, set()).update(
                                    resources
                                )
                            if col.meta:
                                collection_meta.update(col.meta)
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
                                    if (
                                        rid not in violated_by_id
                                        and rid not in failed_by_id
                                    ):
                                        passed_by_id[rid] = item
                                for rid in job_rules.get("disabled", []):
                                    disabled_merged.add(rid)
                                for d in job_rules.get("deprecated", []):
                                    deprecated_by_id[d["id"]] = d

                    if not metadata or not all_rule_resources or not collection_meta:
                        _LOG.debug(
                            f"No rule resources in interval window for tenant "
                            f"{tenant_name}"
                        )
                        continue

                    report = Report.derive_report(ReportType.OPERATIONAL_ATTACKS)
                    view = MaestroReportResourceView()
                    generator = ReportVisitor.derive_visitor(
                        ReportType.OPERATIONAL_ATTACKS,
                        metadata=metadata,
                        view=view,
                        scope=None,
                    )
                    attacks_data = list(
                        report.accept(
                            generator,
                            rule_resources=all_rule_resources,
                            meta=collection_meta,
                        )
                    )
                    if not attacks_data:
                        _LOG.debug(
                            f"No attacks data in interval window for tenant "
                            f"{tenant_name}"
                        )
                        continue

                    now_ts = utc_datetime()
                    stopped_dts = [
                        utc_datetime(j.stopped_at)
                        for j in jobs_in_window
                        if j.stopped_at
                    ]
                    last_stopped = max(stopped_dts, default=now_ts)
                    first_stopped = min(stopped_dts, default=fetch_start)
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
                    tenant_metadata = self._build_tenant_metadata(
                        tenant,
                        lic,
                        last_scan_date=utc_iso(last_stopped),
                        finished_scans=len(jobs_in_window),
                        succeeded_scans=len(jobs_in_window),
                        rules_data=rules_data,
                    )
                    attacks_payload = build_attacks_report_payload(
                        data=attacks_data,
                        customer=customer.name,
                        tenant_name=tenant_name,
                        tenant_id=tenant.project or "",
                        cloud=cloud,
                        receivers=config.get("receivers") or [],
                        report_from=utc_iso(first_stopped),
                        report_to=utc_iso(last_stopped),
                        created_at=utc_iso(now_ts),
                        tenant_metadata=tenant_metadata,
                        jobs_count=len(jobs_in_window),
                    )
                    model = self._rabbitmq_service.build_m3_json_model(
                        notification_type=SRE_REPORTS_TYPE_TO_M3_MAPPING[
                            ReportType.OPERATIONAL_ATTACKS
                        ],
                        data=attacks_payload,
                    )

                    code = self._rabbitmq_service.send_to_m3(
                        rabbitmq=rabbitmq,
                        command=RabbitCommand.SEND_MAIL,
                        models=[model],
                    )
                    if code == 200:
                        sent_count += 1
                        _LOG.info(
                            f"Sent interval attacks report for tenant "
                            f"{tenant_name}, {len(jobs_in_window)} jobs"
                        )
                    else:
                        _LOG.warning(
                            f"Failed to send interval attacks report for tenant "
                            f"{tenant_name}, {len(jobs_in_window)} jobs"
                        )

                self._update_last_report_sent_at(lic, now)

        if sent_count:
            _LOG.info(f"Processed interval reports: {sent_count} sent")
        else:
            _LOG.info("No interval reports sent")
