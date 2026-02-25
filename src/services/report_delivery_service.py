"""
Report delivery service for event-driven (REACTIVE) jobs.

Handles:
- Job completion notification: enqueue report generation for immediate mode
- Attacks report delivery via RabbitMQ/Maestro
- Interval mode: aggregated attacks report per interval window
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from helpers.constants import Cloud, JobState, JobType, RabbitCommand, ReportType
from helpers.log_helper import get_logger
from helpers.time_helper import utc_datetime, utc_iso
from services.metadata import Metadata
from services.modular_helpers import get_tenant_regions, tenant_cloud
from services.reports import Report, ReportVisitor
from services.resources import MaestroReportResourceView, iter_rule_resources
from typing_extensions import Self

if TYPE_CHECKING:
    from modular_sdk.modular import ModularServiceProvider
    from models.job import Job
    from modular_sdk.models.tenant import Tenant
    from services.job_service import JobService
    from services.license_service import License, LicenseService
    from services.rabbitmq_service import RabbitMQService
    from services.report_service import ReportService
    from services.setting_service import SettingsService
    from services.sharding import ShardsCollection

_LOG = get_logger(__name__)

REPORT_DELIVERY_MODE_IMMEDIATE = 'immediate'
REPORT_DELIVERY_MODE_INTERVAL = 'interval'
CUSTODIAN_ATTACKS_REPORT = 'CUSTODIAN_ATTACKS_REPORT'
# Buffer for job completion: jobs may take up to ~4h
JOB_COMPLETION_BUFFER_MINUTES = 240

# Fields to strip from violations for Maestro (matches _operational_attacks_custom)
_ATTACKS_VIOLATION_STRIP = frozenset({
    'description', 'remediation', 'remediation_complexity', 'severity',
})


def _strip_attacks_violations(data: list[dict]) -> None:
    """Strip Maestro-unwanted fields from violations. Mutates in place."""
    for item in data:
        for attack in item.get('attacks', ()):
            for v in attack.get('violations', ()):
                for key in _ATTACKS_VIOLATION_STRIP:
                    v.pop(key, None)


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
    _strip_attacks_violations(data)
    payload = {
        'receivers': receivers,
        'customer': customer,
        'metadata': {
            'type': ReportType.OPERATIONAL_ATTACKS.value,
            'description': ReportType.OPERATIONAL_ATTACKS.description,
            'version': '2.0.0',
            'created_at': created_at,
            'to': report_to,
            'from': report_from,
        },
        'externalData': False,
        'data': data,
        'exceptions_data': [],
        'tenant_name': tenant_name,
        'id': tenant_id,
        'cloud': cloud.value,
        'tenant_metadata': tenant_metadata or {},
    }
    if jobs_count is not None:
        payload['jobs_count'] = jobs_count
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
        rd = ed.get('report_delivery')
        if not rd or not (rd.get('enabled') if isinstance(rd, dict) else False):
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
            lic=lic, customer=tenant.customer_name, tenant_name=tenant.name
        ):
            return None
        if not lic.event_driven.get('active'):
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
    ) -> dict:
        """
        Build tenant_metadata dict for Maestro attacks report payload.
        Matches structure expected by Maestro (licenses, last_scan_date, etc.).
        """
        tss = self._modular_client.tenant_settings_service()
        activated_regions = tuple(
            sorted(get_tenant_regions(tenant, tss))
        )
        allowance = lic.allowance or {}
        license_meta = {
            'id': lic.license_key,
            'rulesets': list(lic.ruleset_ids) if lic.ruleset_ids else [],
            'total_rules': 0,
            'jobs': allowance.get('job_balance', 0),
            'per': allowance.get('time_range', 'DAY'),
            'description': lic.description or '',
            'valid_until': utc_iso(lic.expiration) if lic.expiration else None,
            'valid_from': utc_iso(lic.valid_from) if lic.valid_from else None,
        }
        return {
            'licenses': [license_meta],
            'is_automatic_scans_enabled': True,
            'in_progress_scans': 0,
            'finished_scans': finished_scans,
            'succeeded_scans': succeeded_scans,
            'last_scan_date': last_scan_date,
            'activated_regions': list(activated_regions),
            'rules': {
                'total': 0,
                'disabled': [],
                'deprecated': [],
                'passed': [],
                'failed': [],
                'violated': [],
            },
        }

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
                f'No event-driven license for tenant {tenant.name}, '
                'skip report notification'
            )
            return

        config = self._get_report_delivery_config(lic)
        if not config:
            return

        mode = config.get('mode') or REPORT_DELIVERY_MODE_IMMEDIATE
        if mode != REPORT_DELIVERY_MODE_IMMEDIATE:
            _LOG.info(
                f'Report delivery mode is {mode!r}, not immediate; skip enqueue'
            )
            return

        try:
            from onprem.tasks import generate_reactive_report

            generate_reactive_report.delay(job.id)
            _LOG.info(f'Enqueued generate_reactive_report for job {job.id}')
        except Exception:
            _LOG.exception(
                f'Failed to enqueue generate_reactive_report for job {job.id}'
            )

    @staticmethod
    def _get_rule_resources(
        collection: 'ShardsCollection',
        cloud: Cloud,
        metadata: Metadata,
        account_id: str = '',
    ) -> dict[str, set]:
        """Build rule -> set of CloudResource from collection (same as metrics pipeline)."""
        from services.resources import CloudResource

        dct: dict[str, set[CloudResource]] = {}
        for rule, resources_it in iter_rule_resources(
            collection=collection,
            cloud=cloud,
            metadata=metadata,
            account_id=account_id,
        ):
            resources = set(resources_it)
            if resources:
                dct[rule] = resources
        return dct

    def _collect_attacks_for_job(
        self,
        job: Job,
        tenant: Tenant,
        cloud: Cloud,
        lic: License,
    ) -> list[dict] | None:
        """Load collection and generate attacks report data. Returns None if platform job."""
        if job.is_platform_job:
            return None
        collection = self._report_service.job_collection(tenant, job)
        collection.meta = self._report_service.fetch_meta(tenant)
        collection.fetch_all()
        metadata = self._license_service.get_metadata_for_licenses([lic])
        rule_resources = self._get_rule_resources(
            collection, cloud, metadata, tenant.project or ''
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
        return list(
            report.accept(
                generator,
                rule_resources=rule_resources,
                meta=collection.meta,
            )
        ) or None

    def generate_and_send_report_immediate(self, job_id: str) -> bool:
        """
        Generate attacks report for a single job and send via RabbitMQ.
        Returns True if sent, False if skipped (no attacks, no config, etc).
        """
        job = self._job_service.get_nullable(job_id)
        if not job:
            _LOG.warning(f'Job {job_id} not found')
            return False
        if job.job_type != JobType.REACTIVE:
            _LOG.debug(f'Job {job_id} is not REACTIVE, skip')
            return False

        tenant = self._modular_client.tenant_service().get(job.tenant_name)
        if not tenant:
            _LOG.warning(f'Tenant {job.tenant_name} not found')
            return False

        lic = self._get_event_driven_license(tenant)
        if not lic:
            _LOG.debug(f'No event-driven license for tenant {job.tenant_name}')
            return False

        config = self._get_report_delivery_config(lic)
        if not config or config.get('mode') != REPORT_DELIVERY_MODE_IMMEDIATE:
            return False

        cloud = tenant_cloud(tenant)
        attacks_data = self._collect_attacks_for_job(job, tenant, cloud, lic)
        if not attacks_data:
            _LOG.info(f'No attacks for job {job_id}, skip report send')
            return False

        rabbitmq = self._rabbitmq_service.get_customer_rabbitmq(
            job.customer_name
        )
        now = utc_datetime()
        report_end = job.stopped_at and utc_iso(
            utc_datetime(job.stopped_at)
        ) or utc_iso(now)
        report_from = job.submitted_at and utc_iso(
            utc_datetime(job.submitted_at)
        ) or utc_iso(now - timedelta(days=7))
        tenant_metadata = self._build_tenant_metadata(
            tenant,
            lic,
            last_scan_date=report_end,
            finished_scans=1,
            succeeded_scans=1,
        )
        attacks_payload = build_attacks_report_payload(
            data=attacks_data,
            customer=job.customer_name,
            tenant_name=job.tenant_name,
            tenant_id=tenant.project or '',
            cloud=cloud,
            receivers=config.get('receivers') or [],
            report_from=report_from,
            report_to=report_end,
            created_at=utc_iso(now),
            tenant_metadata=tenant_metadata,
        )
        model = self._rabbitmq_service.build_m3_json_model(
            notification_type=CUSTODIAN_ATTACKS_REPORT,
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
            _LOG.warning(f'RabbitMQ send returned {code}')
            return False
        _LOG.info(f'Sent attacks report for job {job_id}')
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
            rabbitmq = self._rabbitmq_service.get_customer_rabbitmq(
                customer.name
            )
            if not rabbitmq:
                _LOG.warning(
                    f"No RabbitMQ for customer {customer.name} "
                     "for event-driven report delivery"
                )
                continue

            for lic in self._license_service.iter_customer_licenses(
                customer.name
            ):
                config = self._get_report_delivery_config(lic)
                if (
                    not config
                    or config.get('mode') != REPORT_DELIVERY_MODE_INTERVAL
                ):
                    _LOG.debug(
                        f"No report delivery config for license {lic.license_key} "
                        f"for customer {customer.name}"
                    )
                    continue

                interval_min = config.get('interval_minutes') or 60
                last_sent = lic.event_driven.get('last_report_sent_at')
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

                scope = (lic.customers or {}).get(customer.name) or {}
                tenant_names = list(scope.get('tenants') or [])
                if not tenant_names:
                    _LOG.debug(
                        f"No tenants for license {lic.license_key} "
                        f"for customer {customer.name}, fetching tenants"
                    )
                    tenant_names = [
                        t.name
                        for t in tenant_service.i_get_tenant_by_customer(
                            customer.name
                        )
                    ]

                license_key = getattr(lic, 'license_key', None) or getattr(lic, 'key', str(id(lic)))
                _LOG.debug(
                    f"License key for license {lic.license_key} "
                    f"for customer {customer.name}: {license_key}"
                )

                for tenant_name in tenant_names:
                    tenant = tenant_service.get(tenant_name)
                    if not tenant:
                        _LOG.debug(
                            f"Tenant {tenant_name} not found for license {lic.license_key} "
                            f"for customer {customer.name}"
                        )
                        continue
                    if not self._license_service.is_subject_applicable(
                        lic=lic,
                        customer=customer.name,
                        tenant_name=tenant_name,
                    ):
                        _LOG.debug(
                            f"Tenant {tenant_name} is not applicable for license {lic.license_key} "
                            f"for customer {customer.name}"
                        )
                        continue

                    cloud = tenant_cloud(tenant)
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
                        continue

                    all_rule_resources: dict[str, set] = {}
                    collection_meta = {}
                    tenant_lic = self._get_event_driven_license(tenant)
                    metadata = (
                        self._license_service.get_metadata_for_licenses(
                            [tenant_lic]
                        )
                        if tenant_lic
                        else None
                    )

                    for j in jobs_in_window:
                        if metadata and not j.is_platform_job:
                            col = self._report_service.job_collection(
                                tenant, j
                            )
                            col.meta = self._report_service.fetch_meta(tenant)
                            col.fetch_all()
                            rr = self._get_rule_resources(
                                col, cloud, metadata,
                                tenant.project or ''
                            )
                            for rule, resources in rr.items():
                                all_rule_resources.setdefault(
                                    rule, set()
                                ).update(resources)
                            if col.meta:
                                collection_meta.update(col.meta)

                    if not metadata or not all_rule_resources or not collection_meta:
                        _LOG.debug(
                            f'No rule resources in interval window for tenant '
                            f'{tenant_name}'
                        )
                        continue

                    report = Report.derive_report(
                        ReportType.OPERATIONAL_ATTACKS
                    )
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
                        continue

                    assert tenant_lic is not None  # ensured by metadata check above
                    now_ts = utc_datetime()
                    last_stopped = max(
                        (utc_datetime(j.stopped_at) for j in jobs_in_window if j.stopped_at),
                        default=now_ts,
                    )
                    tenant_metadata = self._build_tenant_metadata(
                        tenant,
                        tenant_lic,
                        last_scan_date=utc_iso(last_stopped),
                        finished_scans=len(jobs_in_window),
                        succeeded_scans=len(jobs_in_window),
                    )
                    attacks_payload = build_attacks_report_payload(
                        data=attacks_data,
                        customer=customer.name,
                        tenant_name=tenant_name,
                        tenant_id=tenant.project or '',
                        cloud=cloud,
                        receivers=config.get('receivers') or [],
                        report_from=utc_iso(
                            now_ts - timedelta(days=7)
                        ),
                        report_to=utc_iso(now_ts),
                        created_at=utc_iso(now_ts),
                        tenant_metadata=tenant_metadata,
                        jobs_count=len(jobs_in_window),
                    )
                    model = self._rabbitmq_service.build_m3_json_model(
                        notification_type=CUSTODIAN_ATTACKS_REPORT,
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
                            f'Sent interval attacks report for tenant '
                            f'{tenant_name}, {len(jobs_in_window)} jobs'
                        )

                self._update_last_report_sent_at(lic, now)

        if sent_count:
            _LOG.info(f'Processed interval reports: {sent_count} sent')
