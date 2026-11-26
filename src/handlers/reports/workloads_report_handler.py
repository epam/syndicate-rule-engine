import tempfile
from http import HTTPStatus
from typing import BinaryIO, cast

from xlsxwriter import Workbook

from handlers import AbstractHandler, Mapping
from helpers.constants import Cloud, Endpoint, HTTPMethod, ReportFormat
from helpers.lambda_response import build_response
from helpers.log_helper import get_logger
from models.job import Job
from services import SP
from services.job_service import JobService
from services.k8s_workloads import (
    POD,
    WorkloadFinding,
    build_tables,
    rule_matches,
)
from services.k8s_workloads_xlsx import WorkloadsXlsxWriter
from services.license_service import LicenseService
from services.platform_service import Platform, PlatformService
from services.report_service import ReportResponse, ReportService
from services.resources import (
    K8SResource,
    iter_rule_resource,
    prepare_resource_type,
)
from services.sharding import ShardsCollection
from validators.swagger_request_models import (
    PlatformK8sWorkloadsReportGetModel,
)
from validators.utils import validate_kwargs

_LOG = get_logger(__name__)

DETAILED = 'detailed'
SUMMARY = 'summary'


class WorkloadsReportHandler(AbstractHandler):
    def __init__(
        self,
        report_service: ReportService,
        job_service: JobService,
        platform_service: PlatformService,
        license_service: LicenseService,
    ):
        self._report_service = report_service
        self._job_service = job_service
        self._platform_service = platform_service
        self._ls = license_service

    @classmethod
    def build(cls) -> WorkloadsReportHandler:
        return cls(
            report_service=SP.report_service,
            job_service=SP.job_service,
            platform_service=SP.platform_service,
            license_service=SP.license_service,
        )

    @property
    def mapping(self) -> Mapping:
        return {
            Endpoint.REPORTS_WORKLOADS_DETAILED_LATEST: {
                HTTPMethod.GET: self.get_detailed_latest
            },
            Endpoint.REPORTS_WORKLOADS_DETAILED_JOB: {
                HTTPMethod.GET: self.get_detailed_job
            },
            Endpoint.REPORTS_WORKLOADS_SUMMARY_LATEST: {
                HTTPMethod.GET: self.get_summary_latest
            },
            Endpoint.REPORTS_WORKLOADS_SUMMARY_JOB: {
                HTTPMethod.GET: self.get_summary_job
            },
        }

    @validate_kwargs
    def get_detailed_latest(
        self, event: PlatformK8sWorkloadsReportGetModel, platform_id: str
    ):
        return self._latest(event, platform_id, DETAILED)

    @validate_kwargs
    def get_summary_latest(
        self, event: PlatformK8sWorkloadsReportGetModel, platform_id: str
    ):
        return self._latest(event, platform_id, SUMMARY)

    @validate_kwargs
    def get_detailed_job(
        self, event: PlatformK8sWorkloadsReportGetModel, job_id: str
    ):
        return self._job(event, job_id, DETAILED)

    @validate_kwargs
    def get_summary_job(
        self, event: PlatformK8sWorkloadsReportGetModel, job_id: str
    ):
        return self._job(event, job_id, SUMMARY)

    def _latest(
        self,
        event: PlatformK8sWorkloadsReportGetModel,
        platform_id: str,
        kind: str,
    ):
        platform = self._platform_service.get_nullable(hash_key=platform_id)
        if not platform or (
            event.customer and platform.customer != event.customer
        ):
            return build_response(
                code=HTTPStatus.NOT_FOUND, content='Platform not found'
            )
        collection = self._report_service.platform_latest_collection(platform)
        collection.fetch_all()
        collection.fetch_meta()
        return self._build_report(
            event=event,
            platform=platform,
            collection=collection,
            kind=kind,
            filename=f'workloads-{kind}-platform-{platform.id}-latest.xlsx',
        )

    def _job(
        self,
        event: PlatformK8sWorkloadsReportGetModel,
        job_id: str,
        kind: str,
    ):
        job = self._job_service.get_nullable(job_id)
        if not job or not job.is_platform_job:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content='Platform scan job not found',
            )
        platform = self._platform_service.get_nullable(
            hash_key=job.platform_id
        )
        if not platform or (
            event.customer and platform.customer != event.customer
        ):
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content='Platform scan job not found',
            )
        if job.is_ed_job:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content='Workloads report is not available for '
                'event-driven scans',
            )
        collection = self._report_service.platform_job_collection(
            platform, job
        )
        collection.meta = self._report_service.fetch_meta(platform)
        collection.fetch_all()
        return self._build_report(
            event=event,
            platform=platform,
            collection=collection,
            kind=kind,
            filename=(
                f'workloads-{kind}-platform-{platform.id}-job-{job.id}.xlsx'
            ),
            job=job,
        )

    @staticmethod
    def _pod_rules(collection: ShardsCollection, query: str | None) -> set:
        return {
            rule
            for rule, meta in (collection.meta or {}).items()
            if prepare_resource_type(
                meta.get('resource') or '', Cloud.KUBERNETES
            )
            == POD
            and rule_matches(rule, query)
        }

    @staticmethod
    def _iter_findings(
        collection: ShardsCollection, metadata, rules: set[str]
    ) -> list[WorkloadFinding]:
        findings = []
        it = iter_rule_resource(
            collection=collection,
            cloud=Cloud.KUBERNETES,
            metadata=metadata,
            policies=rules,
            resource_types=(POD,),
        )
        for rule, res in it:
            assert isinstance(res, K8SResource)
            findings.append(
                WorkloadFinding(
                    rule=rule,
                    name=res.name,
                    namespace=res.namespace or '',
                    data=res.data,
                )
            )
        return findings

    def _build_report(
        self,
        *,
        event: PlatformK8sWorkloadsReportGetModel,
        platform: Platform,
        collection: ShardsCollection,
        kind: str,
        filename: str,
        job: Job | None = None,
    ):
        rules = self._pod_rules(collection, event.rule)
        if event.rule and not rules:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content=f'Pod rule {event.rule} not found in scan results',
            )
        metadata = self._ls.get_customer_metadata(event.customer_id)
        findings = self._iter_findings(collection, metadata, rules)
        descriptions = {
            rule: meta.get('description') or ''
            for rule, meta in (collection.meta or {}).items()
        }
        tables = build_tables(findings, metadata, descriptions)
        _LOG.debug(f'Built {len(tables)} workloads tables')

        with tempfile.TemporaryFile() as buffer:
            with Workbook(buffer, {'constant_memory': True}) as wb:
                WorkloadsXlsxWriter(wb, summary=kind == SUMMARY).write(tables)
            buffer.seek(0)
            url = self._report_service.one_time_url(
                cast(BinaryIO, buffer), filename
            )
        content = ReportResponse(platform, url, fmt=ReportFormat.XLSX).dict()
        if job:
            content['job_id'] = job.id
        return build_response(content=content)
