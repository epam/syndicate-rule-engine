import tempfile
from http import HTTPStatus
from typing import BinaryIO, cast

from xlsxwriter import Workbook

from handlers import AbstractHandler, Mapping
from helpers.constants import (
    K8S_RBAC_OPTIONAL_RESOURCE_TYPES,
    Cloud,
    Endpoint,
    HTTPMethod,
    ReportFormat,
)
from helpers.lambda_response import build_response
from helpers.log_helper import get_logger
from models.job import Job
from services import SP
from services.job_service import JobService
from services.k8s_rbac import (
    FINDING_TYPES,
    RbacFinding,
    RbacGraph,
    build_summary_rows,
    build_tables,
)
from services.k8s_rbac_xlsx import RbacXlsxWriter
from services.license_service import LicenseService
from services.platform_service import Platform, PlatformService
from services.report_service import ReportResponse, ReportService
from services.resources import K8SResource, iter_rule_resource
from services.sharding import ShardsCollection
from validators.swagger_request_models import PlatformK8sRbacReportGetModel
from validators.utils import validate_kwargs

_LOG = get_logger(__name__)

WARNING_EMPTY_INVENTORY = (
    'The RBAC inventory snapshot for this scan is empty. Subjects, pods and '
    'aggregation cannot be resolved.'
)
WARNING_NO_BINDINGS = (
    'No role bindings were found in this scan inventory snapshot. Subjects '
    'cannot be resolved.'
)
WARNING_NO_WORKLOADS = (
    'The RBAC inventory snapshot for this scan has no {}. Pods and workloads '
    'of service accounts may be missing.'
)


class RbacReportHandler(AbstractHandler):
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
    def build(cls) -> RbacReportHandler:
        return cls(
            report_service=SP.report_service,
            job_service=SP.job_service,
            platform_service=SP.platform_service,
            license_service=SP.license_service,
        )

    @property
    def mapping(self) -> Mapping:
        return {
            Endpoint.REPORTS_RBAC_DETAILED_LATEST: {
                HTTPMethod.GET: self.get_detailed_latest
            },
            Endpoint.REPORTS_RBAC_DETAILED_JOB: {
                HTTPMethod.GET: self.get_detailed_job
            },
            Endpoint.REPORTS_RBAC_COMPACT_LATEST: {
                HTTPMethod.GET: self.get_single_pane_latest
            },
            Endpoint.REPORTS_RBAC_COMPACT_JOB: {
                HTTPMethod.GET: self.get_single_pane_job
            },
        }

    @staticmethod
    def _iter_findings(
        collection: ShardsCollection, metadata
    ) -> list[RbacFinding]:
        findings = []
        it = iter_rule_resource(
            collection=collection,
            cloud=Cloud.KUBERNETES,
            metadata=metadata,
            resource_types=tuple(FINDING_TYPES),
        )
        for rule, res in it:
            assert isinstance(res, K8SResource)
            findings.append(
                RbacFinding(
                    rule=rule,
                    resource_type=res.resource_type,
                    name=res.name,
                    namespace=res.namespace,
                    data=res.data,
                )
            )
        return findings

    @validate_kwargs
    def get_detailed_latest(
        self, event: PlatformK8sRbacReportGetModel, platform_id: str
    ):
        platform = self._platform_service.get_nullable(hash_key=platform_id)
        if not platform or (
            event.customer and platform.customer != event.customer
        ):
            return build_response(
                code=HTTPStatus.NOT_FOUND, content='Platform not found'
            )
        job_id = self._report_service.platform_latest_rbac_job_id(platform)
        if not job_id:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content='No complete RBAC scan snapshot found',
            )
        job = self._job_service.get_nullable(job_id)
        if not job or job.platform_id != platform.id:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content='RBAC scan job not found',
            )
        return self._build_report(
            event=event,
            platform=platform,
            job=job,
            filename=f'rbac-detailed-platform-{platform.id}-latest.xlsx',
        )

    @validate_kwargs
    def get_detailed_job(
        self,
        event: PlatformK8sRbacReportGetModel,
        job_id: str,
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
                content='RBAC report is not available for event-driven scans',
            )
        return self._build_report(
            event=event,
            platform=platform,
            job=job,
            filename=(
                f'rbac-detailed-platform-{platform.id}-job-{job.id}.xlsx'
            ),
        )

    @validate_kwargs
    def get_single_pane_latest(
        self, event: PlatformK8sRbacReportGetModel, platform_id: str
    ):
        platform = self._platform_service.get_nullable(hash_key=platform_id)
        if not platform or (
            event.customer and platform.customer != event.customer
        ):
            return build_response(
                code=HTTPStatus.NOT_FOUND, content='Platform not found'
            )
        job_id = self._report_service.platform_latest_rbac_job_id(platform)
        if not job_id:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content='No complete RBAC scan snapshot found',
            )
        job = self._job_service.get_nullable(job_id)
        if not job or job.platform_id != platform.id:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content='RBAC scan job not found',
            )
        return self._build_single_pane_report(
            platform=platform,
            job=job,
            filename=f'rbac-single-pane-platform-{platform.id}-latest.xlsx',
        )

    @validate_kwargs
    def get_single_pane_job(
        self,
        event: PlatformK8sRbacReportGetModel,
        job_id: str,
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
                content='RBAC report is not available for event-driven scans',
            )
        return self._build_single_pane_report(
            platform=platform,
            job=job,
            filename=(
                f'rbac-single-pane-platform-{platform.id}-job-{job.id}.xlsx'
            ),
        )

    @staticmethod
    def _snapshot_warnings(
        inventory: dict[str, list[dict]],
        graph: RbacGraph,
        *,
        include_workloads: bool,
    ) -> list[str]:
        warnings = []
        if not any(inventory.values()):
            warnings.append(WARNING_EMPTY_INVENTORY)
        elif not graph.has_bindings:
            warnings.append(WARNING_NO_BINDINGS)
        if include_workloads:
            missing = [
                rt
                for rt in K8S_RBAC_OPTIONAL_RESOURCE_TYPES
                if rt not in inventory
            ]
            if missing:
                warnings.append(
                    WARNING_NO_WORKLOADS.format(', '.join(missing))
                )
        return warnings

    def _build_report(
        self,
        *,
        event: PlatformK8sRbacReportGetModel,
        platform: Platform,
        job: Job,
        filename: str,
    ):
        inventory = self._report_service.platform_rbac_snapshot_inventory(
            platform, job
        )
        if inventory is None:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content=(
                    'Complete RBAC inventory snapshot not found for scan job'
                ),
            )

        collection = self._report_service.platform_job_collection(
            platform, job
        )
        collection.meta = self._report_service.fetch_meta(platform)
        collection.fetch_all()
        metadata = self._ls.get_customer_metadata(event.customer_id)
        findings = self._iter_findings(collection, metadata)
        graph = RbacGraph.build(inventory)

        warnings = self._snapshot_warnings(
            inventory, graph, include_workloads=True
        )

        descriptions = {
            rule: meta.get('description') or ''
            for rule, meta in collection.meta.items()
        }
        tables = build_tables(graph, findings, metadata, descriptions)
        _LOG.debug(f'Built {len(tables)} RBAC tables for job {job.id}')

        with tempfile.TemporaryFile() as buffer:
            # rows are written strictly in order, so they can be flushed
            # to disk instead of being kept in memory
            with Workbook(buffer, {'constant_memory': True}) as wb:
                RbacXlsxWriter(wb).write_tables(tables)
            buffer.seek(0)
            url = self._report_service.one_time_url(
                cast(BinaryIO, buffer),
                filename,
            )
        content = ReportResponse(platform, url, fmt=ReportFormat.XLSX).dict()
        content['job_id'] = job.id
        if warnings:
            content['warnings'] = warnings
        return build_response(content=content)

    def _build_single_pane_report(
        self,
        *,
        platform: Platform,
        job: Job,
        filename: str,
    ):
        inventory = self._report_service.platform_rbac_snapshot_inventory(
            platform, job
        )
        if inventory is None:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content=(
                    'Complete RBAC inventory snapshot not found for scan job'
                ),
            )

        graph = RbacGraph.build(inventory)
        rows = build_summary_rows(graph)
        warnings = self._snapshot_warnings(
            inventory, graph, include_workloads=False
        )
        with tempfile.TemporaryFile() as buffer:
            with Workbook(buffer, {'constant_memory': True}) as wb:
                RbacXlsxWriter(wb).write_summary(rows)
            buffer.seek(0)
            url = self._report_service.one_time_url(
                cast(BinaryIO, buffer),
                filename,
            )
        content = ReportResponse(platform, url, fmt=ReportFormat.XLSX).dict()
        content['job_id'] = job.id
        if warnings:
            content['warnings'] = warnings
        return build_response(content=content)
