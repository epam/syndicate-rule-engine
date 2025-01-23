import tempfile
from datetime import datetime
from functools import cmp_to_key
from http import HTTPStatus
from itertools import chain
from typing import Any, Iterator, Optional, TypedDict

from modular_sdk.models.tenant import Tenant
from modular_sdk.services.tenant_service import TenantService
from xlsxwriter import Workbook
from xlsxwriter.worksheet import Worksheet

from handlers import AbstractHandler, Mapping
from helpers import filter_dict, flip_dict, hashable
from helpers.constants import (
    JOB_ID_ATTR,
    REPORT_FIELDS,
    TYPE_ATTR,
    CustodianEndpoint,
    HTTPMethod,
    JobState,
    JobType,
    ReportFormat,
    Severity,
    Cloud
)
from helpers.lambda_response import build_response
from helpers.log_helper import get_logger
from helpers.reports import severity_cmp
from helpers.time_helper import utc_iso
from services import SP, modular_helpers, obfuscation
from services.ambiguous_job_service import AmbiguousJob, AmbiguousJobService
from services.license_service import LicenseService
from services.metadata import Metadata
from services.modular_helpers import tenant_cloud
from services.platform_service import Platform, PlatformService
from services.report_service import ReportResponse, ReportService
from services.reports import ShardsCollectionDataSource
from services.sharding import ShardsCollection
from services.xlsx_writer import CellContent, Table, XlsxRowsWriter
from validators.swagger_request_models import (
    PlatformK8sResourcesReportGetModel,
    ResourceReportJobGetModel,
    ResourceReportJobsGetModel,
    ResourcesReportGetModel,
)
from validators.utils import validate_kwargs

_LOG = get_logger(__name__)

# rule, region, dto, matched_dto, timestamp
Payload = tuple[str, str, dict, dict, float]


class MatchedResourcesIterator(Iterator[Payload]):
    def __init__(
        self,
        collection: ShardsCollection,
        cloud: Cloud,
        resource_type: Optional[str] = None,
        region: Optional[str] = None,
        exact_match: bool = True,
        search_by_all: bool = False,
        search_by: Optional[dict] = None,
        dictionary_out: dict | None = None,
    ):
        self._collection = collection
        self._cloud = cloud
        self._resource_type = resource_type
        self._region = region

        self._exact_match = exact_match
        self._search_by_all = search_by_all
        self._search_by = search_by or {}
        self._dictionary_out = dictionary_out

        self._it = None

    @property
    def collection(self) -> ShardsCollection:
        return self._collection

    def create_resources_generator(
        self,
    ) -> ShardsCollectionDataSource.ResourcesGenerator:
        """
        See metrics_service.create_resources_generator
        :return:
        """
        source = ShardsCollectionDataSource(
            collection=self._collection,
            metadata=Metadata.empty(),
            cloud=self._cloud
        )
        return source.create_resources_generator(
            only_report_fields=False,
            active_regions=(self._region,) if self._region else (),
            resource_types=(self._resource_type,)
            if self._resource_type
            else (),
        )

    def __iter__(self):
        self._it = self.create_resources_generator()
        return self

    def does_match(self, provided: Any, real: Any) -> bool:
        if isinstance(real, (list, dict)):
            return False
        if self._exact_match:
            return str(provided) == str(real)
        return str(provided).lower() in str(real).lower()

    def match_by_all(self, dto: dict) -> dict:
        """
        Takes all the values from search_by and tries to match all the keys.
        Returns the first matched
        """
        expected = set(map(str, self._search_by.values()))
        for key, real in dto.items():  # nested_items(dto)
            for provided in expected:
                if self.does_match(provided, real):
                    return {key: real}
        return {}

    def match(self, dto: dict) -> dict:
        """
        Matches by all the keys and values from search_by
        """
        result = {}
        for key, provided in self._search_by.items():
            real = dto.get(key)
            if not self.does_match(provided, real):
                return {}
            result[key] = real
        return result

    def __next__(self) -> Payload:
        while True:
            rule, region, dto, ts = next(self._it)
            if not self._search_by:
                if isinstance(self._dictionary_out, dict):
                    obfuscation.obfuscate_finding(dto, self._dictionary_out)
                return rule, region, dto, {}, ts
            if self._search_by_all:
                match = self.match_by_all(dto)
            else:
                match = self.match(dto)
            if match:
                if isinstance(self._dictionary_out, dict):
                    obfuscation.obfuscate_finding(dto, self._dictionary_out)
                return rule, region, dto, match, ts


class ResourceReportBuilder:
    class ResourceReport(TypedDict):
        account_id: Optional[str]
        platform_id: Optional[str]
        data: dict
        violated_rules: list[dict]
        matched_by: dict
        region: str
        resource_type: str
        last_found: float

    def __init__(
        self,
        matched_findings_iterator: MatchedResourcesIterator,
        entity: Tenant | Platform,
        metadata: Metadata,
        full: bool = True,
    ):
        self._it = matched_findings_iterator
        self._entity = entity
        self._full = full
        self._meta = metadata

    def _build_rules(self, rules: set[str]) -> list[dict]:
        res = []
        for rule in rules:
            rm = self._meta.rule(rule)
            res.append(
                {
                    'name': rule,
                    'description': self._it.collection.meta.get(rule, {}).get(
                        'description'
                    ),
                    'severity': rm.severity.value,
                    'remediation': rm.remediation,
                    'article': rm.article,
                    'impact': rm.impact,
                }
            )
        return res

    def build(self) -> list[ResourceReport]:
        datas = {}

        # the same resources have the same resource_type,
        # region and REPORT_FIELDS (id, name, arn)
        def get_report_fields(r):
            return set(self._meta.rule(r).report_fields) | REPORT_FIELDS

        for rule, region, dto, match_dto, ts in self._it:
            unique = hashable(
                (
                    filter_dict(dto, REPORT_FIELDS),
                    region,
                    self._it.collection.meta.get(rule, {}).get('resource'),
                )
            )
            # data, rules, matched_by, timestamp
            inner = datas.setdefault(unique, [{}, set(), {}, ts])
            if not self._full:
                dto = filter_dict(dto, get_report_fields(rule))
            inner[0].update(dto)
            inner[1].add(rule)
            inner[2].update(match_dto)
            inner[3] = max(inner[3], ts)
        result = []
        identifier = (
            {'account_id': self._entity.project}
            if isinstance(self._entity, Tenant)
            else {'platform_id': self._entity.id}
        )
        for unique, inner in datas.items():
            result.append(
                {
                    **identifier,
                    'data': inner[0],
                    'violated_rules': self._build_rules(inner[1]),
                    'matched_by': inner[2],
                    'region': unique[1],
                    'resource_type': unique[2],
                    'last_found': inner[3],
                }
            )
        return result


class ResourceReportXlsxWriter:
    head = (
        '№',
        'Service',
        'Resource',
        'Region',
        'Date updated',
        'Rule',
        'Description',
        'Severity',
        'Article',
        'Remediation',
    )

    def __init__(
        self,
        it: MatchedResourcesIterator,
        metadata: Metadata,
        full: bool = True,
        keep_region: bool = True,
    ):
        self._it = it
        self._full = full
        self._keep_region = keep_region
        self._meta = metadata

    def _aggregated(self) -> dict[tuple, list]:
        """
        Just makes a mapping of a unique resource to rules it violates
        """

        def get_report_fields(r):
            return set(self._meta.rule(r).report_fields) | REPORT_FIELDS

        res = {}
        for rule, region, dto, match_dto, ts in self._it:
            unique = hashable(
                (
                    filter_dict(dto, REPORT_FIELDS),
                    region,
                    self._it.collection.meta.get(rule, {}).get('resource'),
                )
            )
            data = res.setdefault(unique, [set(), {}, ts])
            data[0].add(rule)
            if not self._full:
                dto = filter_dict(dto, get_report_fields(rule))
            data[1].update(dto)
            data[2] = max(data[2], ts)
        return res

    def write(self, wsh: Worksheet, wb: Workbook):
        bold = wb.add_format({'bold': True})
        red = wb.add_format({'bg_color': '#da9694'})
        yellow = wb.add_format({'bg_color': '#ffff00'})
        green = wb.add_format({'bg_color': '#92d051'})
        gray = wb.add_format({'bg_color': '#bfbfbf'})

        def sf(sev: str):
            """Format for severity"""
            if sev == Severity.HIGH:
                return red
            if sev == Severity.MEDIUM:
                return yellow
            if sev == Severity.LOW:
                return green
            return gray

        table = Table()
        table.new_row()
        for h in self.head:
            table.add_cells(CellContent(h, bold))

        # a bit devilish code :(
        # imagine you have a list of lists or ints. The thing below sorts the
        # main lists when the key equal to the maximum value of inner lists.
        # But,
        # - instead of ints -> severities and custom cmp function
        # - instead of list of lists -> dict there values are tuples with
        # the first element - that list
        # - values of inner lists not the actual values to sort by. They
        # are not severities. Actual severities must be retrieved from a map
        key = cmp_to_key(severity_cmp)
        aggregated = dict(
            sorted(
                self._aggregated().items(),
                key=lambda p: key(
                    self._meta.rule(
                        max(
                            p[1][0],
                            key=lambda x: key(
                                self._meta.rule(x).severity.value
                            ),
                        )
                    ).severity.value
                ),
                reverse=True,
            )
        )
        i = 0
        for unique, data in aggregated.items():
            _, region, resource = unique
            rules, dto, ts = data
            rules = sorted(
                rules,
                key=lambda x: key(self._meta.rule(x).severity.value),
                reverse=True,
            )
            table.new_row()
            table.add_cells(CellContent(i))
            services = set(
                filter(None, (self._meta.rule(rule).service for rule in rules))
            )
            if services:
                table.add_cells(CellContent(', '.join(services)))
            else:
                table.add_cells()
            table.add_cells(CellContent(dto))
            if self._keep_region:
                table.add_cells(CellContent(region))
            else:
                table.add_cells(CellContent())
            table.add_cells(CellContent(utc_iso(datetime.fromtimestamp(ts))))
            table.add_cells(*[CellContent(rule) for rule in rules])
            table.add_cells(
                *[
                    CellContent(self._it.collection.meta[rule]['description'])
                    for rule in rules
                ]
            )
            table.add_cells(
                *[
                    CellContent(
                        self._meta.rule(rule).severity.value,
                        sf(self._meta.rule(rule).severity.value),
                    )
                    for rule in rules
                ]
            )
            table.add_cells(
                *[CellContent(self._meta.rule(rule).article) for rule in rules]
            )
            table.add_cells(
                *[
                    CellContent(self._meta.rule(rule).remediation)
                    for rule in rules
                ]
            )
            i += 1
        writer = XlsxRowsWriter()
        writer.write(wsh, table)


class ResourceReportHandler(AbstractHandler):
    def __init__(
        self,
        ambiguous_job_service: AmbiguousJobService,
        tenant_service: TenantService,
        report_service: ReportService,
        platform_service: PlatformService,
        license_service: LicenseService,
    ):
        self._ambiguous_job_service = ambiguous_job_service
        self._tenant_service = tenant_service
        self._report_service = report_service
        self._platform_service = platform_service
        self._ls = license_service

    @property
    def rs(self):
        return self._report_service

    @property
    def ajs(self):
        return self._ambiguous_job_service

    @classmethod
    def build(cls):
        return cls(
            ambiguous_job_service=SP.ambiguous_job_service,
            tenant_service=SP.modular_client.tenant_service(),
            report_service=SP.report_service,
            platform_service=SP.platform_service,
            license_service=SP.license_service,
        )

    @property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.REPORTS_RESOURCES_PLATFORMS_K8S_PLATFORM_ID_LATEST: {
                HTTPMethod.GET: self.k8s_platform_get_latest
            },
            CustodianEndpoint.REPORTS_RESOURCES_TENANTS_TENANT_NAME_LATEST: {
                HTTPMethod.GET: self.get_latest
            },
            CustodianEndpoint.REPORTS_RESOURCES_TENANTS_TENANT_NAME_JOBS: {
                HTTPMethod.GET: self.get_jobs
            },
            CustodianEndpoint.REPORTS_RESOURCES_JOBS_JOB_ID: {
                HTTPMethod.GET: self.get_specific_job
            },
        }

    @validate_kwargs
    def k8s_platform_get_latest(
        self, event: PlatformK8sResourcesReportGetModel, platform_id: str
    ):
        platform = self._platform_service.get_nullable(hash_key=platform_id)
        if (
            not platform
            or event.customer
            and platform.customer != event.customer
        ):
            return build_response(
                code=HTTPStatus.NOT_FOUND, content='Platform not found'
            )
        metadata = self._ls.get_customer_metadata(event.customer_id)
        collection = self._report_service.platform_latest_collection(platform)
        _LOG.debug('Fetching collection')
        collection.fetch_all()
        _LOG.debug('Fetching meta')
        collection.fetch_meta()

        dictionary_url = None
        dictionary = {}  # todo maybe refactor somehow
        matched = MatchedResourcesIterator(
            collection=collection,
            cloud=Cloud.KUBERNETES,
            resource_type=event.resource_type,
            exact_match=event.exact_match,
            search_by_all=event.search_by_all,
            search_by=event.extras,
            dictionary_out=dictionary if event.obfuscated else None,
        )
        content = {}
        match event.format:
            case ReportFormat.JSON:
                content = ResourceReportBuilder(
                    matched_findings_iterator=matched,
                    entity=platform,
                    full=event.full,
                    metadata=metadata,
                ).build()
                if event.obfuscated:
                    flip_dict(dictionary)
                    dictionary_url = self._report_service.one_time_url_json(
                        dictionary, 'dictionary.json'
                    )
                if event.href:
                    url = self._report_service.one_time_url_json(
                        content, f'{platform.id}-latest.json'
                    )
                    content = ReportResponse(
                        platform, url, dictionary_url, event.format
                    ).dict()
            case ReportFormat.XLSX:
                buffer = tempfile.TemporaryFile()
                with Workbook(buffer, {'strings_to_numbers': True}) as wb:
                    ResourceReportXlsxWriter(
                        matched,
                        full=event.full,
                        keep_region=False,
                        metadata=metadata,
                    ).write(wb=wb, wsh=wb.add_worksheet('resources'))
                if event.obfuscated:
                    flip_dict(dictionary)
                    dictionary_url = self._report_service.one_time_url_json(
                        dictionary, 'dictionary.json'
                    )
                buffer.seek(0)
                url = self._report_service.one_time_url(
                    buffer, f'{platform.id}-latest.xlsx'
                )
                content = ReportResponse(
                    platform, url, dictionary_url, event.format
                ).dict()
        return build_response(content=content)

    @validate_kwargs
    def get_latest(self, event: ResourcesReportGetModel, tenant_name: str):
        tenant_item = self._tenant_service.get(tenant_name)
        tenant_item = modular_helpers.assert_tenant_valid(
            tenant_item, event.customer
        )

        collection = self._report_service.tenant_latest_collection(tenant_item)
        if event.region:
            _LOG.debug(
                'Region is provided. Fetching only shard with ' 'this region'
            )
            collection.fetch(region=event.region)
        else:
            _LOG.debug('Region is not provided. Fetching all shards')
            collection.fetch_all()
        _LOG.debug('Fetching meta')
        collection.fetch_meta()
        metadata = self._ls.get_customer_metadata(event.customer_id)

        dictionary_url = None
        dictionary = {}  # todo maybe refactor somehow
        matched = MatchedResourcesIterator(
            collection=collection,
            cloud=tenant_cloud(tenant_item),
            resource_type=event.resource_type,
            region=event.region,
            exact_match=event.exact_match,
            search_by_all=event.search_by_all,
            search_by=event.extras,
            dictionary_out=dictionary if event.obfuscated else None,
        )
        content = {}
        match event.format:
            case ReportFormat.JSON:
                content = ResourceReportBuilder(
                    matched_findings_iterator=matched,
                    entity=tenant_item,
                    full=event.full,
                    metadata=metadata,
                ).build()
                if event.obfuscated:
                    flip_dict(dictionary)
                    dictionary_url = self._report_service.one_time_url_json(
                        dictionary, 'dictionary.json'
                    )
                if event.href:
                    url = self._report_service.one_time_url_json(
                        content, f'{tenant_name}-latest.json'
                    )
                    content = ReportResponse(
                        tenant_item, url, dictionary_url, event.format
                    ).dict()
            case ReportFormat.XLSX:
                buffer = tempfile.TemporaryFile()
                with Workbook(buffer, {'strings_to_numbers': True}) as wb:
                    ResourceReportXlsxWriter(matched, metadata).write(
                        wb=wb, wsh=wb.add_worksheet(tenant_name)
                    )
                if event.obfuscated:
                    flip_dict(dictionary)
                    dictionary_url = self._report_service.one_time_url_json(
                        dictionary, 'dictionary.json'
                    )
                buffer.seek(0)
                url = self._report_service.one_time_url(
                    buffer, f'{tenant_name}-latest.xlsx'
                )
                content = ReportResponse(
                    tenant_item, url, dictionary_url, event.format
                ).dict()
        return build_response(content=content)

    @validate_kwargs
    def get_jobs(self, event: ResourceReportJobsGetModel, tenant_name: str):
        tenant_item = self._tenant_service.get(tenant_name)
        tenant_item = modular_helpers.assert_tenant_valid(
            tenant_item, event.customer
        )

        jobs = self.ajs.get_by_tenant_name(
            tenant_name=tenant_name,
            job_type=event.job_type,
            status=JobState.SUCCEEDED,
            start=event.start_iso,
            end=event.end_iso,
        )
        metadata = self._ls.get_customer_metadata(event.customer_id)

        source_response = {}
        for source in self.ajs.to_ambiguous(jobs):
            if source.is_platform_job:
                continue
            if not source.is_ed_job:
                collection = self._report_service.job_collection(
                    tenant_item, source.job
                )
            else:
                collection = self._report_service.ed_job_collection(
                    tenant_item, source.job
                )
            if event.region:
                _LOG.debug(
                    'Region is provided. Fetching only shard with '
                    'this region'
                )
                collection.fetch(region=event.region)
            else:
                _LOG.debug('Region is not provided. Fetching all shards')
                collection.fetch_all()
            collection.meta = self._report_service.fetch_meta(tenant_item)
            matched = MatchedResourcesIterator(
                collection=collection,
                cloud=tenant_cloud(tenant_item),
                resource_type=event.resource_type,
                region=event.region,
                exact_match=event.exact_match,
                search_by_all=event.search_by_all,
                search_by=event.extras,
            )
            response = ResourceReportBuilder(
                matched_findings_iterator=matched,
                entity=tenant_item,
                full=event.full,
                metadata=metadata,
            ).build()
            if not response:
                _LOG.debug(f'No resources found for job {source}. Skipping')
                continue
            source_response[source] = response
        return build_response(
            content=chain.from_iterable(
                self.dto(s, r) for s, r in source_response.items()
            )
        )

    @validate_kwargs
    def get_specific_job(self, event: ResourceReportJobGetModel, job_id: str):
        job = self.ajs.get_job(
            job_id=job_id, typ=event.job_type, customer=event.customer
        )
        if not job:
            return build_response(
                content=f'Job {job_id} not found', code=HTTPStatus.NOT_FOUND
            )
        if job.is_platform_job:
            return build_response(
                code=HTTPStatus.NOT_IMPLEMENTED,
                content='Platform job resources report is not available now',
            )
        tenant = self._tenant_service.get(job.tenant_name)
        tenant = modular_helpers.assert_tenant_valid(tenant, event.customer)

        if job.type == JobType.MANUAL:
            collection = self._report_service.job_collection(tenant, job.job)
        else:
            collection = self._report_service.ed_job_collection(
                tenant, job.job
            )
        if event.region:
            _LOG.debug(
                'Region is provided. Fetching only shard with ' 'this region'
            )
            collection.fetch(region=event.region)
        else:
            _LOG.debug('Region is not provided. Fetching all shards')
            collection.fetch_all()
        collection.meta = self._report_service.fetch_meta(tenant)
        metadata = self._ls.get_customer_metadata(event.customer_id)

        dictionary_url = None
        dictionary = {}
        matched = MatchedResourcesIterator(
            collection=collection,
            cloud=tenant_cloud(tenant),
            resource_type=event.resource_type,
            region=event.region,
            exact_match=event.exact_match,
            search_by_all=event.search_by_all,
            search_by=event.extras,
            dictionary_out=dictionary if event.obfuscated else None,
        )
        response = ResourceReportBuilder(
            matched_findings_iterator=matched,
            entity=tenant,
            full=event.full,
            metadata=metadata,
        ).build()
        if event.obfuscated:
            flip_dict(dictionary)
            dictionary_url = self._report_service.one_time_url_json(
                dictionary, 'dictionary.json'
            )
        if event.href:
            url = self._report_service.one_time_url_json(
                response, f'{job_id}-resources.json'
            )
            content = ReportResponse(
                job, url, dictionary_url, ReportFormat.JSON
            ).dict()
        else:
            content = self.dto(job, response)
        return build_response(content=content)

    @staticmethod
    def dto(job: AmbiguousJob, response: list) -> list[dict]:
        return [
            {JOB_ID_ATTR: job.id, TYPE_ATTR: job.type, **res}
            for res in response
        ]
