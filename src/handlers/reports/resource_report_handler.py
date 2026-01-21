import tempfile
from collections import ChainMap
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
from helpers import flip_dict
from helpers.constants import (
    JOB_ID_ATTR,
    TYPE_ATTR,
    Cloud,
    Endpoint,
    HTTPMethod,
    JobState,
    ReportFormat,
    Severity,
)
from helpers.lambda_response import build_response
from helpers.log_helper import get_logger
from helpers.reports import severity_cmp
from helpers.time_helper import utc_iso
from models.job import Job
from services import SP, modular_helpers, obfuscation
from services.job_service import JobService
from services.license_service import LicenseService
from services.metadata import Metadata
from services.modular_helpers import tenant_cloud
from services.platform_service import Platform, PlatformService
from services.report_service import ReportResponse, ReportService
from services.resources import (
    AWSResource,
    AZUREResource,
    CloudResource,
    GOOGLEResource,
    InPlaceResourceView,
    K8SResource,
    ResourceVisitor,
    iter_rule_resource,
)
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

# rule, resource, matched_dto
Payload = tuple[str, CloudResource, dict]


class ResourceMatcher(ResourceVisitor[dict]):
    def __init__(
        self, search_by: dict, exact_match: bool, search_by_all: bool
    ):
        self._search_by = search_by
        self._exact_match = exact_match

        if search_by_all:
            self._match_any = set(map(str, self._search_by.values()))
        else:
            self._match_any = None

    def _does_match(self, provided: Any, real: Any) -> bool:
        if isinstance(real, (list, dict)):
            return False
        if self._exact_match:
            return str(provided) == str(real)
        return str(provided).lower() in str(real).lower()

    @staticmethod
    def _aws_view_map(resource: AWSResource) -> dict:
        keys = {'id': resource.id, 'name': resource.name}
        if resource.arn is not None:
            keys['arn'] = resource.arn
        if resource.date is not None:
            keys['date'] = resource.date_as_utc_iso()
        return ChainMap(keys, resource.data)

    @staticmethod
    def _azure_view_map(resource: AZUREResource) -> dict:
        keys = {'id': resource.id, 'name': resource.name}
        return ChainMap(keys, resource.data)

    @staticmethod
    def _google_view_map(resource: GOOGLEResource) -> dict:
        keys = {'id': resource.id, 'name': resource.name}
        if resource.urn is not None:
            keys['urn'] = resource.urn
        return ChainMap(keys, resource.data)

    @staticmethod
    def _k8s_view_map(resource: K8SResource) -> dict:
        keys = {'id': resource.id, 'name': resource.name}
        if resource.namespace is not None:
            keys['namespace'] = resource.namespace
        return ChainMap(keys, resource.data)

    def _match_by_all(self, dto: dict) -> dict:
        """
        Takes all the values from search_by and tries to match all the keys.
        Returns the first matched
        """
        for key, real in dto.items():
            for provided in self._match_any:
                if self._does_match(provided, real):
                    return {key: real}
        return {}

    def _match(self, dto: dict) -> dict:
        """
        Matches by all the keys and values from search_by
        """
        result = {}
        for key, provided in self._search_by.items():
            real = dto.get(key)
            if not self._does_match(provided, real):
                return {}
            result[key] = real
        return result

    def visitAWSResource(
        self, resource: 'AWSResource', /, *args, **kwargs
    ) -> dict:
        view = self._aws_view_map(resource)
        if self._match_any:
            return self._match_by_all(view)
        else:
            return self._match(view)

    def visitAZUREResource(
        self, resource: 'AZUREResource', /, *args, **kwargs
    ) -> dict:
        view = self._azure_view_map(resource)
        if self._match_any:
            return self._match_by_all(view)
        else:
            return self._match(view)

    def visitGOOGLEResource(
        self, resource: 'GOOGLEResource', /, *args, **kwargs
    ) -> dict:
        view = self._google_view_map(resource)
        if self._match_any:
            return self._match_by_all(view)
        else:
            return self._match(view)

    def visitK8SResource(
        self, resource: 'K8SResource', /, *args, **kwargs
    ) -> dict:
        view = self._k8s_view_map(resource)
        if self._match_any:
            return self._match_by_all(view)
        else:
            return self._match(view)


class MatchedResourcesIterator(Iterator[Payload]):
    def __init__(
        self,
        collection: ShardsCollection,
        cloud: Cloud,
        metadata: Metadata,
        account_id: str = '',
        resource_type: Optional[str] = None,
        region: Optional[str] = None,
        exact_match: bool = True,
        search_by_all: bool = False,
        search_by: Optional[dict] = None,
    ):
        self._collection = collection
        self._cloud = cloud
        self._metadata = metadata
        self._account_id = account_id
        self._resource_type = resource_type
        self._region = region

        if search_by:
            self._matcher = ResourceMatcher(
                search_by=search_by or {},
                exact_match=exact_match,
                search_by_all=search_by_all,
            )
        else:
            self._matcher = None

    @property
    def collection(self) -> ShardsCollection:
        return self._collection

    def __iter__(self):
        self._it = iter_rule_resource(
            collection=self._collection,
            cloud=self._cloud,
            metadata=self._metadata,
            account_id=self._account_id,
            regions=(self._region,) if self._region else None,
            resource_types=(self._resource_type,) if self._resource_type else None,
        )
        return self

    def __next__(self) -> Payload:
        while True:
            rule, res = next(self._it)

            if not self._matcher:
                return rule, res, {}

            # self._matcher exists
            match = res.accept(self._matcher)
            if match:
                return rule, res, match


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
        dictionary_out: dict | None = None,
    ):
        self._it = matched_findings_iterator
        self._entity = entity
        self._full = full
        self._meta = metadata

        self._dictionary_out = dictionary_out

    def _build_rules(self, rules: list[str]) -> list[dict]:
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
        for rule, resource, match_dto in self._it:
            # [rules, match_dto, sync_date]
            inner = datas.setdefault(resource, [[], {}, resource.sync_date])
            inner[0].append(rule)
            inner[1].update(match_dto)
            inner[2] = max(inner[2], resource.sync_date)
        result = []
        identifier = (
            {'account_id': self._entity.project}
            if isinstance(self._entity, Tenant)
            else {'platform_id': self._entity.id}
        )

        view = InPlaceResourceView(self._full)
        for unique, inner in datas.items():
            data = unique.accept(view)
            if self._dictionary_out is not None:
                data = obfuscation.obfuscate_finding(
                    data, self._dictionary_out
                )
                inner[1] = obfuscation.obfuscate_finding(
                    inner[1], self._dictionary_out
                )

            result.append(
                {
                    **identifier,
                    'data': data,
                    'violated_rules': self._build_rules(inner[0]),
                    'matched_by': inner[1],
                    'region': unique.location,
                    'resource_type': unique.resource_type,
                    'last_found': inner[2],
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
        dictionary_out: dict | None = None,
    ):
        self._it = it
        self._full = full
        self._keep_region = keep_region
        self._meta = metadata

        self._dictionary_out = dictionary_out

    def _aggregated(self) -> dict[CloudResource, list]:
        """
        Just makes a mapping of a unique resource to rules it violates
        """
        res = {}
        for rule, resource, _ in self._it:
            inner = res.setdefault(resource, [[], resource.sync_date])
            inner[0].append(rule)
            inner[1] = max(inner[1], resource.sync_date)
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
        view = InPlaceResourceView(self._full)

        i = 0
        for unique, data in aggregated.items():
            rules, ts = data
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

            dto = unique.accept(view)
            if self._dictionary_out is not None:
                dto = obfuscation.obfuscate_finding(dto, self._dictionary_out)

            table.add_cells(CellContent(dto))
            if self._keep_region:
                table.add_cells(CellContent(unique.location))
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
        job_service: JobService,
        tenant_service: TenantService,
        report_service: ReportService,
        platform_service: PlatformService,
        license_service: LicenseService,
    ):
        self._job_service = job_service
        self._tenant_service = tenant_service
        self._report_service = report_service
        self._platform_service = platform_service
        self._ls = license_service

    @property
    def rs(self):
        return self._report_service

    @classmethod
    def build(cls):
        return cls(
            job_service=SP.job_service,
            tenant_service=SP.modular_client.tenant_service(),
            report_service=SP.report_service,
            platform_service=SP.platform_service,
            license_service=SP.license_service,
        )

    @property
    def mapping(self) -> Mapping:
        return {
            Endpoint.REPORTS_RESOURCES_PLATFORMS_K8S_PLATFORM_ID_LATEST: {
                HTTPMethod.GET: self.k8s_platform_get_latest
            },
            Endpoint.REPORTS_RESOURCES_TENANTS_TENANT_NAME_LATEST: {
                HTTPMethod.GET: self.get_latest
            },
            Endpoint.REPORTS_RESOURCES_TENANTS_TENANT_NAME_JOBS: {
                HTTPMethod.GET: self.get_jobs
            },
            Endpoint.REPORTS_RESOURCES_JOBS_JOB_ID: {
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
            metadata=metadata,
            resource_type=event.resource_type,
            exact_match=event.exact_match,
            search_by_all=event.search_by_all,
            search_by=event.extras,
        )
        content = {}
        match event.format:
            case ReportFormat.JSON:
                content = ResourceReportBuilder(
                    matched_findings_iterator=matched,
                    entity=platform,
                    full=event.full,
                    metadata=metadata,
                    dictionary_out=dictionary if event.obfuscated else None,
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
                        dictionary_out=dictionary
                        if event.obfuscated
                        else None,
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
                'Region is provided. Fetching only shard with this region'
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
            metadata=metadata,
            account_id=tenant_item.project,
            resource_type=event.resource_type,
            region=event.region,
            exact_match=event.exact_match,
            search_by_all=event.search_by_all,
            search_by=event.extras,
        )
        content = {}
        match event.format:
            case ReportFormat.JSON:
                content = ResourceReportBuilder(
                    matched_findings_iterator=matched,
                    entity=tenant_item,
                    full=event.full,
                    metadata=metadata,
                    dictionary_out=dictionary if event.obfuscated else None,
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
                    ResourceReportXlsxWriter(
                        it=matched,
                        metadata=metadata,
                        full=event.full,
                        keep_region=True,
                        dictionary_out=dictionary
                        if event.obfuscated
                        else None,
                    ).write(wb=wb, wsh=wb.add_worksheet(tenant_name))
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

        jobs = self._job_service.get_by_tenant_name(
            tenant_name=tenant_name,
            job_types=event.job_types,
            status=JobState.SUCCEEDED,
            start=event.start_iso,
            end=event.end_iso,
        )
        metadata = self._ls.get_customer_metadata(event.customer_id)

        job_response = {}
        for job in jobs:
            if job.is_platform_job:
                continue
            collection = self._report_service.job_collection(
                tenant=tenant_item,
                job=job,
            )
            if event.region:
                _LOG.debug(
                    'Region is provided. Fetching only shard with this region'
                )
                collection.fetch(region=event.region)
            else:
                _LOG.debug('Region is not provided. Fetching all shards')
                collection.fetch_all()
            collection.meta = self._report_service.fetch_meta(tenant_item)
            matched = MatchedResourcesIterator(
                collection=collection,
                cloud=tenant_cloud(tenant_item),
                metadata=metadata,
                account_id=tenant_item.project,
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
                _LOG.debug(f'No resources found for job {job.id}. Skipping')
                continue
            job_response[job] = response
        return build_response(
            content=chain.from_iterable(
                self.dto(j, r) for j, r in job_response.items()
            )
        )

    @validate_kwargs
    def get_specific_job(self, event: ResourceReportJobGetModel, job_id: str):
        cursor = self._job_service.get_by_job_types(
            customer_name=event.customer,
            job_id=job_id,
            job_types=event.job_types,
        )
        job = next(cursor, None)
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

        collection = self._report_service.job_collection(tenant, job)
        if event.region:
            _LOG.debug(
                'Region is provided. Fetching only shard with this region'
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
            metadata=metadata,
            account_id=tenant.project,
            resource_type=event.resource_type,
            region=event.region,
            exact_match=event.exact_match,
            search_by_all=event.search_by_all,
            search_by=event.extras,
        )
        response = ResourceReportBuilder(
            matched_findings_iterator=matched,
            entity=tenant,
            full=event.full,
            metadata=metadata,
            dictionary_out=dictionary if event.obfuscated else None,
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
    def dto(job: Job, response: list) -> list[dict[str, Any]]:
        return [
            {JOB_ID_ATTR: job.id, TYPE_ATTR: job.job_type, **res}
            for res in response
        ]
