from datetime import datetime
from functools import cached_property
from http import HTTPStatus
from typing import Tuple, Set, Iterator, Optional, Any, TypedDict, List, Dict

from modular_sdk.models.tenant import Tenant

from handlers.abstracts.abstract_handler import AbstractHandler
from helpers import build_response
from helpers.constants import AZURE_CLOUD_ATTR, CUSTOMER_ATTR, \
    TENANT_NAME_ATTR, START_ISO_ATTR, END_ISO_ATTR, TYPE_ATTR, \
    ID_ATTR, JOB_ID_ATTR, TENANT_ATTR, HTTPMethod
from helpers.constants import IDENTIFIER_ATTR
from helpers.log_helper import get_logger
from helpers.reports import hashable
from helpers.time_helper import utc_datetime
from models.job import SUBMITTED_AT_ATTR
from services import SERVICE_PROVIDER
from services.ambiguous_job_service import AmbiguousJobService, Source
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService
from services.findings_service import FindingsService
from services.metrics_service import MetricsService, ResourcesGenerator
from services.modular_service import ModularService
from services.report_service import ReportService, DETAILED_REPORT_FILE
from services.rule_meta_service import LazyLoadedMappingsCollector

_LOG = get_logger(__name__)


class MatchedFindingsIterator(Iterator):

    def __init__(self, findings: dict, cloud: str, resource_id: str,
                 exact_match: bool = True, search_by: Optional[set] = None,
                 search_by_all: bool = False,
                 resource_type: Optional[str] = None,
                 region: Optional[str] = None):
        self._findings = findings
        self._cloud = cloud
        self._resource_id = resource_id
        self._exact_match = exact_match

        self._search_by = search_by
        self._search_by_all = search_by_all
        self._resource_type = resource_type
        self._region = region

        self._it = None

    @property
    def findings(self) -> dict:
        return self._findings

    @property
    def resource_id(self) -> str:
        return self._resource_id

    def create_resources_generator(self) -> ResourcesGenerator:
        """
        See metrics_service.create_resources_generator
        :return:
        """
        ms = self.metrics_service
        resources = ms.iter_resources(self._findings)
        if self._cloud != AZURE_CLOUD_ATTR:
            resources = ms.expose_multiregional(resources)
        resources = ms.custom_modify(resources, self._findings)
        # removing duplicates within rule-region
        resources = ms.deduplicated(resources)
        if self._region:
            resources = ms.allow_only_regions(resources, {self._region})
        if self._resource_type:
            resources = ms.allow_only_resource_type(resources, self._findings,
                                                    self._resource_type)
        return resources

    @cached_property
    def mappings_collector(self) -> LazyLoadedMappingsCollector:
        return SERVICE_PROVIDER.mappings_collector()

    @cached_property
    def metrics_service(self) -> MetricsService:
        return SERVICE_PROVIDER.metrics_service()

    def rule_report_fields(self, rule: str) -> Set[str]:
        """
        Fields to search by. If search_by is specified, they will be used.
        In case the parameter is not specified, report_fields will be used.
        In case search_by_all is True, all fields will be checked
        :param rule:
        :return:
        """
        if self._search_by_all:
            return set()
        rf = self._search_by
        if not rf:
            rf = self.mappings_collector.human_data.get(rule, {}).get(
                'report_fields') or set()
        return set(k.lower() for k in rf or set())

    def __iter__(self):
        self._it = self.create_resources_generator()
        return self

    def does_match(self, value: Any) -> bool:
        if self._exact_match:
            return str(self._resource_id) == str(value)
        return str(self._resource_id).lower() in str(value).lower()

    def __next__(self) -> Tuple[str, str, dict, dict]:
        while True:
            rule, region, dto = next(self._it)
            search_by = self.rule_report_fields(rule)
            for key, value in dto.items():  # nested_items(dto)
                if search_by and key.lower() not in search_by:
                    continue  # skipping key and value
                if self.does_match(value):
                    return rule, region, dto, {key: value}


class ViolatedRule(TypedDict):
    name: str
    description: Optional[str]
    severity: str


class ResourceReport(TypedDict):
    account_id: str
    input_identifier: str  # one user provided
    identifier: str  # one that was matched by user's
    last_scan_date: Optional[str]
    violated_rules: List[ViolatedRule]
    data: Dict
    region: str
    resource_type: str


class ResourceReportBuilder:
    def __init__(self, matched_findings_iterator: MatchedFindingsIterator,
                 tenant_item: Tenant, last_scan_date: Optional[str] = None):
        self._it = matched_findings_iterator
        self._tenant_item = tenant_item
        self._last_scan_date = last_scan_date

    @cached_property
    def mappings_collector(self) -> LazyLoadedMappingsCollector:
        return SERVICE_PROVIDER.mappings_collector()

    @cached_property
    def metrics_service(self) -> MetricsService:
        return SERVICE_PROVIDER.metrics_service()

    def _build_rules(self, rules: List[str]) -> List[ViolatedRule]:
        return [{
            'name': rule,
            'description': self._it.findings.get(rule, {}).get('description'),
            'severity': str(
                self.mappings_collector.severity.get(rule) or 'Unknown')
        } for rule in rules]

    def build(self) -> List[ResourceReport]:
        datas = {}
        # the same resources have the same resource_type,
        # region and match_identifier
        for rule, region, dto, match_dto in self._it:
            unique = hashable({
                # 'identifier': next(iter(match_dto.values())),
                'identifier': match_dto,
                'region': region,
                'resource_type': self.metrics_service.adjust_resource_type(
                    self._it.findings.get(rule, {}).get('resourceType', '')
                )
            })
            inner = datas.setdefault(unique, {'data': {}, 'rules': []})
            inner['data'].update(dto)
            inner['rules'].append(rule)
        result = []
        for unique, inner in datas.items():
            result.append({
                'account_id': self._tenant_item.project,
                'input_identifier': self._it.resource_id,
                'last_scan_date': self._last_scan_date,
                'data': inner['data'],
                **unique,  # identifier, region & resource_type
                'violated_rules': self._build_rules(inner['rules'])
            })
        return result


class ResourceReportHandler(AbstractHandler):
    def __init__(self, ambiguous_job_service: AmbiguousJobService,
                 modular_service: ModularService,
                 report_service: ReportService,
                 findings_service: FindingsService,
                 metrics_service: MetricsService,
                 mappings_collector: LazyLoadedMappingsCollector,
                 s3_client: S3Client,
                 environment_service: EnvironmentService):
        self._ambiguous_job_service = ambiguous_job_service
        self._modular_service = modular_service
        self._report_service = report_service
        self._findings_service = findings_service
        self._metrics_service = metrics_service
        self._mappings_collector = mappings_collector
        self._s3_client = s3_client
        self._environment_service = environment_service

    @property
    def rs(self):
        return self._report_service

    @property
    def ajs(self):
        return self._ambiguous_job_service

    @classmethod
    def build(cls):
        return cls(
            ambiguous_job_service=SERVICE_PROVIDER.ambiguous_job_service(),
            modular_service=SERVICE_PROVIDER.modular_service(),
            report_service=SERVICE_PROVIDER.report_service(),
            findings_service=SERVICE_PROVIDER.findings_service(),
            metrics_service=SERVICE_PROVIDER.metrics_service(),
            mappings_collector=SERVICE_PROVIDER.mappings_collector(),
            s3_client=SERVICE_PROVIDER.s3(),
            environment_service=SERVICE_PROVIDER.environment_service()
        )

    def define_action_mapping(self) -> dict:
        return {
            '/reports/resources/tenants/{tenant_name}/state/latest': {
                HTTPMethod.GET: self.get_latest
            },
            '/reports/resources/tenants/{tenant_name}/jobs': {
                HTTPMethod.GET: self.get_jobs
            },
            '/reports/resources/jobs/{id}': {
                HTTPMethod.GET: self.get_specific_job
            }
        }

    def get_latest(self, event: dict) -> dict:
        tenant_name = event.get(TENANT_NAME_ATTR)
        customer = event.get(CUSTOMER_ATTR)
        resource_id = event.get(IDENTIFIER_ATTR)  # can be literally anything
        exact_match: bool = event.get('exact_match')
        search_by = event.get('search_by')
        search_by_all = event.get('search_by_all')
        resource_type = event.get('resource_type')
        region = event.get('region')

        tenant_item = self._modular_service.get_tenant(tenant_name)
        self._modular_service.assert_tenant_valid(tenant_item, customer)
        findings = self._findings_service.get_findings_content(
            tenant_item.project)
        matched = MatchedFindingsIterator(
            findings=findings, cloud=tenant_item.cloud,
            resource_id=resource_id, exact_match=exact_match,
            search_by=search_by, search_by_all=search_by_all,
            resource_type=resource_type, region=region
        )
        last_scan_date = self._findings_service.get_latest_findings_key(
            tenant_item.project)
        _LOG.debug(f'Last scan date: {last_scan_date}')
        if not last_scan_date:
            _LOG.warning('Something is wrong with last_scan_date')
        else:
            last_scan_date = last_scan_date.strip('/').split('/')[1]
        _LOG.debug(f'Last scan date after processing {last_scan_date}')
        response = ResourceReportBuilder(
            matched_findings_iterator=matched,
            tenant_item=tenant_item,
            last_scan_date=last_scan_date
        ).build()
        return build_response(content=response)

    def get_jobs(self, event: dict) -> dict:
        customer = event.get(CUSTOMER_ATTR)
        tenant_name = event[TENANT_NAME_ATTR]
        start_iso: datetime = event[START_ISO_ATTR]
        end_iso: datetime = event[END_ISO_ATTR]
        # href = event.get(HREF_ATTR)
        typ = event.get(TYPE_ATTR)

        resource_id = event.get(IDENTIFIER_ATTR)  # can be literally anything
        exact_match: bool = event.get('exact_match')
        search_by = event.get('search_by')
        search_by_all = event.get('search_by_all')
        resource_type = event.get('resource_type')
        region = event.get('region')

        tenant_item = self._modular_service.get_tenant(tenant_name)
        self._modular_service.assert_tenant_valid(tenant_item, customer)

        typ_params_map = self._ambiguous_job_service.derive_typ_param_map(
            typ=typ, tenants=[tenant_item.name], cloud_ids=[]
        )
        source_list = self._ambiguous_job_service.batch_list(
            typ_params_map=typ_params_map, customer=customer,
            start=start_iso, end=end_iso, sort=True
        )
        if not source_list:
            return build_response(code=HTTPStatus.NOT_FOUND,
                                  content='No jobs found')
        key_source = {
            self.rs.derive_job_object_path(
                self.ajs.get_attribute(item, ID_ATTR),
                DETAILED_REPORT_FILE): item for item in source_list
        }  # sorted
        gen = self._s3_client.get_json_batch(
            bucket_name=self._environment_service.default_reports_bucket_name(),
            keys=key_source.keys()
        )  # not sorted
        key_response = {}
        for key, detailed in gen:
            findings = self.rs.derive_findings_from_report(detailed, True)
            matched = MatchedFindingsIterator(
                findings=findings.serialize(), cloud=tenant_item.cloud,
                resource_id=resource_id, exact_match=exact_match,
                search_by=search_by, search_by_all=search_by_all,
                resource_type=resource_type, region=region
            )
            sa = self.ajs.get_attribute(key_source[key], SUBMITTED_AT_ATTR)
            response = ResourceReportBuilder(
                matched_findings_iterator=matched,
                tenant_item=tenant_item,
                last_scan_date=utc_datetime(sa).date().isoformat()
            ).build()
            if not response:
                _LOG.info(f'No resources found for job {key}. Skipping')
                continue
            key_response[key] = response
        result = []
        for key, resources in key_response.items():
            result.extend(self.dto(key_source.get(key), resources))
        return build_response(content=result)

    def get_specific_job(self, event: dict) -> dict:
        job_id: str = event[ID_ATTR]
        typ: str = event[TYPE_ATTR]
        customer = event.get(CUSTOMER_ATTR)

        resource_id = event.get(IDENTIFIER_ATTR)  # can be literally anything
        exact_match: bool = event.get('exact_match')
        search_by = event.get('search_by')
        search_by_all = event.get('search_by_all')
        resource_type = event.get('resource_type')
        region = event.get('region')

        source = self.ajs.get(job_id, typ)
        if not source:
            return build_response(content=f'Job {job_id} not found',
                                  code=HTTPStatus.NOT_FOUND)
        tenant_item = self._modular_service.get_tenant(self.ajs.get_attribute(
            source, TENANT_ATTR))
        self._modular_service.assert_tenant_valid(tenant_item, customer)

        key = self.rs.derive_job_object_path(
            self.ajs.get_attribute(source, ID_ATTR), DETAILED_REPORT_FILE)
        detailed = self.rs.pull_job_report(key)
        findings = self.rs.derive_findings_from_report(detailed, True)
        matched = MatchedFindingsIterator(
            findings=findings.serialize(), cloud=tenant_item.cloud,
            resource_id=resource_id, exact_match=exact_match,
            search_by=search_by, search_by_all=search_by_all,
            resource_type=resource_type, region=region
        )
        sa = self.ajs.get_attribute(source, SUBMITTED_AT_ATTR)
        response = ResourceReportBuilder(
            matched_findings_iterator=matched,
            tenant_item=tenant_item,
            last_scan_date=utc_datetime(sa).date().isoformat()
        ).build()
        return build_response(content=self.dto(source, response))

    def dto(self, source: Source, response: List[Dict]) -> List[dict]:
        return [{
            JOB_ID_ATTR: self.ajs.get_attribute(item=source, attr=ID_ATTR),
            TYPE_ATTR: self.ajs.get_type(source),
            SUBMITTED_AT_ATTR: self.ajs.get_attribute(source,
                                                      SUBMITTED_AT_ATTR),
            **res,

        } for res in response]
