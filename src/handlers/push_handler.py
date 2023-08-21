from datetime import datetime
from itertools import chain
from pathlib import PurePosixPath
from typing import Optional, Dict, List, Iterable

import requests
from modular_sdk.commons.constants import TENANT_PARENT_MAP_SIEM_DEFECT_DOJO_TYPE
from modular_sdk.services.impl.maestro_credentials_service import \
    DefectDojoApplicationMeta, DefectDojoApplicationSecret
from models.modular.tenants import Tenant
from models.modular.parents import DefectDojoParentMeta
from handlers.base_handler import \
    BaseReportHandler, Source, AmbiguousJobService, \
    ModularService, ReportService, \
    SourceReportDerivation
from helpers import RESPONSE_RESOURCE_NOT_FOUND_CODE, RESPONSE_OK_CODE, \
    build_response, RESPONSE_BAD_REQUEST_CODE
from helpers.constants import CUSTOMER_ATTR, TENANT_ATTR, TENANTS_ATTR, \
    POST_METHOD, ID_ATTR, TYPE_ATTR, START_ISO_ATTR, END_ISO_ATTR, \
    JOB_ID_ATTR
from helpers.log_helper import get_logger
from integrations.defect_dojo_adapter import DefectDojoAdapter
from integrations.security_hub_adapter import SecurityHubAdapter
from services.clients.ssm import SSMClient
from services.report_service import DETAILED_REPORT_FILE

_LOG = get_logger(__name__)

TENANTS_TO_SKIP = 'tenants_to_skip'


class SiemPushHandler(BaseReportHandler):
    _source_report_derivation_attr: Optional[SourceReportDerivation]

    def __init__(self, ambiguous_job_service: AmbiguousJobService,
                 modular_service: ModularService, report_service: ReportService,
                 ssm_client: SSMClient):
        super().__init__(
            ambiguous_job_service=ambiguous_job_service,
            modular_service=modular_service,
            report_service=report_service
        )
        self._ssm_client = ssm_client

    def _reset(self):
        super()._reset()
        self._source_report_derivation_attr = None

    @property
    def _source_report_derivation_function(self) -> SourceReportDerivation:
        return self._source_report_derivation_attr

    @property
    def ajs(self) -> AmbiguousJobService:
        return self._ambiguous_job_service

    def _dojo_report_sourced_derivation(self, source: Source, **kwargs):
        """
        Obtains dojo-compatible report, based on a given source, be it:
         - manual Job
         - event-driven Processing
        :parameter source: Source=Union[Job, BatchResult]
        :parameter kwargs: Dict
        :parameter: List[Dict]
        """
        ref = None
        ajs = self._ambiguous_job_service
        rs = self._report_service
        path = rs.derive_job_object_path(
            job_id=self._ambiguous_job_service.get_attribute(source, ID_ATTR),
            typ=DETAILED_REPORT_FILE
        )

        _typ = ajs.get_type(item=source)
        _uid = ajs.get_attribute(item=source, attr=ID_ATTR)
        _tn = ajs.get_attribute(item=source, attr=TENANT_ATTR)
        _cn = ajs.get_attribute(item=source, attr=CUSTOMER_ATTR)
        head = f'{_typ.capitalize()} Job:\'{_uid}\' of \'{_tn}\' tenant'

        if _tn in kwargs.get(TENANTS_TO_SKIP, []):
            _LOG.warning(head + ' is set to skip, due to adapter issues.')
            return ref

        _LOG.info(head + ' pulling formatted, detailed report.')
        detailed_report = rs.pull_job_report(path=path)
        if detailed_report:
            _LOG.info(head + ' preparing dojo compatible report of policies.')
            dojo_policy_reports = rs.formatted_to_dojo_policy_report(
                detailed_report=detailed_report
            )
            if dojo_policy_reports:
                ref = dojo_policy_reports

        return ref

    def _download_finding_for_one_job(self, job_id: str,
                                      reports_bucket_name: str
                                      ) -> Iterable[Dict]:
        findings_key = str(PurePosixPath(job_id, 'findings'))
        s3 = self._report_service.s3_client
        keys = (
            k for k in s3.list_dir(reports_bucket_name, findings_key)
            if k.endswith('.json') or k.endswith('.json.gz')
        )
        return chain.from_iterable(
            pair[1] for pair in s3.get_json_batch(reports_bucket_name, keys)
        )

    def _download_findings(self, sources: List[Source],
                           reports_bucket_name: str) -> Dict[Source, List]:
        gens = {
            source: self._download_finding_for_one_job(
                self.ajs.get_attribute(source, ID_ATTR), reports_bucket_name)
            for source in sources
        }
        return {source: list(findings) for source, findings in gens.items()}

    def _attain_sources_to_push(
            self, start_iso: datetime, end_iso: datetime,
            customer: Optional[str] = None,
            tenants: Optional[List[str]] = None,
            cloud_ids: Optional[List[str]] = None,
            account_dn: Optional[str] = None,
            typ: Optional[str] = None
    ) -> Optional[List[Source]]:

        cloud_ids = cloud_ids or []
        ajs = self._ambiguous_job_service

        head = f'Account:\'{account_dn}\'' if account_dn else ''

        # Log-Header.
        if tenants:
            multiple = len(tenants) > 1
            bind = ', '.join(map("'{}'".format, tenants or []))
            if head:
                bind = f', bound to {bind}'
            head += f'{bind} tenant'
            if multiple:
                head += 's'

        if customer:
            head = 'Tenants' if not head else head
            head += f' of \'{customer}\' customer'

        typ_scope = f'{typ} type' if typ else 'all types'
        time_scope = f'from {start_iso.isoformat()} till {end_iso.isoformat()}'
        job_scope = f'job(s) of {typ_scope}, {time_scope}'

        # todo Responsibility chain

        _LOG.info(f'Obtaining {job_scope}, for {head or "tenants"}.')
        head = head or 'Tenants'
        typ_params_map = ajs.derive_typ_param_map(
            typ=typ, tenants=tenants,
            cloud_ids=cloud_ids
        )
        sources = ajs.batch_list(
            typ_params_map=typ_params_map, customer=customer,
            start=start_iso, end=end_iso, sort=True
        )
        if not sources:
            message = f' - no source-data of {job_scope} could be derived.'
            _LOG.warning(head + message)
            self._code = RESPONSE_RESOURCE_NOT_FOUND_CODE
            self._content = head + message

        return sources

    def define_action_mapping(self) -> dict:
        return {
            '/reports/push/dojo/{job_id}': {
                POST_METHOD: self.push_dojo_by_job_id
            },
            '/reports/push/security-hub/{job_id}': {
                POST_METHOD: self.push_security_hub_by_job_id
            },
            '/reports/push/dojo': {
                POST_METHOD: self.push_dojo_multiple_jobs
            },
            '/reports/push/security-hub': {
                POST_METHOD: self.push_security_hub_multiple_jobs
            }
        }

    def initialize_dojo_adapter(self, tenant: Tenant) -> DefectDojoAdapter:
        _not_configured = lambda: build_response(
            code=RESPONSE_BAD_REQUEST_CODE,
            content=f'Tenant {tenant.name} does not have dojo configuration'
        )
        parent = self._modular_service.get_tenant_parent(
            tenant, TENANT_PARENT_MAP_SIEM_DEFECT_DOJO_TYPE
        )
        if not parent:
            _LOG.debug('Parent does not exist')
            return _not_configured()
        parent_meta = DefectDojoParentMeta.from_dict(parent.meta.as_dict())
        application = self._modular_service.get_parent_application(parent)
        if not application or not application.secret:
            _LOG.debug('Application or application.secret do not exist')
            return _not_configured()
        raw_secret = self._modular_service.modular_client.assume_role_ssm_service().get_parameter(application.secret)
        if not raw_secret or not isinstance(raw_secret, dict):
            _LOG.debug(f'SSM Secret by name {application.secret} not found')
            return _not_configured()
        meta = DefectDojoApplicationMeta.from_dict(application.meta.as_dict())
        secret = DefectDojoApplicationSecret.from_dict(raw_secret)
        try:
            _LOG.info('Initializing dojo client')
            return DefectDojoAdapter(
                host=meta.url,
                api_key=secret.api_key,
                entities_mapping=parent_meta.entities_mapping,
                display_all_fields=parent_meta.display_all_fields,
                upload_files=parent_meta.upload_files,
                resource_per_finding=parent_meta.resource_per_finding
            )
        except requests.RequestException as e:
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE,
                content=f'Could not init dojo client: {e}'
            )

    def push_dojo_by_job_id(self, event: dict) -> dict:
        job_id = event[JOB_ID_ATTR]
        customer = event.get(CUSTOMER_ATTR)
        tenants = event.get(TENANTS_ATTR) or []
        item = self._attain_source(
            uid=job_id,
            customer=customer,
            tenants=tenants
        )
        if not item:
            return self.response
        # retrieve config
        tenant_name = self.ajs.get_attribute(item, TENANT_ATTR)
        tenant = self._attain_tenant(
            name=tenant_name, customer=customer, active=True)
        if not tenant:
            return self.response

        adapter = self.initialize_dojo_adapter(tenant)

        report = self._dojo_report_sourced_derivation(item)
        if not report:
            return build_response(code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                                  content='Could not retrieve job report')
        adapter.add_entity(
            job_id=self.ajs.get_attribute(item=item, attr=ID_ATTR),
            started_at=self.ajs.get_attribute(item, 'started_at'),
            stopped_at=self.ajs.get_attribute(item, 'stopped_at'),
            tenant_display_name=self.ajs.get_attribute(item=item,
                                                       attr=TENANT_ATTR),
            customer_display_name=self.ajs.get_attribute(item=item,
                                                         attr=CUSTOMER_ATTR),
            policy_reports=report,
            job_type=self.ajs.get_type(item=item)
        )
        _LOG.info('Uploading entity to DOJO')
        result = adapter.upload_all_entities()[0]  # always one here

        return build_response(code=result['status'], content=result)

    def push_security_hub_by_job_id(self, event: dict) -> dict:
        job_id = event[JOB_ID_ATTR]
        customer = event.get(CUSTOMER_ATTR)
        tenants = event.get(TENANTS_ATTR) or []
        item = self._attain_source(
            uid=job_id,
            customer=customer,
            tenants=tenants
        )
        if not item:
            return self.response
        tenant_name = self.ajs.get_attribute(item, TENANT_ATTR)
        tenant = self._attain_tenant(
            name=tenant_name, customer=customer, active=True)
        if not tenant:
            return self.response
        _not_configured = lambda: build_response(
            code=RESPONSE_BAD_REQUEST_CODE,
            content=f'Tenant {tenant_name} does not have SH configuration'
        )
        application = self._modular_service.get_application('mock')  # AWS_ROLE
        if not application:
            return _not_configured()
        mcs = self._modular_service.modular_client.maestro_credentials_service()
        creds = mcs.get_by_application(application)
        if not creds:
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE,
                content='Cannot get credentials to push to SH'
            )
        findings = self._download_finding_for_one_job(
            job_id=self.ajs.get_attribute(item, ID_ATTR),
            reports_bucket_name=self._report_service.job_report_bucket
        )
        adapter = SecurityHubAdapter(
            aws_region=creds.AWS_DEFAULT_REGION,
            product_arn='',
            aws_access_key_id=creds.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=creds.AWS_SECRET_ACCESS_KEY,
            aws_session_token=creds.AWS_SESSION_TOKEN,
            aws_default_region=creds.AWS_DEFAULT_REGION
        )
        adapter.add_entity(
            job_id=self.ajs.get_attribute(item, ID_ATTR),
            job_type=self.ajs.get_type(item=item),
            findings=list(findings)
        )
        _LOG.info('Uploading entity to Security Hub')
        result = adapter.upload_all_entities()[0]  # always one here

        return build_response(code=result['status'], content=result)

    def push_dojo_multiple_jobs(self, event: dict) -> dict:
        tenant = event[TENANT_ATTR]
        _type = event.get(TYPE_ATTR)
        customer = event.get(CUSTOMER_ATTR)
        start_iso: datetime = event.get(START_ISO_ATTR)
        end_iso: datetime = event.get(END_ISO_ATTR)
        sources = self._attain_sources_to_push(
            start_iso=start_iso, end_iso=end_iso,
            customer=customer, tenants=[tenant],
            typ=_type
        )
        if not sources:
            return self.response

        tenant_item = self._attain_tenant(
            name=tenant, customer=customer, active=True
        )
        if not tenant_item:
            return self.response
        adapter = self.initialize_dojo_adapter(tenant_item)
        self._source_report_derivation_attr = self._dojo_report_sourced_derivation

        source_to_reports: Dict[Source, List] = self._attain_source_report_map(
            source_list=sources
        )
        if not source_to_reports:
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content='No reports found'
            )
        self._source_report_derivation_attr = None

        for source, reports in source_to_reports.items():
            adapter.add_entity(
                job_id=self.ajs.get_attribute(item=source, attr=ID_ATTR),
                started_at=self.ajs.get_attribute(source, 'started_at'),
                stopped_at=self.ajs.get_attribute(source, 'stopped_at'),
                tenant_display_name=tenant,
                customer_display_name=self.ajs.get_attribute(item=source,
                                                             attr=CUSTOMER_ATTR),
                policy_reports=reports,
                job_type=self.ajs.get_type(item=source)
            )
        self._content = adapter.upload_all_entities()
        self._code = RESPONSE_OK_CODE
        return self.response

    def push_security_hub_multiple_jobs(self, event: dict) -> dict:
        tenant = event[TENANT_ATTR]
        _type = event.get(TYPE_ATTR)
        customer = event.get(CUSTOMER_ATTR)
        start_iso: datetime = event.get(START_ISO_ATTR)
        end_iso: datetime = event.get(END_ISO_ATTR)
        sources = self._attain_sources_to_push(
            start_iso=start_iso, end_iso=end_iso,
            customer=customer, tenants=[tenant],
            typ=_type
        )
        if not sources:
            return self.response

        tenant_item = self._attain_tenant(
            name=tenant, customer=customer, active=True
        )
        if not tenant_item:
            return self.response
        _not_configured = lambda: build_response(
            code=RESPONSE_BAD_REQUEST_CODE,
            content=f'Tenant {tenant} does not have SH configuration'
        )
        application = self._modular_service.get_application('mock')
        if not application:
            return _not_configured()
        mcs = self._modular_service.modular_client.maestro_credentials_service()
        creds = mcs.get_by_application(application)
        if not creds:
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE,
                content='Cannot get credentials to push to SH'
            )
        adapter = SecurityHubAdapter(
            aws_region=creds.AWS_DEFAULT_REGION,
            product_arn='',  # TODO from parent ?
            aws_access_key_id=creds.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=creds.AWS_SECRET_ACCESS_KEY,
            aws_session_token=creds.AWS_SESSION_TOKEN,
            aws_default_region=creds.AWS_DEFAULT_REGION
        )
        reports_bucket_name = self._report_service.job_report_bucket
        source_findings = self._download_findings(sources, reports_bucket_name)
        for source, findings in source_findings.items():
            adapter.add_entity(
                job_id=self.ajs.get_attribute(item=source, attr=ID_ATTR),
                job_type=self.ajs.get_type(item=source),
                findings=findings
            )
        self._content = adapter.upload_all_entities()
        self._code = RESPONSE_OK_CODE
        return self.response
