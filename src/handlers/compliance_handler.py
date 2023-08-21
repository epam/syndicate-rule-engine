from typing import Optional

from handlers.base_handler import \
    BaseReportHandler, SourceReportDerivation, \
    Report, ModularService, AmbiguousJobService, Source
from helpers import RESPONSE_RESOURCE_NOT_FOUND_CODE, RESPONSE_OK_CODE
from helpers.constants import (
    GET_METHOD, CUSTOMER_ATTR, TENANTS_ATTR,
    TENANT_ATTR, HREF_ATTR, CONTENT_ATTR, ID_ATTR, AWS_CLOUD_ATTR,
    AZURE_CLOUD_ATTR)
from helpers.log_helper import get_logger
from services.coverage_service import CoverageService, RegionPoints
from services.findings_service import FindingsService
from services.modular_service import Tenant
from services.report_service import ReportService, DETAILED_REPORT_FILE

DEFAULT_UNRESOLVABLE_RESPONSE = 'Request has run into an unresolvable issue.'

TYPE_ATTR = 'type'
JOB_ID_ATTR = 'job_id'

ENTITY_ATTR_KEY = 'entity_attr'
ENTITY_VALUE_KEY = 'entity_value'
STANDARDS_COVERAGE_KEY = 'standards_coverage'

TENANT_NAME_ATTR = 'tenant_name'
TENANT_DISPLAY_NAME_ATTR = 'tenant_display_name'

REGIONS_TO_INCLUDE_ATTR = 'regions_to_include'
REGIONS_TO_EXCLUDE_ATTR = 'regions_to_exclude'
ACTIVE_ONLY_ATTR = 'active_only'

_LOG = get_logger(__name__)

# Done
JOB_ENDPOINT = '/reports/compliance/jobs/{id}'
TENANT_ENDPOINT = '/reports/compliance/tenants/{tenant_name}'

# Scrapped, due to attribute naming inconsistency

NO_RESOURCES_FOR_REPORT = ' maintain(s) no resources to derive a report.'


class BaseComplianceReportHandler(BaseReportHandler):
    """
    Provides base behaviour of compliance-reporting, establishing
     report-derivation function.
    """

    def __init__(self, ambiguous_job_service: AmbiguousJobService,
                 modular_service: ModularService,
                 report_service: ReportService,
                 coverage_service: CoverageService):
        super().__init__(
            ambiguous_job_service=ambiguous_job_service,
            modular_service=modular_service,
            report_service=report_service,
        )
        self._coverage_service = coverage_service

    def _compliance_per_source_derivation(self, source: Source, **kwargs
                                          ) -> Optional[Report]:

        """
        Obtains compliance report of a sourced job, returning a
        respective detailed-report.
        :param source: Source
        :param kwargs: Dict, maintains:
            - `href` attribute, denoting demand for hypertext reference.
            - `standards_coverage`, providing cloud-respective standard data.

        :return: Optional[Report]
        """
        jid = self._ambiguous_job_service.get_attribute(source, ID_ATTR)
        href = kwargs.get(HREF_ATTR)
        tenant: Tenant = kwargs.get('tenant')
        rs = self._report_service
        cs = self._coverage_service

        detailed_path = rs.derive_job_object_path(job_id=jid,
                                                  typ=DETAILED_REPORT_FILE)
        xlsx_compliance_path = rs.derive_compliance_report_object_path(
            job_id=jid, fext='xlsx'
        )

        _head = f'Job:\'{jid}\''

        ref = None
        if href:
            _LOG.info(_head + ' going to obtain hypertext reference'
                              'of the compliance report.')
            ref = rs.href_concrete_report(
                path=xlsx_compliance_path, check=True
            )
            if ref:
                return ref

        # Maintains raw data, from now on.
        message = ' pulling detailed report for compliance derivation.'
        _LOG.warning(_head + message)
        detailed = self._report_service.pull_job_report(path=detailed_path)
        if detailed:
            points: RegionPoints = cs.derive_points_from_detailed_report(
                detailed_report=detailed
            )
            if tenant.cloud == AWS_CLOUD_ATTR:
                points = cs.distribute_multiregion(points)
            elif tenant.cloud == AZURE_CLOUD_ATTR:
                points = cs.congest_to_multiregion(points)
            if points:
                message = ' deriving compliance coverage of points.'
                _LOG.info(_head + message)
                ref = cs.calculate_region_coverages(
                    points=points, cloud=tenant.cloud
                )
            if ref and href:
                # todo retain .json as well, for faster derivation.
                _LOG.warning(_head + ' deriving xlsx compliance report.')
                file_name = rs.derive_name_of_report_object_path(
                    object_path=xlsx_compliance_path
                )
                path = rs.derive_compliance_report_excel_path(
                    file_name=file_name, coverages=ref,
                    standards_coverage=cs.standards_coverage(tenant.cloud)
                )
                if path:
                    _LOG.warning(_head + ' patching compliance-report.')
                    if rs.put_path_retained_concrete_report(stream_path=path,
                                                            object_path=xlsx_compliance_path) is None:
                        message = ' compliance-report could not be patched.'
                        _LOG.warning(_head + message)
                        # Explicitly denote reference absence.
                        ref = None

        if href and ref:
            # Pull after successful patch.
            ref = self._report_service.href_job_report(
                path=xlsx_compliance_path, check=False
            )

        return ref


class JobsComplianceHandler(BaseComplianceReportHandler):

    def define_action_mapping(self):
        return {
            JOB_ENDPOINT: {
                GET_METHOD: self.get_by_job
            }
        }

    @property
    def _source_report_derivation_function(self) -> SourceReportDerivation:
        return self._compliance_per_source_derivation

    def get_by_job(self, event: dict):
        _LOG.info(f'GET Job Details Report(s) - {event}.')
        # Note that `job_id` denotes the primary-key's hash-key of entities.
        uid: str = event[ID_ATTR]
        typ: str = event[TYPE_ATTR]
        customer = event.get(CUSTOMER_ATTR)
        tenants = event.get(TENANTS_ATTR)
        href = event.get(HREF_ATTR)

        head = f'{typ.capitalize()} Job:\'{uid}\''

        source = self._attain_source(
            uid=uid, typ=typ, customer=customer, tenants=tenants
        )
        if not source:
            return self.response

        source_tenant = self._ambiguous_job_service.get_attribute(
            item=source, attr=TENANT_ATTR
        )
        tenant_item = self._attain_tenant(
            name=source_tenant, customer=customer, active=True
        )
        if not tenant_item:
            return self.response
        _LOG.info(head + ' obtaining source to standards coverage mapping.')

        referenced_reports = self._attain_source_report_map(
            source_list=[source, ], tenant=tenant_item,
            href=href,
        )
        if referenced_reports:
            ref_attr = HREF_ATTR if href else CONTENT_ATTR
            self._code = RESPONSE_OK_CODE
            self._content = [
                self.dto(source=source, report=report, ref_attr=ref_attr)
                for source, report in referenced_reports.items()
            ]
        else:
            message = f' - no report could be derived.'
            _LOG.warning(head + message)
            self._code = RESPONSE_RESOURCE_NOT_FOUND_CODE
            self._content = head + NO_RESOURCES_FOR_REPORT

        return self.response

    def dto(self, source: Source, report: Report, ref_attr: str):
        return {
            ID_ATTR: self._ambiguous_job_service.get_attribute(
                item=source, attr=ID_ATTR
            ),
            TYPE_ATTR: self._ambiguous_job_service.get_type(item=source),
            ref_attr: report
        }


class EntityComplianceHandler(BaseComplianceReportHandler):

    def __init__(self, ambiguous_job_service: AmbiguousJobService,
                 modular_service: ModularService,
                 report_service: ReportService,
                 findings_service: FindingsService,
                 coverage_service: CoverageService):
        super().__init__(
            ambiguous_job_service=ambiguous_job_service,
            modular_service=modular_service,
            report_service=report_service,
            coverage_service=coverage_service
        )
        self._findings_service = findings_service

    def define_action_mapping(self):
        return {
            TENANT_ENDPOINT: {
                GET_METHOD: self.get_by_tenant
            }
        }

    def get_by_tenant(self, event: dict):
        _LOG.info(f'GET compliance Report(s) of a Tenant - {event}.')
        # `tenant` has been injected via the restriction service.
        tenant_name = event[TENANT_ATTR]

        head = f'Tenant:\'{tenant_name}\''

        href = event.get(HREF_ATTR)
        customer = event.get(CUSTOMER_ATTR)
        tenant = self._attain_tenant(
            name=tenant_name, customer=customer, active=True
        )
        if not tenant:
            return self.response

        # todo, coverage for multiple tenants ?
        report = self._compliance_per_tenant_derivation(
            tenant=tenant,
            href=href,
        )

        if report:
            ref_attr = HREF_ATTR if href else CONTENT_ATTR
            self._code = RESPONSE_OK_CODE
            self._content = [
                self.dto(
                    entity_attr=TENANT_ATTR, entity_value=tenant_name,
                    report=report, ref_attr=ref_attr
                )
            ]
        else:
            message = f' - accumulated report could not be derived.'
            _LOG.warning(head + message)
            self._code = RESPONSE_RESOURCE_NOT_FOUND_CODE
            self._content = head + NO_RESOURCES_FOR_REPORT

        return self.response

    def _compliance_per_tenant_derivation(self, tenant: Tenant,
                                          **kwargs: dict) -> Optional[Report]:

        """
        Obtains compliance report of an Account, returning a respective report.
        :param account: Account
        :param:
        :param kwargs: Dict, maintains:
            - `href`: bool, denotes demand for hypertext reference.
            - `regions_to_include`: List[str], denotes regions to target.
            - `regions_to_exclude`: List[str], denotes regions to omit.
            - `active_only`: bool = True , denotes to use active regions only
        :return: Optional[Report]
        """

        cid = tenant.project
        href = kwargs.get(HREF_ATTR)
        req_regions = kwargs.get(REGIONS_TO_INCLUDE_ATTR)
        omit_regions = kwargs.get(REGIONS_TO_EXCLUDE_ATTR)
        active_only = kwargs.get(ACTIVE_ONLY_ATTR, True)

        rs = self._report_service
        cs = self._coverage_service
        fs = self._findings_service

        head = f'Tenant:\'{cid}\''

        active_regions = None

        if active_only:
            active_regions = self._modular_service.get_tenant_regions(tenant)

            if omit_regions:
                message = f' filtering out requested regions - '
                message += ', '.join(omit_regions)
                _LOG.info(head + message)
                active_regions = list(set(active_regions) - set(omit_regions))

            if req_regions:
                message = f' retaining active requested regions - '
                message += ', '.join(req_regions)
                _LOG.info(head + message)
                active_regions = list(set(req_regions) & set(active_regions))

            if not active_regions:
                _LOG.warning(head + ' no active regions are available.')
                return

        xlsx_compliance_path = rs.derive_compliance_report_object_path(
            entity_attr=TENANT_ATTR, entity_value=cid, fext='xlsx',
        )

        # todo given Entity Based Compliance is dynamic, i.e.,
        #  changes with time - ergo "retain-any-new, then-pull" won't work
        #  Consider `select_object_content` for query-like requests.

        # Dynamically generates compliance.

        message = ' pulling findings content. '
        _LOG.warning(head + message)
        ref = {}
        findings: Optional[dict] = fs.get_findings_content(identifier=cid)
        if findings:
            message = ' deriving points based on'
            if active_only:
                message += f' active region(s): {", ".join(active_regions)}'
                message += ' within'
            message += ' findings.'

            _LOG.info(head + message)
            # Given active_only==False, active_regions=None, ergo ignored.
            points: RegionPoints = cs.derive_points_from_findings(
                findings=findings, regions=active_regions
            )
            if tenant.cloud == AWS_CLOUD_ATTR:
                points = cs.distribute_multiregion(points)
            elif tenant.cloud == AZURE_CLOUD_ATTR:
                points = cs.congest_to_multiregion(points)
            if points:
                message = ' deriving compliance coverage of points.'
                _LOG.info(head + message)
                ref = cs.calculate_region_coverages(
                    points=points, cloud=tenant.cloud
                )
            if ref and href:
                _LOG.info(head + ' deriving xlsx compliance report.')
                file_name = rs.derive_name_of_report_object_path(
                    object_path=xlsx_compliance_path)
                path = rs.derive_compliance_report_excel_path(
                    file_name=file_name, coverages=ref,
                    standards_coverage=cs.standards_coverage(tenant.cloud)
                )
                if path:
                    message = ' generating compliance hypertext reference.'
                    _LOG.warning(head + message)
                    if rs.put_path_retained_concrete_report(
                            stream_path=path, object_path=xlsx_compliance_path
                    ) is None:
                        message = ' compliance-report hypertext could not'
                        message += ' be provided.'
                        _LOG.warning(head + message)
                        # Explicitly denote reference absence.
                        ref = None

        if href and ref:
            ref = self._report_service.href_job_report(
                path=xlsx_compliance_path, check=False
            )

        return ref

    @staticmethod
    def dto(entity_attr: str, entity_value: str,
            report: Report, ref_attr: str):
        return {
            entity_attr: entity_value,
            ref_attr: report
        }
