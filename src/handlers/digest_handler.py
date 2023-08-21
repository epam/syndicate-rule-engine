from datetime import datetime
from typing import List, Optional, Dict

from helpers import RESPONSE_RESOURCE_NOT_FOUND_CODE, RESPONSE_OK_CODE
from helpers.constants import (
    GET_METHOD, CUSTOMER_ATTR, TENANTS_ATTR, TENANT_ATTR,
    START_ISO_ATTR, END_ISO_ATTR, HREF_ATTR, CONTENT_ATTR,
    ID_ATTR
)
from helpers.log_helper import get_logger
from handlers.base_handler import \
    BaseReportHandler, \
    SourceReportDerivation, \
    EntitySourcedReportDerivation, SourcedReport, Report, EntityToReport, \
    SourceToReport
from services.report_service import \
    DIGEST_REPORT_FILE, \
    DETAILED_REPORT_FILE
from services.ambiguous_job_service import Source

DEFAULT_UNRESOLVABLE_RESPONSE = 'Request has run into an unresolvable issue.'

TYPE_ATTR = 'type'
JOB_ID_ATTR = 'job_id'
RAW_ATTR = 'raw'

ENTITY_ATTR_KEY = 'entity_attr'
ENTITY_VALUE_KEY = 'entity_value'

TENANT_NAME_ATTR = 'tenant_name'
TENANT_DISPLAY_NAME_ATTR = 'tenant_display_name'

_LOG = get_logger(__name__)

# Report Digest of Jobs resources
JOB_ENDPOINT = '/reports/digests/jobs/{id}'
TENANTS_JOBS_ENDPOINT = '/reports/digests/tenants/jobs'
TENANT_JOBS_ENDPOINT = '/reports/digests/tenants/{tenant_name}/jobs'

TENANTS_ENDPOINT = '/reports/digests/tenants'
TENANT_ENDPOINT = '/reports/digests/tenants/{tenant_name}'

# Scrapped, due to attribute naming inconsistency
ACCOUNTS_ENDPOINT = '/reports/digests/tenants/{tenant_name}/accounts'

NO_RESOURCES_FOR_REPORT = ' maintain(s) no resources to derive a report.'


class BaseDigestReportHandler(BaseReportHandler):
    """
    Provides base behaviour of digest-reporting, establishing
     report-derivation function.
    """

    @property
    def _source_report_derivation_function(self) -> SourceReportDerivation:
        return self._digest_report_derivation

    def _digest_report_derivation(self, source: Source, **kwargs: dict):
        """
        Thread-safe report-derivation.
        """
        jid = self._ambiguous_job_service.get_attribute(source, ID_ATTR)
        href = kwargs.get(HREF_ATTR)
        detailed_path = self._report_service.derive_job_object_path(
            job_id=jid, typ=DETAILED_REPORT_FILE)
        digest_path = self._report_service.derive_job_object_path(
            job_id=jid, typ=DIGEST_REPORT_FILE)

        _head = f'Native Job:\'{jid}\''

        if href:
            _LOG.info(_head + ' going to obtain hypertext reference'
                              'of the digest report.')
            ref: Optional[str] = self._report_service.href_job_report(
                path=digest_path, check=True
            )
            if ref:
                return ref

            # Going to generate standalone.

        # Maintains raw data, from now on.
        ref: Optional[dict] = None

        if not href:
            # Has not been pulled yet.
            ref = self._report_service.pull_job_report(path=digest_path)

        if not ref:
            _LOG.warning(_head + ' digest-report could not be found, '
                                 'pulling detailed one.')
            detailed = self._report_service.pull_job_report(path=detailed_path)
            if detailed:
                ref: dict = self._report_service.generate_digest(
                    detailed_report=detailed
                )
                _LOG.warning(_head + ' patching digest report absence.')
                if self._report_service.put_job_report(
                        path=digest_path, data=ref
                ) is None:
                    message = ' digest report could not be patched.'
                    _LOG.warning(_head + message)
                    ref = None if href else ref

        if href and ref:
            # Pull after successful patch.
            ref: Optional[str] = self._report_service.href_job_report(
                path=digest_path, check=False
            )

        return ref

    @property
    def _entity_sourced_report_derivation_function(
            self) -> EntitySourcedReportDerivation:
        return self._entity_report_derivation

    def _entity_report_derivation(
            self, sourced_reports: List[SourcedReport], **kwargs: dict
    ) -> Optional[Report]:
        """
        Accumulates digest reports of entity-related sources, returning a
        respective entity-based report of digests.
        :param sourced_reports: List[SourcedReport]
        :param kwargs: Dict, maintains `entity_attr` and
         optional, target `entity_value`, as well as `href` attribute,
         denoting demand for hypertext reference.
        :return: List[Report]
        """
        entity_attr: str = kwargs.get(ENTITY_ATTR_KEY, '')
        entity_value: str = kwargs.get(ENTITY_VALUE_KEY, '')
        assert entity_attr, 'Entity attribute is missing'

        dynamic_entity = not bool(entity_value)

        _head = f'{entity_attr.capitalize()}'
        href = kwargs.get(HREF_ATTR)

        ref: List[dict] = []
        ajs = self._ambiguous_job_service
        rs = self._report_service
        # Given that reports are not sorted, manually establish bounds.

        start, end = None, None
        for sourced in sourced_reports:
            source, report = sourced

            if dynamic_entity:
                _entity = ajs.get_attribute(item=source, attr=entity_attr)
                if not entity_value:
                    entity_value = _entity

                elif entity_value != _entity:
                    # Precautionary verification.
                    typ = ajs.get_type(item=source)
                    uid = ajs.get_attribute(item=source, attr=ID_ATTR)
                    _e_scope = f'{_head}:\'{entity_value}\''
                    _s_scope = f'{_entity} of {typ} \'{uid}\' job'
                    _LOG.error(f'{_e_scope} mismatches {_s_scope}.')
                    ref = []
                    break

            ref.append(report)
            sk = ajs.sort_value_into_datetime(item=source)
            if not start or not end:
                start = end = sk

            start = sk if sk > start else start
            end = sk if sk < end else end

        if not ref:
            return None

        _head += f':\'{entity_value}\''

        # Establish a report-key
        path = rs.derive_digests_report_object_path(
            entity_value=entity_value, entity_attr=entity_attr,
            start=start, end=end
        )

        if href:
            message = ' obtaining hypertext reference of entity-digest report.'
            _LOG.info(_head + message)
            url: str = rs.href_concrete_report(path=path, check=True)
            if url:
                return url
        else:
            message = ' checking for entity-digest report presence.'
            _LOG.info(_head + message)
            if rs.check_concrete_report(path=path):
                precomputed: dict = rs.pull_concrete_report(path=path)
                if precomputed:
                    return precomputed

        # Data does not exist or could not be retrieved - generating.
        # Going to generate standalone.
        message = ' accumulating points of sourced out digests.'
        _LOG.info(_head + message)
        ref: Dict[str, int] = rs.accumulate_digest(digest_list=ref)

        # todo if expiration rule is assigned, retain per each
        #  otherwise - retain only per hypertext request.

        if href and ref:
            # Pull after successful patch.
            message = ' retaining accumulated digest report.'
            _LOG.info(_head + message)
            if rs.put_json_concrete_report(data=ref, path=path):
                message = ' obtaining self-retained hypertext reference.'
                _LOG.info(_head + message)
                ref = rs.href_concrete_report(path=path, check=False)

        return ref


class JobsDigestHandler(BaseDigestReportHandler):

    def define_action_mapping(self):
        return {
            JOB_ENDPOINT: {
                GET_METHOD: self.get_by_job
            },
            TENANT_JOBS_ENDPOINT: {
                GET_METHOD: self.get_by_tenant
            },
            TENANTS_JOBS_ENDPOINT: {
                GET_METHOD: self.query_by_tenant
            },
        }

    def get_by_job(self, event: dict):
        _LOG.info(f'GET Job Digest Report(s) - {event}.')
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

        referenced_reports = self._attain_source_report_map(
            source_list=[source], href=href
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

    def get_by_tenant(self, event: dict):
        _LOG.info(f'GET Job Digest Report(s) of a Tenant - {event}.')
        # `tenant` has been injected via the restriction service.
        tenant_name = event[TENANT_ATTR]
        # Lower bound.
        start_iso: datetime = event[START_ISO_ATTR]
        # Upper bound.
        end_iso: datetime = event[END_ISO_ATTR]
        href = event.get(HREF_ATTR)

        customer = event.get(CUSTOMER_ATTR)
        tenant = self._attain_tenant(
            name=tenant_name, customer=customer, active=True
        )
        if not tenant:
            return self.response

        referenced_reports = self._attain_referenced_reports(
            start_iso=start_iso, end_iso=end_iso,
            customer=tenant.customer_name, tenants=[tenant_name],
            typ=event.get(TYPE_ATTR), href=event.get(HREF_ATTR)
        )

        if referenced_reports:
            ref_attr = HREF_ATTR if href else CONTENT_ATTR
            self._code = RESPONSE_OK_CODE
            self._content = [
                self.dto(source=source, report=report, ref_attr=ref_attr)
                for source, report in referenced_reports.items()
            ]

        return self.response

    def query_by_tenant(self, event: dict):

        _LOG.info(f'GET Job Digest Report(s) of Tenant(s) - {event}.')
        # `tenants` has been injected via the restriction service.
        tenant_names = event[TENANTS_ATTR]
        start_iso: datetime = event[START_ISO_ATTR]
        end_iso: datetime = event[END_ISO_ATTR]
        customer = event.get(CUSTOMER_ATTR)
        href = event.get(HREF_ATTR)

        referenced_reports = self._attain_referenced_reports(
            start_iso=start_iso, end_iso=end_iso,
            customer=customer, tenants=tenant_names,
            typ=event.get(TYPE_ATTR), href=event.get(HREF_ATTR)
        )

        if referenced_reports:
            ref_attr = HREF_ATTR if href else CONTENT_ATTR
            self._code = RESPONSE_OK_CODE
            self._content = [
                self.dto(source=source, report=report, ref_attr=ref_attr)
                for source, report in referenced_reports.items()
            ]

        return self.response

    def _attain_referenced_reports(
            self, start_iso: datetime, end_iso: datetime,
            customer: Optional[str] = None,
            tenants: Optional[List[str]] = None,
            cloud_ids: Optional[List[str]] = None,
            account_dn: Optional[str] = None,
            typ: Optional[str] = None, href: bool = False
    ) -> Optional[SourceToReport]:
        """
        account_dn here is obsolete
        """

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
        source_list = ajs.batch_list(
            typ_params_map=typ_params_map, customer=customer,
            start=start_iso, end=end_iso, sort=True
        )
        if not source_list:
            message = f' - no source-data of {job_scope} could be derived.'
            _LOG.warning(head + message)
            self._code = RESPONSE_RESOURCE_NOT_FOUND_CODE
            self._content = head + NO_RESOURCES_FOR_REPORT
            return

        _source_scope = ', '.join(
            ajs.get_attribute(item=s, attr=ID_ATTR)
            for s in source_list
        )
        _LOG.info(head + f' retrieving reports of {_source_scope} source(s).')
        source_to_report = self._attain_source_report_map(
            source_list=source_list, href=href
        )
        if not source_to_report:
            message = f' - no reports of {job_scope} could be derived.'
            _LOG.warning(head + message)
            self._code = RESPONSE_RESOURCE_NOT_FOUND_CODE
            self._content = head + NO_RESOURCES_FOR_REPORT
            return

        return source_to_report

    def dto(self, source: Source, report: Report, ref_attr: str):
        return {
            ID_ATTR: self._ambiguous_job_service.get_attribute(
                item=source, attr=ID_ATTR
            ),
            TYPE_ATTR: self._ambiguous_job_service.get_type(item=source),
            ref_attr: report
        }


class EntityDigestHandler(BaseDigestReportHandler):

    def define_action_mapping(self):
        return {
            TENANT_ENDPOINT: {
                GET_METHOD: self.get_by_tenant
            },
            TENANTS_ENDPOINT: {
                GET_METHOD: self.query_by_tenant
            },
        }

    def get_by_tenant(self, event: dict):
        _LOG.info(f'GET State Digest Report(s) of a Tenant - {event}.')
        # `tenant` has been injected via the restriction service.
        tenant_name = event[TENANT_ATTR]
        # Lower bound.
        start_iso: datetime = event[START_ISO_ATTR]
        # Upper bound.
        end_iso: datetime = event[END_ISO_ATTR]

        href = event.get(HREF_ATTR)
        customer = event.get(CUSTOMER_ATTR)
        tenant = self._attain_tenant(
            name=tenant_name, customer=customer, active=True
        )
        if not tenant:
            return self.response

        referenced_reports = self._attain_referenced_reports(
            entity_attr=TENANT_ATTR, entity_value=tenant_name,
            start_iso=start_iso, end_iso=end_iso,
            customer=tenant.customer_name, tenants=[tenant_name],
            typ=event.get(TYPE_ATTR), href=event.get(HREF_ATTR)
        )
        if referenced_reports:
            ref_attr = HREF_ATTR if href else CONTENT_ATTR
            self._code = RESPONSE_OK_CODE
            self._content = [
                self.dto(
                    entity_attr=TENANT_ATTR, entity_value=entity,
                    report=report, ref_attr=ref_attr
                )
                for entity, report in referenced_reports.items()
            ]
        return self.response

    def query_by_tenant(self, event: dict):

        _LOG.info(f'GET Job Digest Report(s) of Tenant(s) - {event}.')
        # `tenants` has been injected via the restriction service.
        tenant_names = event[TENANTS_ATTR]
        start_iso: datetime = event[START_ISO_ATTR]
        end_iso: datetime = event[END_ISO_ATTR]

        customer = event.get(CUSTOMER_ATTR)
        href = event.get(HREF_ATTR)

        referenced_reports = self._attain_referenced_reports(
            entity_attr=TENANT_ATTR,
            start_iso=start_iso, end_iso=end_iso,
            customer=customer, tenants=tenant_names,
            typ=event.get(TYPE_ATTR), href=event.get(HREF_ATTR)
        )

        if referenced_reports:
            ref_attr = HREF_ATTR if href else CONTENT_ATTR
            self._code = RESPONSE_OK_CODE
            self._content = [
                self.dto(
                    entity_attr=TENANT_ATTR, entity_value=entity,
                    report=report, ref_attr=ref_attr
                )
                for entity, report in referenced_reports.items()
            ]

        return self.response

    def _attain_referenced_reports(
            self, entity_attr: str,
            start_iso: datetime, end_iso: datetime,
            customer: Optional[str] = None,
            tenants: Optional[List[str]] = None,
            cloud_ids: Optional[List[str]] = None,
            account_dn: Optional[str] = None,
            typ: Optional[str] = None, href: bool = False,
            entity_value: Optional[str] = None
    ) -> EntityToReport:
        """
        account_dn here is obsolete
        """

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

        entity_scope = f'\'{entity_attr}\' entity'
        if entity_value:
            entity_scope += f' of the \'{entity_value}\' target value'

        # todo Responsibility chain

        _LOG.info(f'Obtaining {job_scope}, for {head or "tenants"}.')
        head = head or 'Tenants'
        typ_params_map = ajs.derive_typ_param_map(
            typ=typ, tenants=tenants,
            cloud_ids=cloud_ids
        )
        source_list = ajs.batch_list(
            typ_params_map=typ_params_map, customer=customer,
            start=start_iso, end=end_iso, sort=False
        )
        if not source_list:
            message = f' - no source-data of {job_scope} could be derived.'
            _LOG.warning(head + message)
            self._code = RESPONSE_RESOURCE_NOT_FOUND_CODE
            self._content = head + NO_RESOURCES_FOR_REPORT
            return self.response

        _source_scope = ', '.join(
            ajs.get_attribute(item=s, attr=ID_ATTR) for s in source_list
        )

        _LOG.info(head + f' retrieving reports of {_source_scope} source(s).')
        source_to_report = self._attain_source_report_map(
            source_list=source_list
        )
        if not source_to_report:
            message = f' - no reports of {job_scope} could be derived.'
            _LOG.warning(head + message)
            self._code = RESPONSE_RESOURCE_NOT_FOUND_CODE
            self._content = head + NO_RESOURCES_FOR_REPORT
            return self.response

        message = f' mapping sourced reports to {entity_scope}.'
        _LOG.info(head + message)

        entity_sourced_reports = self._attain_entity_sourced_reports(
            entity_attr=entity_attr, entity_value=entity_value,
            source_to_report=source_to_report
        )
        if not entity_sourced_reports:
            message = f' - no reports of {job_scope} could be mapped '
            message += f' to {entity_scope}.'
            _LOG.warning(head + message)
            self._code = RESPONSE_RESOURCE_NOT_FOUND_CODE
            self._content = head + NO_RESOURCES_FOR_REPORT
            return self.response

        message = f' deriving digest reports of {entity_scope}.'
        _LOG.info(head + message)

        entity_to_report = self._attain_entity_report_map_from_sourced_reports(
            entity_sourced_reports=entity_sourced_reports,
            entity_attr=entity_attr, entity_value=entity_value, href=href
        )
        if not entity_to_report:
            message = f' - no reports of {job_scope} could be derived'
            message += f' based on {entity_scope}.'
            _LOG.warning(head + message)
            self._code = RESPONSE_RESOURCE_NOT_FOUND_CODE
            self._content = head + NO_RESOURCES_FOR_REPORT
            return self.response

        return entity_to_report

    @staticmethod
    def dto(
            entity_attr: str, entity_value: str, report: Report, ref_attr: str
    ):
        return {
            entity_attr: entity_value,
            ref_attr: report
        }
