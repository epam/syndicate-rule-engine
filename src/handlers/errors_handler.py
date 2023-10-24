from datetime import datetime
from http import HTTPStatus
from typing import List, Optional, Dict, Any

from handlers.base_handler import \
    BaseReportHandler, \
    SourceReportDerivation, \
    EntitySourcedReportDerivation, SourcedReport, Report, EntityToReport
from helpers.constants import (CUSTOMER_ATTR, TENANTS_ATTR, TENANT_ATTR,
                               START_ISO_ATTR, END_ISO_ATTR, HREF_ATTR,
                               CONTENT_ATTR,
                               ID_ATTR, FORMAT_ATTR, JSON_ATTR,
                               PARAM_REQUEST_PATH, PARAM_HTTP_METHOD,
                               HTTPMethod)
from helpers.log_helper import get_logger
from services.ambiguous_job_service import Source
from services.report_service import \
    STATISTICS_FILE

DEFAULT_UNRESOLVABLE_RESPONSE = 'Request has run into an unresolvable issue.'

TYPE_ATTR = 'type'
JOB_ID_ATTR = 'job_id'
RAW_ATTR = 'raw'

SUBTYPE_ATTR = 'subtype'
ACCESS_ATTR = 'access'
CORE_ATTR = 'core'

ENTITY_ATTR_KEY = 'entity_attr'
ENTITY_VALUE_KEY = 'entity_value'

TENANT_NAME_ATTR = 'tenant_name'
TENANT_DISPLAY_NAME_ATTR = 'tenant_display_name'

_LOG = get_logger(__name__)

# todo allow explicit file-extension, i.e., .json, .xlsx

# Report Errors of Jobs resources
JOB_ENDPOINT = '/reports/errors/jobs/{id}'
JOB_ACCESS_ENDPOINT = '/reports/errors/access/jobs/{id}'
JOB_CORE_ENDPOINT = '/reports/errors/core/jobs/{id}'

# Report Errors of accumulated Jobs, driven by entity-driven scope.
TENANTS_ENDPOINT = '/reports/errors/tenants'
TENANT_ENDPOINT = '/reports/errors/tenants/{tenant_name}'

TENANTS_ACCESS_ENDPOINT = '/reports/errors/access/tenants'
TENANT_ACCESS_ENDPOINT = '/reports/errors/access/tenants/{tenant_name}'

TENANTS_CORE_ENDPOINT = '/reports/errors/core/tenants'
TENANT_CORE_ENDPOINT = '/reports/errors/core/tenants/{tenant_name}'

CHILD_TENANTS_ENDPOINT = '/tenants'
CHILD_TENANT_ENDPOINT = '/tenants/{tenant_name}'

NO_RESOURCES_FOR_REPORT = ' maintain(s) no resources to derive a report.'

FailedRule = Dict[str, Dict[str, Any]]  # job-id to rule-data
FailedRules = Dict[str, List[FailedRule]]  # rule-id to failed rule


class BaseErrorsReportHandler(BaseReportHandler):
    """
    Provides base behaviour of error-reporting, establishing
     report-derivation function.
    """

    @property
    def _source_report_derivation_function(self) -> SourceReportDerivation:
        return self._statistics_report_derivation

    def _statistics_report_derivation(
            self, source: Source, **kwargs: dict
    ) -> Optional[Report]:

        """
        Obtains report of rule-failed statistics of a sourced job,
        returning a respective report.
        :param source: Source
        :param kwargs: Dict
        :return: Optional[Report]
        """
        jid = self._ambiguous_job_service.get_attribute(source, ID_ATTR)
        rs = self._report_service
        path = rs.derive_job_object_path(job_id=jid, typ=STATISTICS_FILE)
        return rs.pull_job_statistics(path=path)

    @property
    def _entity_sourced_report_derivation_function(self) -> \
            EntitySourcedReportDerivation:
        return self._entity_failed_rule_report_derivation

    def _entity_failed_rule_report_derivation(
            self, sourced_reports: List[SourcedReport], **kwargs: dict
    ) -> Optional[Report]:

        entity_attr: str = kwargs.get(ENTITY_ATTR_KEY, '')
        entity_value: str = kwargs.get(ENTITY_VALUE_KEY, '')
        subtype: str = kwargs.get(SUBTYPE_ATTR, '')
        href: bool = kwargs.get(HREF_ATTR, False)
        frmt: str = kwargs.get(FORMAT_ATTR, '')
        assert entity_attr, 'Entity attribute is missing'
        return self._attain_failed_rule_map(
            sourced_reports=sourced_reports,
            entity_attr=entity_attr,
            entity_value=entity_value,
            href=href, frmt=frmt,
            subtype=subtype
        )

    def _attain_failed_rule_map(
            self, sourced_reports: List[SourcedReport],
            entity_attr: str, entity_value: Optional[str] = None,
            href: Optional[bool] = False, frmt: Optional[str] = None,
            subtype: Optional[str] = None
    ):
        """
        Derives relation map of failed rule, merged amongst sourced reports.
        :param sourced_reports: List[Source, List[Dict]]
        :param entity_attr: str, denotes unique entity id-attribute
        :param entity_value: Optional[str], denotes ta target entity-id value
        :return: Dict[str, List[Dict]]
        """
        head = entity_attr.capitalize()
        if entity_value:
            head += f':\'{entity_value}\''

        ajs = self._ambiguous_job_service
        rs = self._report_service

        failed_rule_map: FailedRules = {}
        _LOG.info(head + ' merging failed rules, amongst report(s).')

        start, end = None, None
        source = None
        for sourced in sourced_reports:
            source, statistics = sourced

            if not entity_value:
                _entity = ajs.get_attribute(item=source, attr=entity_attr)
                if not entity_value:
                    entity_value = _entity

                elif entity_value != _entity:
                    # Precautionary verification.
                    typ = ajs.get_type(item=source)
                    uid = ajs.get_attribute(item=source, attr=ID_ATTR)
                    _e_scope = f'{head}:\'{entity_value}\''
                    _s_scope = f'{_entity} of {typ} \'{uid}\' job'
                    _LOG.error(f'{_e_scope} mismatches {_s_scope}.')
                    failed_rule_map = {}
                    break

            # Establishes start and end dates, for the report.
            sk = ajs.sort_value_into_datetime(item=source)
            if not start or not end:
                start = end = sk
            start = sk if sk > start else start
            end = sk if sk < end else end

            # Derives failed rule map.
            jid = self._ambiguous_job_service.get_attribute(source, ID_ATTR)
            failed_ref = rs.derive_failed_rule_map(
                job_id=jid, statistics=statistics
            )
            if not failed_ref:
                message = f' could not establish failed rules of {jid} job.'
                _LOG.warning(head + message)
                continue

            for rid, data_list in failed_ref.items():
                rule_scope = failed_rule_map.setdefault(rid, [])
                rule_scope.extend(data_list)

        if not failed_rule_map:
            _LOG.warning(head + ' no failed rules could be established.')

        else:

            if subtype:
                _LOG.info(head + f' deriving {subtype} typed failed rule map.')
                failed_rule_map = rs.derive_type_based_failed_rule_map(
                    typ=subtype, failed_rule_map=failed_rule_map
                )

            if failed_rule_map and href and frmt:
                message = f' providing hypertext reference to {frmt} file of'
                message += ' failed rules.'
                _LOG.info(head + message)
                job_id = self._ambiguous_job_service.get_attribute(source, ID_ATTR) if len(sourced_reports) == 1 else None
                object_path = rs.derive_error_report_object_path(
                    subtype=subtype, entity_value=entity_value,
                    entity_attr=entity_attr, start=start, end=end,
                    fext=frmt, job_id=job_id
                )

                if frmt == JSON_ATTR:
                    message = ' retaining error json hypertext reference.'
                    _LOG.info(head + message)
                    if not rs.put_json_concrete_report(
                            data=failed_rule_map, path=object_path
                    ):
                        object_path = None

                else:
                    _LOG.info(head + ' deriving xlsx compliance report.')
                    file_name = rs.derive_name_of_report_object_path(
                        object_path=object_path
                    )
                    stream_path = rs.derive_errors_report_excel_path(
                        file_name=file_name, failed_rules=failed_rule_map,
                        subtype=subtype
                    )
                    if stream_path:
                        message = ' retaining error xlsx hypertext reference.'
                        _LOG.warning(head + message)
                        if rs.put_path_retained_concrete_report(
                                stream_path=stream_path,
                                object_path=object_path
                        ) is None:
                            message = ' xlsx error report hypertext could not'
                            message += ' be provided.'
                            _LOG.warning(head + message)
                            # Explicitly denote reference absence.
                            object_path = None

                if object_path:
                    message = ' obtaining hypertext reference.'
                    _LOG.info(head + message)
                    failed_rule_map = rs.href_concrete_report(
                        path=object_path, check=False
                    )

        return failed_rule_map


class JobsErrorsHandler(BaseErrorsReportHandler):

    def define_action_mapping(self):
        return {
            JOB_ENDPOINT: {
                HTTPMethod.GET.value: self.get_job_errors
            },
            JOB_ACCESS_ENDPOINT: {
                HTTPMethod.GET.value: self.get_job_access_errors
            },
            JOB_CORE_ENDPOINT: {
                HTTPMethod.GET.value: self.get_job_core_errors
            }
        }

    def get_job_errors(self, event: dict):
        return self._get_typed_job(event=event)

    def get_job_access_errors(self, event: dict):
        return self._get_typed_job(event=event, error_subtype=ACCESS_ATTR)

    def get_job_core_errors(self, event: dict):
        return self._get_typed_job(event=event, error_subtype=CORE_ATTR)

    def _get_typed_job(self, event: dict, error_subtype: Optional[str] = None):
        error_typ = 'Errors'
        if error_subtype:
            error_typ = f'{error_subtype.capitalize()} {error_typ}'
        _LOG.info(f'GET Job {error_typ} Report - {event}.')

        # Note that `job_id` denotes the primary-key's hash-key of entities.
        uid: str = event[ID_ATTR]
        typ: str = event[TYPE_ATTR]
        customer = event.get(CUSTOMER_ATTR)
        tenants = event.get(TENANTS_ATTR)
        href = event.get(HREF_ATTR)
        frmt = event.get(FORMAT_ATTR)

        entity_attr = f'{typ.capitalize()} Job'
        entity_value = uid
        head = f'{entity_attr}:\'{entity_value}\''

        source = self._attain_source(
            uid=uid, typ=typ, customer=customer, tenants=tenants
        )
        if not source:
            return self.response

        statistics = self._statistics_report_derivation(source=source)
        if not statistics:
            _LOG.warning(head + ' could not obtain statistics report.')
            self._code = HTTPStatus.NOT_FOUND
            self._content = head + NO_RESOURCES_FOR_REPORT
            return self.response

        failed_rule_map = self._attain_failed_rule_map(
            sourced_reports=[(source, statistics)],
            entity_attr=entity_attr, entity_value=entity_value,
            href=href, frmt=frmt, subtype=error_subtype
        )
        ref_attr = HREF_ATTR if href else CONTENT_ATTR
        self._code = HTTPStatus.OK
        self._content = [
            self.dto(
                source=source, report=failed_rule_map, ref_attr=ref_attr
            )
        ]

        return self.response

    def dto(self, source: Source, report: Report, ref_attr: str):
        return {
            ID_ATTR: self._ambiguous_job_service.get_attribute(
                item=source, attr=ID_ATTR
            ),
            TYPE_ATTR: self._ambiguous_job_service.get_type(item=source),
            ref_attr: report
        }


class EntityErrorsHandler(BaseErrorsReportHandler):
    base_resource: str = '/reports/errors'

    def define_action_mapping(self):
        return {
            TENANT_ENDPOINT: {
                HTTPMethod.GET.value: self.mediate_event
            },
            TENANTS_ENDPOINT: {
                HTTPMethod.GET.value: self.mediate_event
            },
            TENANT_ACCESS_ENDPOINT: {
                HTTPMethod.GET.value: self.mediate_event
            },
            TENANTS_ACCESS_ENDPOINT: {
                HTTPMethod.GET.value: self.mediate_event
            },
            TENANT_CORE_ENDPOINT: {
                HTTPMethod.GET.value: self.mediate_event
            },
            TENANTS_CORE_ENDPOINT: {
                HTTPMethod.GET.value: self.mediate_event
            },
        }

    @property
    def mediator_map(self):
        return {
            CHILD_TENANT_ENDPOINT: {
                HTTPMethod.GET.value: self._get_by_tenant
            },
            CHILD_TENANTS_ENDPOINT: {
                HTTPMethod.GET.value: self._query_by_tenant
            }
        }

    def mediate_event(self, event: dict):
        path = event.get(PARAM_REQUEST_PATH, '')
        method = event.get(PARAM_HTTP_METHOD)
        mediated_map = self.mediator_map
        _, child_path = path.split(self.base_resource, 1)
        params = dict(event=event)

        if child_path not in mediated_map and '/' in child_path[1:]:
            child_path = child_path[1:]
            index = child_path.index('/')
            params.update(error_subtype=child_path[:index])
            child_path = child_path[index:]

        f = mediated_map.get(child_path, {}).get(method, None)
        if f:
            return f(**params)

    def _get_by_tenant(self, event: dict, error_subtype: Optional[str] = None):
        error_typ = 'Errors'
        if error_subtype:
            error_typ = f'{error_subtype.capitalize()} {error_typ}'
        _LOG.info(f'GET {error_typ} Report(s) of a Tenant - {event}.')
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
            typ=event.get(TYPE_ATTR), href=href, frmt=event.get(FORMAT_ATTR),
            error_subtype=error_subtype

        )

        ref_attr = HREF_ATTR if href else CONTENT_ATTR
        self._code = HTTPStatus.OK
        self._content = [
            self.dto(
                entity_attr=TENANT_ATTR, entity_value=entity,
                report=report, ref_attr=ref_attr
            )
            for entity, report in referenced_reports.items()
        ]
        return self.response

    def _query_by_tenant(
            self, event: dict, error_subtype: Optional[str] = None
    ):
        error_typ = 'Errors'
        if error_subtype:
            error_typ = f'{error_subtype.capitalize()} {error_typ}'
        _LOG.info(f'GET {error_typ} Report(s) of Tenant(s) - {event}.')

        # `tenants` has been injected via the restriction service.
        tenant_names = event[TENANTS_ATTR]
        # Lower bound.
        start_iso: datetime = event[START_ISO_ATTR]
        # Upper bound.
        end_iso: datetime = event[END_ISO_ATTR]

        customer = event.get(CUSTOMER_ATTR)
        href = event.get(HREF_ATTR)

        referenced_reports = self._attain_referenced_reports(
            entity_attr=TENANT_ATTR,
            start_iso=start_iso, end_iso=end_iso,
            customer=customer, tenants=tenant_names,
            typ=event.get(TYPE_ATTR),
            href=href, frmt=event.get(FORMAT_ATTR),
            error_subtype=error_subtype
        )
        ref_attr = HREF_ATTR if href else CONTENT_ATTR
        self._code = HTTPStatus.OK
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
            typ: Optional[str] = None,
            href: bool = False, frmt: Optional[str] = None,
            entity_value: Optional[str] = None,
            error_subtype: Optional[str] = None
    ) -> EntityToReport:

        cloud_ids = cloud_ids or []
        ajs = self._ambiguous_job_service

        head = ''

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
            self._code = HTTPStatus.NOT_FOUND
            self._content = head + NO_RESOURCES_FOR_REPORT
            return self.response

        _source_scope = ', '.join(
            ajs.get_attribute(item=s, attr=ID_ATTR) for s in source_list
        )

        message = f' retrieving statistics of {_source_scope} source(s).'
        _LOG.info(head + message)
        source_to_report = self._attain_source_report_map(
            source_list=source_list
        )
        if not source_to_report:
            message = f' - no reports of {job_scope} could be derived.'
            _LOG.warning(head + message)
            self._code = HTTPStatus.NOT_FOUND
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
            self._code = HTTPStatus.NOT_FOUND
            self._content = head + NO_RESOURCES_FOR_REPORT
            return self.response

        message = f' deriving failed rule reports of {entity_scope}.'
        _LOG.info(head + message)

        entity_to_report = self._attain_entity_report_map_from_sourced_reports(
            entity_sourced_reports=entity_sourced_reports,
            entity_attr=entity_attr, entity_value=entity_value,
            href=href, format=frmt, subtype=error_subtype
        )

        return entity_to_report

    @staticmethod
    def dto(
            entity_attr: str, entity_value: str, report: Report, ref_attr: str
    ):
        return {
            entity_attr: entity_value,
            ref_attr: report
        }
