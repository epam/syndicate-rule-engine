from datetime import datetime
from http import HTTPStatus
from typing import List, Optional, Dict

from modular_sdk.models.tenant import Tenant

from handlers.base_handler import BaseReportHandler, SourceReportDerivation, \
    EntitySourcedReportDerivation, SourcedReport, Report, EntityToReport
from helpers.constants import (
    CUSTOMER_ATTR, TENANT_ATTR, START_ISO_ATTR, END_ISO_ATTR, HREF_ATTR,
    CONTENT_ATTR, ID_ATTR, FORMAT_ATTR, JSON_ATTR, RULE_ATTR
)
from helpers.log_helper import get_logger
from services.ambiguous_job_service import Source
from services.report_service import STATISTICS_FILE

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

# Report Errors of Jobs resources
JOB_ENDPOINT = '/reports/rules/jobs/{id}'
# Report Errors of accumulated Jobs, driven by entity-driven scope.
TENANTS_ENDPOINT = '/reports/rules/tenants'
TENANT_ENDPOINT = '/reports/rules/tenants/{tenant_name}'

NO_RESOURCES_FOR_REPORT = ' maintain(s) no resources to derive a report.'


class BaseRulesReportHandler(BaseReportHandler):
    """
    Provides base behaviour of rule-reporting, establishing
     report-derivation function.
    """

    def define_action_mapping(self):
        return {}

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
        return self._entity_statistics_report_derivation

    def _entity_statistics_report_derivation(
            self, sourced_reports: List[SourcedReport], **kwargs: dict
    ) -> Optional[Report]:

        entity_attr: str = kwargs.get(ENTITY_ATTR_KEY, '')
        entity_value: str = kwargs.get(ENTITY_VALUE_KEY, '')
        href: bool = kwargs.get(HREF_ATTR, False)
        frmt: str = kwargs.get(FORMAT_ATTR, '')
        target_rule: str = kwargs.get(RULE_ATTR, '')
        list_format: bool = kwargs.get('list_format', False)
        assert entity_attr, 'Entity attribute is missing'
        return self._attain_statistics_report(
            sourced_reports=sourced_reports,
            entity_attr=entity_attr,
            entity_value=entity_value,
            href=href, frmt=frmt,
            target_rule=target_rule,
            list_format=list_format
        )

    def _attain_statistics_report(
            self, sourced_reports: List[SourcedReport],
            entity_attr: str, entity_value: Optional[str] = None,
            href: Optional[bool] = False, frmt: Optional[str] = None,
            target_rule: Optional[str] = None,
            list_format: Optional[bool] = False
    ):
        """
        Derives relation map of failed rule, merged amongst sourced reports.
        :param sourced_reports: List[Source, List[Dict]]
        :param entity_attr: str, denotes unique entity id-attribute
        :param entity_value: Optional[str], denotes ta target entity-id value
        :return: Dict[str, List[Dict]]
        """
        head = entity_attr
        if entity_value:
            head += f':\'{entity_value}\''

        ajs = self._ambiguous_job_service
        rs = self._report_service

        statistics_list: List[List[Dict]] = []
        _LOG.info(head + ' extending statistics list, for each sourced list.')

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
            statistics_list.append(statistics)

        job_id = self._ambiguous_job_service.get_attribute(source, ID_ATTR) \
            if len(sourced_reports) == 1 else None
        object_path = rs.derive_rule_report_object_path(
            entity_value=entity_value, entity_attr=entity_attr,
            start=start, end=end, fext=frmt, job_id=job_id
        )

        if href and frmt:
            _LOG.info(head + ' going to obtain hypertext reference'
                             ' of the rule statistic report.')
            ref: Optional[str] = self._report_service.href_job_report(
                path=object_path, check=True
            )
            if ref:
                return ref

        # Otherwise produces data, on its own.

        message = ' removing duplicates'
        if target_rule:
            message += f', filtering by {target_rule} rule'
        message += ' within aggregated statistic lists.'
        _LOG.info(head + message)

        statistics_list: List[
            List[Dict]] = rs.derive_clean_filtered_statistics_list(
            statistic_list=statistics_list, target_rule=target_rule
        )
        if not statistics_list:
            _LOG.warning(head + ' no statistics could be established.')

        else:

            _LOG.info(head + ' averaging out statistics.')
            statistics_list: List[Dict] = rs.average_out_statistics(
                statistics_list=statistics_list, list_format=list_format
            )

            if statistics_list and href and frmt:
                message = f' providing hypertext reference to {frmt} file of'
                message += ' rule statistics.'
                _LOG.info(head + message)
                if frmt == JSON_ATTR:
                    message = ' retaining error json hypertext reference.'
                    _LOG.info(head + message)
                    if not rs.put_json_concrete_report(
                            data=statistics_list, path=object_path
                    ):
                        object_path = None

                else:
                    _LOG.info(head + ' deriving xlsx statistics report.')
                    file_name = rs.derive_name_of_report_object_path(
                        object_path=object_path
                    )
                    stream_path = rs.derive_rule_statistics_report_xlsx_path(
                        file_name=file_name,
                        averaged_statistics=statistics_list
                    )
                    if stream_path:
                        message = ' retaining xlsx hypertext reference.'
                        _LOG.warning(head + message)
                        if rs.put_path_retained_concrete_report(
                                stream_path=stream_path,
                                object_path=object_path
                        ) is None:
                            message = ' xlsx statistic report hypertext could'
                            message += ' not be provided.'
                            _LOG.warning(head + message)
                            # Explicitly denote reference absence.
                            object_path = None

                if object_path:
                    message = ' obtaining hypertext reference.'
                    _LOG.info(head + message)
                    statistics_list = rs.href_concrete_report(
                        path=object_path, check=False
                    )

        return statistics_list


class RuleReportService(BaseRulesReportHandler):

    def define_action_mapping(self):
        return {}

    def get_by_tenant(self, event: dict):
        _LOG.info(f'GET Rule Report(s) of an Tenant - {event}.')

        # `tenant` has been injected via the restriction service.
        tenant_name = event[TENANT_ATTR]
        # Lower bound.
        start_iso: datetime = event[START_ISO_ATTR]
        # Upper bound.
        end_iso: datetime = event[END_ISO_ATTR]

        href = event.get(HREF_ATTR)
        customer = event.get(CUSTOMER_ATTR)

        head = f'Tenant: \'{tenant_name}\''
        tenants: List[Tenant] = [
            *self._modular_service.i_get_tenant(iterator=iter([tenant_name]))
        ]
        if not tenants:
            self._code = HTTPStatus.NOT_FOUND
            self._content = head + ' does not exist'
            return self.response

        referenced_reports = self.attain_referenced_reports(
            entity_attr=TENANT_ATTR, entity_value=tenant_name,
            start_iso=start_iso, end_iso=end_iso,
            customer=customer, tenants=[tenant_name],
            cloud_ids=[each.project for each in tenants],
            typ=event.get(TYPE_ATTR), href=event.get(HREF_ATTR),
            frmt=event.get(FORMAT_ATTR), target_rule=event.get(RULE_ATTR)
        )

        if referenced_reports:
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

    def attain_referenced_reports(
            self, entity_attr: str,
            start_iso: datetime, end_iso: datetime,
            customer: Optional[str] = None,
            tenants: Optional[List[str]] = None,
            cloud_ids: Optional[List[str]] = None,
            typ: Optional[str] = None,
            href: bool = False, frmt: Optional[str] = None,
            entity_value: Optional[str] = None,
            target_rule: Optional[str] = None,
            source_list: Optional[List[Source]] = None,
            list_format: bool = False
    ) -> EntityToReport:

        cloud_ids = cloud_ids or []
        ajs = self._ambiguous_job_service

        head = ''

        # Log-Header.

        if tenants:
            multiple = len(tenants) > 1
            bind = ', '.join(map("'{}'".format, tenants or []))
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
            cloud_ids=cloud_ids,
        )
        if not source_list:
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
            href=href, format=frmt, rule=target_rule,
            list_format=list_format
        )
        if not entity_to_report:
            message = f' - no reports of {job_scope} could be derived'
            message += f' based on {entity_scope}.'
            _LOG.warning(head + message)
            self._code = HTTPStatus.NOT_FOUND
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
