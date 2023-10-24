from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from http import HTTPStatus
from json import dumps
from sys import getsizeof
from typing import List, Optional, Union, Callable, Dict, Any, Tuple

from modular_sdk.models.tenant import Tenant

from handlers.abstracts.abstract_handler import AbstractHandler
from helpers import build_response
from helpers.constants import MANUAL_TYPE_ATTR
from helpers.log_helper import get_logger
from services.ambiguous_job_service import AmbiguousJobService
from services.batch_results_service import BatchResults
from services.job_service import Job, JOB_SUCCEEDED_STATUS
from services.modular_service import ModularService
from services.report_service import \
    ReportService

RESPONSE_SIZE_LIMIT = 6291456
ITEM_SIZE_RESPONSE = 'Item size is too large, please use \'href\' parameter.'
DEFAULT_UNRESOLVABLE_RESPONSE = 'Request has run into an unresolvable issue.'

REACTIVE_TYPE_ATTR = 'reactive'
ID_ATTR = 'id'

_LOG = get_logger(__name__)

# Establish an ambiguous report source
Source = Union[BatchResults, Job]
Report = Union[dict, list, str, bytes]
SourceToReport = Dict[Source, Any]
SourceReportDerivation = Callable[[Source, Dict], Any]
SourcedReport = Tuple[Source, Report]
EntityToSourcedReports = Dict[str, List[SourcedReport]]

EntitySourcedReportDerivation = Callable[[List[SourcedReport], Dict], Any]
EntityToReport = Dict[str, Report]


class BaseReportHandler(AbstractHandler):
    _code: int
    _content: Union[str, dict, list]

    def __init__(self, ambiguous_job_service: AmbiguousJobService,
                 modular_service: ModularService,
                 report_service: ReportService):
        self._ambiguous_job_service = ambiguous_job_service
        self._modular_service = modular_service
        self._report_service = report_service
        self._reset()

    @property
    @abstractmethod
    def _source_report_derivation_function(self) -> SourceReportDerivation:
        pass

    @property
    def response(self):
        _code, _content = self._code, self._content
        self._reset()

        _response = build_response(code=_code, content=_content)

        if _code == isinstance(_content, (dict, list)):  # todo does it work?
            _LOG.info('Going to check for the response size constraint.')
            size = getsizeof(dumps(_response))
            if size > RESPONSE_SIZE_LIMIT:
                _response = None
                _LOG.warning(f'Response size of {size} bytes is too large.')
                _code = HTTPStatus.BAD_REQUEST
                _content = ITEM_SIZE_RESPONSE

        _LOG.info(f'Going to respond with the following '
                  f'code={_code}, content={_content}.')

        return _response or build_response(code=_code, content=_content)

    @property
    def _entity_sourced_report_derivation_function(self) -> Optional[
        EntitySourcedReportDerivation
    ]:
        return ...

    def _reset(self):
        self._code: Optional[int] = HTTPStatus.INTERNAL_SERVER_ERROR
        self._content: Optional[str] = DEFAULT_UNRESOLVABLE_RESPONSE

    def _attain_source(self, uid: str, typ: Optional[str] = None,
                       customer: Optional[str] = None,
                       tenants: Optional[List[str]] = None) -> Optional[
        Source]:
        """
        Mediates report job source attainment, based on the requested,
        previously verified `typ` attribute.
        :param uid: str, unique identifier of the requested entity
        :param typ: str
        :param customer: Optional[str]
        :param tenants: Optional[List[str]]
        :return: Optional[Source]
        """
        ref = {
            MANUAL_TYPE_ATTR: self._attain_manual_source,
            REACTIVE_TYPE_ATTR: self._attain_reactive_source
        }
        if typ:
            assert typ in ref, f'Invalid type {typ}'
            return ref[typ](uid, customer, tenants)
        source = filter(lambda x: x,
                        (get(uid, customer, tenants) for get in ref.values()))
        result = next(source, None)
        if not result and not typ:
            self._content = 'Job does not exist'
        return result

    def _attain_reactive_source(
            self, brid: str, customer: Optional[str] = None,
            tenants: Optional[List[str]] = None
    ):
        """
        Obtains a Batch Results entity, based on a given `brid` partition key,
        verifying access, based on a customer and tenant.
        :param brid: str
        :param customer: Optional[str]
        :param tenants: Optional[List[str]]
        :return: Optional[BatchResults]
        """
        _head = f'{REACTIVE_TYPE_ATTR.capitalize()} job:\'{brid}\''
        _default_404 = _head + ' does not exist.'
        _LOG.info(_head + ' is being obtained.')

        # Todo use a domain entities models rather then persistence models.
        entity = self._ambiguous_job_service.get(
            uid=brid, typ=REACTIVE_TYPE_ATTR
        )
        if not entity:
            _LOG.warning(_default_404)
        elif customer and entity.customer_name != customer:
            _LOG.warning(_head + f' is not bound to \'{customer}\' customer.')
            entity = None
        elif tenants and entity.tenant_name not in tenants:
            _scope = ', '.join(map("'{}'".format, tenants)) + ' tenant(s)'
            _LOG.warning(_head + f' is not bound to any of {_scope}.')
            entity = None
        elif entity.status != JOB_SUCCEEDED_STATUS:
            _status = JOB_SUCCEEDED_STATUS
            _LOG.warning(_head + f' is not of \'{_status}\' status.')
            entity = None

        if not entity:
            self._code = HTTPStatus.NOT_FOUND
            self._content = _default_404

        return entity

    def _attain_manual_source(
            self, jid: str, customer: Optional[str] = None,
            tenants: Optional[List[str]] = None
    ):
        """
        Obtains a Job entity, based on a given `jid` partition key,
        verifying access, based on a customer and tenant.
        :param jid: str
        :param customer: Optional[str]
        :param tenants: Optional[List[str]]
        :return: Optional[BatchResults]
        """
        _head = f'{MANUAL_TYPE_ATTR.capitalize()} job:\'{jid}\''
        _default_404 = _head + ' does not exist.'
        _LOG.info(_head + ' is being obtained.')
        # Todo use a domain entities models rather then persistence models.
        entity = self._ambiguous_job_service.get(uid=jid, typ=MANUAL_TYPE_ATTR)

        if not entity:
            _LOG.warning(_default_404)
        elif customer and entity.customer_display_name != customer:
            _LOG.warning(_head + f' is not bound to \'{customer}\' customer.')
            entity = None
        elif tenants and entity.tenant_display_name not in tenants:
            _scope = ', '.join(map("'{}'".format, tenants)) + ' tenant(s)'
            _LOG.warning(_head + f' is not bound to any of {_scope}.')
            entity = None
        elif entity.status != JOB_SUCCEEDED_STATUS:
            _status = JOB_SUCCEEDED_STATUS
            _LOG.warning(_head + f' is not of \'{_status}\' status.')
            entity = None

        if not entity:
            self._code = HTTPStatus.NOT_FOUND
            self._content = _default_404

        return entity

    def _attain_tenant(self, name: str, customer: Optional[str] = None,
                       active: bool = None) -> Optional[Tenant]:
        """
        Obtains a Tenant entity, based on a given tenant `name` partition key,
        verifying access, based on a customer and activity state.
        :param name: str
        :param customer: Optional[str]
        :param active: Optional[bool]
        :return: Optional[Tenant]
        """
        _head = f'Tenant:\'{name}\''
        _default_404 = _head + ' does not exist.'
        _LOG.info(_head + ' is being obtained.')
        entity = self._modular_service.get_tenant(tenant=name)
        if not entity:
            _LOG.warning(_default_404)
        elif customer and entity.customer_name != customer:
            _LOG.warning(_head + f' is not bound to \'{customer}\' customer.')
            entity = None
        elif active is not None and entity.is_active != active:
            _LOG.warning(_head + f' activity does not equal {active}.')
            entity = None

        if not entity:
            self._code = HTTPStatus.NOT_FOUND
            self._content = _default_404

        return entity

    def _attain_source_report_map(self, source_list: List[Source], **kwargs):
        """
        Returns a map of source-to-report variables, based on the
        instance-specific `source_report_derivation_function`
        :param source_list: Dict[str, List[Dict]]
        :return: SourceToReport
        """
        output = {}

        derivation_function = self._source_report_derivation_function
        if not derivation_function:
            _LOG.error('Report derivation function has not been assigned.')
            return output

        with ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(
                    derivation_function, source, **kwargs
                ): source
                for source in source_list
            }
            for future in as_completed(futures):
                _source: Source = futures[future]
                _typ = self._ambiguous_job_service.get_type(item=_source)
                _uid = self._ambiguous_job_service.get_attribute(
                    item=_source, attr=ID_ATTR
                )
                _head = f'{_typ.capitalize()} Job:\'{_uid}\''
                _head += ' derivation of a report'
                try:
                    _reported = future.result()
                    if _reported:
                        output[_source] = _reported
                    else:
                        _LOG.warning(_head + ' has been unsuccessful.')
                except (Exception, BaseException) as e:
                    _LOG.warning(_head + f' has run into an issue - {e}.')

        return output

    def _attain_entity_report_map_from_sourced_reports(
            self, entity_sourced_reports: EntityToSourcedReports, **kwargs
    ):
        """
        Returns a map of source-to-report variables, based on the
        instance-specific `entity_report_derivation_function`
        :param entity_sourced_reports: EntityToSourcedReports
        :return: EntityToReport
        """
        output = {}

        derivation_function = self._entity_sourced_report_derivation_function
        if not derivation_function:
            _LOG.error('Report derivation function has not been assigned.')
            return output

        with ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(
                    derivation_function,
                    sourced_reports, **kwargs
                ): entity
                for entity, sourced_reports in entity_sourced_reports.items()
            }
            for future in as_completed(futures):
                _entity: str = futures[future]
                _head = f'\'{_entity}\' derivation of an entity-report'
                try:
                    _reported = future.result()
                    if _reported:
                        output[_entity] = _reported
                    else:
                        _LOG.warning(_head + ' has been unsuccessful.')
                except (Exception, BaseException) as e:
                    _LOG.warning(_head + f' has run into an issue - {e}.')

        return output

    def _attain_entity_sourced_reports(
            self, entity_attr: str, source_to_report: SourceToReport,
            entity_value: Optional[str] = None
    ):
        ref: EntityToSourcedReports = {}
        ajs = self._ambiguous_job_service
        for source, report in source_to_report.items():
            typ = ajs.get_type(item=source)
            uid = ajs.get_attribute(item=source, attr=ID_ATTR)
            head = f'{typ.capitalize()} Job:\'{uid}\''
            value = ajs.get_attribute(item=source, attr=entity_attr)
            value = value or entity_value
            if not value:
                _LOG.warning(f'{head} could not resolve {entity_attr} attr.')
                continue
            scope = ref.setdefault(value, [])
            scope.append((source, report))

        return ref
