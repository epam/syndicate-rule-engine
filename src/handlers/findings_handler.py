from typing import Dict, Union, List, Optional

from handlers.abstracts.abstract_handler import AbstractHandler
from helpers import RESPONSE_OK_CODE, RESPONSE_BAD_REQUEST_CODE, \
    RESPONSE_RESOURCE_NOT_FOUND_CODE, RESPONSE_INTERNAL_SERVER_ERROR
from helpers import build_response
from helpers.constants import (
    ACCOUNT_ATTR, GET_METHOD, GET_URL_ATTR,
    DELETE_METHOD, CUSTOMER_ATTR, TENANT_ATTR
)
from helpers.log_helper import get_logger
from models.modular.tenants import Tenant
from services.findings_service import FindingsService, MAP_TYPE_ATTR, \
    MAP_KEY_ATTR
from services.modular_service import ModularService

FINDINGS_PATH = '/findings'

EXPAND_ON_ATTR = 'expand_on'
RAW_ATTR = 'raw'
FILTER_ATTR = 'filter'
DATA_TYPE_ATTR = 'data_type'

REGIONS_TO_INCLUDE_ATTR = 'regions_to_include'
RULES_TO_INCLUDE_ATTR = 'rules_to_include'
RESOURCE_TYPES_TO_INCLUDE_ATTR = 'resource_types_to_include'
SEVERITIES_TO_INCLUDE_ATTR = 'severities_to_include'
DEPENDENT_INCLUSION_ATTR = 'dependent_inclusion'

FINDINGS_PERSISTENCE_ERROR = 'Tenant: \'{identifier}\' has no persisted '\
                             'findings state.'

FINDINGS_DEMAND_STORE_INACCESSIBLE = 'Request to store \'{identifier}\' ' \
                                     'Account Findings state has not been' \
                                     ' successful.'
FINDINGS_DEMAND_URL_INACCESSIBLE = 'Request to generate \'{identifier}\' ' \
                                   'Account Findings state has not been' \
                                   ' successful.'
GENERIC_DEMAND_URL_INACCESSIBLE = 'URL to retrieve Findings state of' \
                                  ' \'{identifier}\' account could not' \
                                  ' be provided.'

FINDINGS_CLEARED = 'Findings state bound to \'{identifier}\' tenant ' \
                   'has been cleared.'

ACCOUNT_PERSISTENCE_ERROR = 'Tenant \'{identifier}\' not found.'

MAP_VALIDATION_KEYS = ('required_type', 'required')

PRESIGNED_URL_PARAM = 'presigned_url'

_LOG = get_logger(__name__)


class FindingsHandler(AbstractHandler):
    """Handles the latest Findings, bound to account """

    FILTER_KEYS: tuple = (
        RULES_TO_INCLUDE_ATTR, REGIONS_TO_INCLUDE_ATTR,
        RESOURCE_TYPES_TO_INCLUDE_ATTR, SEVERITIES_TO_INCLUDE_ATTR
    )

    def __init__(self, service: FindingsService,
                 modular_service: ModularService):
        self._service = service
        self._modular_service = modular_service

    def define_action_mapping(self):
        return {
            FINDINGS_PATH: {
                GET_METHOD: self.get_findings,
                DELETE_METHOD: self.delete_findings
            }
        }

    def get_findings(self, event):
        """
        Retrieves Findings state bound to an account, driven by
        an event. An `account` field, with a respective value, must
        derive an entity, cloud identifier of which is used to get the latest
        related state, mapped content of which is inverted and expanded
        according to the `expand_on` parameter, default value of which is
        `resources`.
        """
        # self._validate_get_findings(event)
        _tenant_name = event.get(TENANT_ATTR)
        _expansion = event.get(EXPAND_ON_ATTR)
        _raw = event.get(RAW_ATTR)
        _tenant = self._get_entity(event)

        self._handle_consequence(
            commenced=bool(_tenant), identifier=_tenant_name,
            code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
            respond=ACCOUNT_PERSISTENCE_ERROR
        )

        _identifier = _tenant.project
        _findings = self._service.get_findings_content(_identifier)
        if _raw:
            _content = _findings
            _LOG.info('Returning raw findings content')
        else:
            # Expands collection into an iterable sequence of vulnerability items
            _iterator = self._service.expand_content(_findings, _expansion)

            # Instantiate a filterable iterator
            _filter_relation = self._get_filter_key_relation()
            _filters = self._get_map_from_reference(event, _filter_relation)

            # Injects dependency (given any has been given) among provided filters
            _dependent = bool(event.get(DEPENDENT_INCLUSION_ATTR))

            _iterator = self._service.filter_iterator(
                iterator=_iterator, dependent=_dependent, **_filters
            )

            # Formats expanded Findings, filtered state in on-demand collection
            _map_key = event.get(MAP_KEY_ATTR)
            if _map_key:
                # Map collection type has been chosen
                # Instantiates an extractive iterator
                _iterator = self._service.extractive_iterator(
                    iterator=_iterator, key=_map_key
                )

            # Formats items in the derived iterator
            _data_type = event.get(DATA_TYPE_ATTR)
            _content = self._service.format_iterator(
                iterator=_iterator, key=_data_type
            )

            if event.get(GET_URL_ATTR):
                _content = self._get_content_url(
                    content=_content, identifier=_identifier, name=_tenant_name
                )

        return build_response(code=RESPONSE_OK_CODE, content=_content)

    def delete_findings(self, event):
        """
        Removes the latest Findings state, bound to an account, driven by
        an event. An `account` field, with a respective value, must
        derive an entity, cloud identifier of which is used to get the latest
        related state.
        """
        tenant_name = event.get(TENANT_ATTR)
        _tenant = self._get_entity(event)
        self._handle_consequence(
            commenced=bool(_tenant), identifier=tenant_name,
            code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
            respond=ACCOUNT_PERSISTENCE_ERROR
        )
        _removed = self._service.delete_findings(_tenant.project)
        self._handle_consequence(
            commenced=bool(_removed), identifier=tenant_name,
            code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
            respond=FINDINGS_PERSISTENCE_ERROR
        )
        _message = FINDINGS_CLEARED.format(identifier=tenant_name)
        return build_response(content=_message)

    def _get_entity(self, event: Dict) -> Optional[Tenant]:
        """
        Retrieves Account entity, restricting respective access for non
        bound customers.
        :return: Optional[Tenant]
        """
        customer = event.get(CUSTOMER_ATTR)
        entity = self._modular_service.get_tenant(
            event.get(TENANT_ATTR)
        )
        if not entity or not entity.is_active or \
                customer and entity.customer_name != customer:
            return
        return entity

    def _get_content_url(self, content: Union[Dict, List],
                         identifier: str, name: str) -> Dict:
        """
        Mandates Findings state retrieval, driven by designated storage and
        presigned URL reference, having previously outsourced the given
        content.
        :param content:Union[Dict, List]
        :param identifier:str
        :param name:str
        """
        _demand = self._service.get_demand_folder()
        _put = self._service.put_findings(content, identifier, _demand)

        self._handle_consequence(
            commenced=bool(_put), identifier=name,
            code=RESPONSE_INTERNAL_SERVER_ERROR,
            respond=GENERIC_DEMAND_URL_INACCESSIBLE,
            log=FINDINGS_DEMAND_STORE_INACCESSIBLE,
            error=True
        )

        _url = self._service.get_findings_url(identifier, _demand)

        self._handle_consequence(
            commenced=bool(_url), identifier=name,
            code=RESPONSE_INTERNAL_SERVER_ERROR,
            respond=GENERIC_DEMAND_URL_INACCESSIBLE,
            log=FINDINGS_DEMAND_URL_INACCESSIBLE,
            error=True
        )

        return {PRESIGNED_URL_PARAM: _url}

    @staticmethod
    def _handle_consequence(commenced: bool, identifier: str, code: int,
                            respond: str, log: str = None,
                            error: bool = False):
        """
        Mandates the consequences of actions, bound to an Account
        entity, derived by a given identifier. Raises a CustodianException
        with provided `code`, `log` and `respond` messages,
        given the action has not successfully `commenced`.
        :parameter commenced:bool
        :parameter identifier:str
        :parameter code: int
        :parameter respond:str
        :parameter log:str
        :raises: CustodianException
        :return None:
        """
        log = log or respond
        if not commenced:
            respond = respond.format(identifier=identifier)
            log = log.format(identifier=identifier)
            _log_action = _LOG.error if error else _LOG.warning
            _log_action(log)
            return build_response(code=code, content=respond)

    @staticmethod
    def _delete_requirement_map() -> Dict[str, Dict]:
        return {ACCOUNT_ATTR: dict(required_type=str, required=True)}

    @staticmethod
    def _filter_requirement_map():
        return {attr: dict(required_type=str, required=False)
                for attr in FindingsHandler.FILTER_KEYS}

    @staticmethod
    def _get_map_from_reference(subject: Dict, reference: Dict):
        return {(_sub or _target): subject[_target]
                for _target, _sub in reference.items() if _target in subject}

    @staticmethod
    def _get_filter_key_relation():
        return dict(
            zip(FindingsHandler.FILTER_KEYS, FindingsService.FILTER_KEYS)
        )

    @staticmethod
    def _map_key_requirement_map(required: bool):
        return {MAP_KEY_ATTR: dict(required_type=str, required=required)}

    @classmethod
    def _validate_requirement_map(cls, subject: Dict,
                                  requirement_map: Dict[str, Dict]):
        for attr, requirement in requirement_map.items():
            cls._validate_type(attr, subject.get(attr), **requirement)
