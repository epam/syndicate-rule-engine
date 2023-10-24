from http import HTTPStatus
from typing import Iterable, Callable, Dict, Union, Type, List, Iterator, \
    Optional

from handlers.abstracts.abstract_handler import AbstractComposedHandler
from handlers.abstracts.abstract_modular_entity_handler import \
    ModularService, AbstractModularEntityHandler, ENTITY_TEMPLATE
from helpers import retrieve_invalid_parameter_types
from helpers.constants import CUSTOMER_ATTR, NAME_ATTR, CUSTOMER_ACTION, \
    PARAM_COMPLETE, LATEST_LOGIN_ATTR, INHERIT_ATTR, HTTPMethod
from helpers.log_helper import get_logger
from services.modular_service import Customer, Complemented
from services.user_service import CognitoUserService

_LOG = get_logger(__name__)

CUSTOMERS_PATH = '/customers'

FORBIDDEN_ACCESS = 'Access to {} entity is forbidden.'

PROPER_ACCEPTANCE = ('true', 'yes')

AUTHORIZATION = 'Authorization'
SPECIFICATION = 'Query-Specification'
VALIDATION = 'Validation'

ACCESSIBILITY = 'Entity-Accessibility'
AMENDMENT = 'Entity-Amendment'
PERSISTENCE = 'Entity-Persistence'

PERSISTENCE_ERROR = ' does not exist'
RETAIN_ERROR = ' could not be persisted'

RETRIEVED = ' has been retrieved'
INSTANTIATED = ' has been instantiated'

ATTRIBUTE_UPDATED = ' {} has been set to {}'
RETAINED = ' has been persisted'

I_SOURCE_ATTR = 'i_source'
CUSTOMER_ENTITY_ATTR = 'customer_entity'


class BaseCustomerHandler(AbstractModularEntityHandler):

    @property
    def entity(self):
        return CUSTOMER_ACTION.capitalize()


class GetCustomerHandler(BaseCustomerHandler):
    i_source: Union[Iterator, Type[None]]

    def __init__(self, modular_service: ModularService,
                 user_service: CognitoUserService):
        self.user_service = user_service
        super().__init__(modular_service=modular_service)

    def _reset(self):
        super()._reset()
        # Declares a subjected query source-iterator
        self.i_source = None

    def define_action_mapping(self):
        return {CUSTOMERS_PATH: {HTTPMethod.GET: self.get_customer}}

    def get_customer(self, event):
        action = HTTPMethod.GET.capitalize()
        return self._process_action(event=event, action=action)

    @property
    def attributes_to_log(self) -> Dict[Union[str, Type[None]], Iterable[str]]:
        return {
            AUTHORIZATION: [CUSTOMER_ATTR, NAME_ATTR],
            SPECIFICATION: [
                CUSTOMER_ATTR, NAME_ATTR, PARAM_COMPLETE, I_SOURCE_ATTR
            ]
        }

    @property
    def responsibilities(self) -> Dict[
        str, Iterable[Callable[[Dict], Union[Dict, Type[None]]]]
    ]:
        return {
            AUTHORIZATION: self.authorization_responsibility,
            SPECIFICATION: self.specification_responsibilities
        }

    @property
    def authorization_responsibility(self):
        return [self._access_restriction_step]

    @property
    def specification_responsibilities(self):
        return [
            self._obscure_specification_step,
            self._complement_specification_step
        ]

    def _access_restriction_step(self, event: Dict) -> \
            Union[Dict, Type[None]]:
        """
        Mandates access authorization of SYSTEM and ordinary customers,
        allowing action to commence for the primary ones as well as for
        the later ones, which have sent pending inquery, targeting themselves.
        :parameter event: Dict
        :return: Union[Dict, Type[None]]
        """
        customer: str = event.get(CUSTOMER_ATTR)
        if customer is not None:
            name: Union[str, Type[None]] = event.get(NAME_ATTR, None)
            if name and name != customer:
                event = None
                self._code = HTTPStatus.FORBIDDEN
                self._content = FORBIDDEN_ACCESS.format(self.entity)
        return event

    def _obscure_specification_step(self, event: Dict) -> \
            Union[Dict, Type[None]]:
        """
        Mandates concealing specification, based on given query,
        either following:
        1. Given a customer `name` has been issued, respectively
         amends said specification to retrieve a demanded entity.
        2. Given a non-system customer has issued one, obscures
         the view to the said customer.
        :parameter event: Dict
        :return: Union[Dict, Type[None]]
        """

        customer = event.get(CUSTOMER_ATTR)
        name = event.get(NAME_ATTR)
        _any = any((customer, name))
        self.i_source = iter([customer or name]) if _any else self.i_source
        return event

    def _complement_specification_step(self, event: Dict) \
            -> Union[Dict, Type[None]]:
        """
        Mandates parent-complement specification, based on given query.
        Given a `complete` parameter has been issued, respectively
        amends said specification to retrieve an infused entity.
        :parameter event: Dict
        :return: Union[Dict, Type[None]]
        """
        if event.get(PARAM_COMPLETE):
            self.i_source = self.i_query
        return event

    @property
    def i_query(self) -> Iterator:
        """
        Produces a query iterator-like output, based on the pending
        specification, resetting it afterwards.
        :return: Iterator
        """
        i_source = self.i_source

        if i_source:
            query = self.modular_service.i_get_customer(i_source)
        else:
            query = self.modular_service.i_get_customers()

        self.i_source = None
        return query

    def _produce_response_dto(self, event: Optional[Dict] = None) -> \
            Union[str, Dict, List, Type[None]]:
        """
        Mandates derivation of a query-response data transfer object,
        based on a pending source-iterator. Apart from that,
        for each entity, a user-respective latest-login is injected
        into attached customer dto.
        :parameter event: Optional[Dict]
        :return: Union[Dict, List, Type[None]]
        """
        attr = NAME_ATTR
        i_query, dto = self.i_query, []
        get_dto = self.modular_service.get_dto
        get_logins = self.user_service.get_customers_latest_logins

        customers: list = [get_dto(each) for each in i_query]
        names: list = [each.get(attr) for each in customers if attr in each]

        login_reference: dict = get_logins(names) if names else {}

        for customer in customers:
            name = customer.get(attr)
            if name in login_reference:
                customer[LATEST_LOGIN_ATTR] = login_reference[name]

        return customers


class PatchCustomerHandler(BaseCustomerHandler):
    customer_entity: Union[Customer, Complemented, Type[None]]

    def __init__(self, modular_service: ModularService):
        super().__init__(modular_service=modular_service)

    def _reset(self):
        super()._reset()
        # Declares request-pending customer entity
        self.customer_entity = None

    def define_action_mapping(self):
        return {CUSTOMERS_PATH: {HTTPMethod.PATCH: self.patch_customer}}

    def patch_customer(self, event):
        action = HTTPMethod.PATCH.capitalize()
        return self._process_action(event=event, action=action)

    @property
    def attributes_to_log(self) -> Dict[Union[str, Type[None]], Iterable[str]]:
        return {
            VALIDATION: list(self._patch_required_map),
            ACCESSIBILITY: [NAME_ATTR, CUSTOMER_ENTITY_ATTR],
            AMENDMENT: [CUSTOMER_ENTITY_ATTR] + self._amend_attribute_list,
            PERSISTENCE: [CUSTOMER_ENTITY_ATTR]
        }

    @property
    def responsibilities(self) -> Dict[
        str, Iterable[Callable[[Dict], Union[Dict, Type[None]]]]
    ]:
        return {
            VALIDATION: self.validation_responsibilities,
            ACCESSIBILITY: self.access_responsibilities,
            AMENDMENT: self.amendment_responsibilities,
            PERSISTENCE: self.persistence_responsibilities
        }

    @property
    def validation_responsibilities(self):
        return [self._validate_payload_types_step]

    @property
    def access_responsibilities(self):
        return [
            self._access_customer_entity_step
        ]

    @property
    def amendment_responsibilities(self):
        return [self._amend_customer_parent_step]

    @property
    def persistence_responsibilities(self):
        return [self._persist_customer_parent_step]

    @property
    def _patch_required_map(self):
        return {CUSTOMER_ATTR: str, INHERIT_ATTR: bool}

    @property
    def _amend_attribute_list(self):
        return [INHERIT_ATTR]

    def _validate_payload_types_step(self, event: Dict):
        """
        Mandates payload parameters for the patch event, adhering
        to the respective requirement map.
        :parameter event: Dict
        :return: Union[Dict, Type[None]]
        """
        content = retrieve_invalid_parameter_types(
            event=event, required_param_types=self._patch_required_map
        )
        if content:
            event = None
            self._code = HTTPStatus.BAD_REQUEST
            self._content = content

        return event

    def _access_customer_entity_step(self, event: Dict):
        """
        Mandates complemented Customer Parent entity retrieval and lack of
        persistence.
        Response variables are assigned, under the following predicates:
        Given absence: `code` - 404, `content` - persistence reason.
        :parameter event: Dict
        :return: Union[Dict, Type[None]]
        """

        name = event.get(CUSTOMER_ATTR, '')
        _template = ENTITY_TEMPLATE.format(entity=self.entity, id=name)

        parent = self.modular_service.get_customer_bound_parent(name)
        if not parent:
            event = None
            self._code = HTTPStatus.FORBIDDEN
            self._content = 'Custodian customer parent does not exist. ' \
                            'Cannot set inherit'
            return
        customer = self.modular_service.get_customer(name)
        self.customer_entity = Complemented(
            entity=customer, complement=parent
        )

        _LOG.info(_template + RETRIEVED)
        return event

    def _amend_customer_parent_step(self, event: Dict):
        """
        Mandates complemented Customer Parent entity patching.\n
        Response variables are assigned, under the following predicates:
        \nGiven absence: `code` - 404, `content` - persistence reason.
        :parameter event: Dict
        :return: Union[Dict, Type[None]]
        """
        entity: Complemented = self.customer_entity
        if entity:
            head = ENTITY_TEMPLATE.format(entity=self.entity, id=entity.name)
            for attribute in self._amend_attribute_list:
                if attribute in event:
                    args = attribute, event.get(attribute)
                    setattr(entity, *args)
                    _LOG.info(head + ATTRIBUTE_UPDATED.format(*args))
        else:
            _LOG.error('Execution step missing \'customer_entity\' variable.')
            event = None

        return event

    def _persist_customer_parent_step(self, event: Dict):
        """
        Mandates Complemented Customer Parent entity persistence.\n
        Response variables are assigned, under the following predicates:
        \nGiven absence: `code` - 500, `content` - failed to retain reason.
        :parameter event: Dict
        :return: Union[Dict, Type[None]]
        """
        entity: Complemented = self.customer_entity
        if not entity:
            _LOG.error('Execution step missing \'customer_entity\' variable.')
            return None

        _template = ENTITY_TEMPLATE.format(entity=self.entity, id=entity.name)
        modular = self.modular_service
        if entity and modular.save(entity=entity):
            _LOG.info(_template + RETAINED)
        elif entity:
            event = None
            _LOG.error(_template + RETAIN_ERROR)

        return event

    def _produce_response_dto(self, event: Optional[Dict] = None) -> \
            Union[str, Dict, List, Type[None]]:
        """
        Mandates derivation of a patch-response data transfer object,
        based on a pending Customer Parent entity.
        :parameter event: Dict
        :return: Union[Dict, List, Type[None]]
        """
        get_dto = self.modular_service.get_dto
        entity, self.customer_entity = self.customer_entity, None
        return get_dto(entity=entity) or []


class CustomerHandler(AbstractComposedHandler):
    ...


def instantiate_customer_handler(modular_service: ModularService,
                                 user_service: CognitoUserService):
    patch_customer_handler = PatchCustomerHandler(
        modular_service=modular_service)
    get_customer_handler = GetCustomerHandler(
        modular_service=modular_service, user_service=user_service
    )
    return CustomerHandler(
        resource_map={
            CUSTOMERS_PATH: {
                HTTPMethod.GET: get_customer_handler,
            }
        }
    )
