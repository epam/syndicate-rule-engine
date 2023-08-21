from typing import Iterable, Optional, Callable, Dict, Union, Type, List, \
    Iterator

from helpers import retrieve_invalid_parameter_types, \
    BAD_REQUEST_IMPROPER_TYPES, RESPONSE_BAD_REQUEST_CODE, \
    RESPONSE_RESOURCE_NOT_FOUND_CODE, RESPONSE_OK_CODE, RESPONSE_CONFLICT

from helpers.constants import GET_METHOD, POST_METHOD, DELETE_METHOD,\
    PATCH_METHOD, LICENSE_KEYS_ATTR, \
    CUSTOMER_ATTR, TENANT_ATTR, TENANTS_ATTR, \
    GOVERNANCE_ENTITY_ID_ATTR, GOVERNANCE_ENTITY_TYPE_ATTR, RULESET_ATTR, \
    ALLOWED_GOVERNANCE_ENTITY_TYPE_ATTRS, ACCOUNT_ATTR, \
    LICENSE_PRIORITY_TYPE_ATTR, LICENSE_KEYS_TO_APPEND_ATTR,\
    LICENSE_KEYS_TO_PREPEND_ATTR, LICENSE_KEYS_TO_DETACH_ATTR

from handlers.abstracts.abstract_handler import AbstractComposedHandler

from handlers.abstracts.abstract_modular_entity_handler import \
    ModularService, ENTITY_TEMPLATE, AbstractModularEntityHandler

from services.modular_service import Tenant, Complemented
from services.rbac.governance.priority_governance_service import\
    PriorityGovernanceService, MANAGEMENT_ID_ATTR, GOVERNANCE_ATTR, \
    MANAGEMENT_ATTR
from services.license_service import LicenseService
from services.ruleset_service import RulesetService, Ruleset

from helpers.log_helper import get_logger

_LOG = get_logger('tenant_license_priority_handler')

TENANTS_LICENSE_PRIORITIES_PATH = '/tenants/license-priorities'

FORBIDDEN_ACCESS = 'Access to {} entity is forbidden.'

AUTHORIZATION = 'Authorization'
SPECIFICATION = 'Query-Specification'
VALIDATION = 'Validation'
ACCESSIBILITY = 'Entity-Accessibility'
INSTANTIATION = 'Entity-Instantiation'
AMENDMENT = 'Entity-Amendment'
PERSISTENCE = 'Entity-Persistence'

SPECIFICATION_ATTR = 'specification'

PERSISTENCE_ERROR = ' does not exist'
RETAIN_ERROR = ' could not be persisted'

RETRIEVED = ' has been retrieved'
INSTANTIATED = ' has been instantiated'
REMOVED = ' has been removed'
CONFLICT = ' already exists'

ATTRIBUTE_UPDATED = ' {} has been set to {}'
RETAINED = ' has been persisted'

I_SOURCE_ATTR = 'i_source'
CUSTOMER_ENTITY_ATTR = 'customer_entity'

ACCOUNTS_ATTR = ACCOUNT_ATTR + 's'

PRIORITY_ID_ATTR = 'priority_id'


class BaseTenantLicensePriorityHandler(AbstractModularEntityHandler):

    tenant_entity: Optional[Union[Tenant, Complemented]]
    governance_entity_type: Optional[str]

    def __init__(
        self, modular_service: ModularService,
        priority_governance_service: PriorityGovernanceService
    ):
        self.priority_governance_service = priority_governance_service
        # Attaches resource specific attributes.
        self.priority_governance_service.entity_type = Tenant
        self.priority_governance_service.managed_attr = LICENSE_KEYS_ATTR
        self.priority_governance_service.delegated_attr = ACCOUNTS_ATTR
        super().__init__(modular_service=modular_service)

    def _reset(self):
        # Mutative action-applicable.
        super()._reset()
        self.tenant_entity = None
        self.mid = None
        self.governance_entity_type = None

    @property
    def entity(self) -> str:
        """
        :return: str, `Tenant-License-Priority`
        """
        subtype = LICENSE_PRIORITY_TYPE_ATTR.split('_')
        subtype = '-'.join(each.capitalize() for each in subtype)
        return f'{TENANT_ATTR.capitalize()}-{subtype}'

    def _get_expanded_dto(self, dto: Dict[str, Union[str, Dict]]):
        """
        Expands given object-aggregated management-dto into a list of separate
        managed dto(s).
        :parameter dto: Dict[str, Union[str. Dict]]
        :return: List[Dict[str, Union[str, Dict]]]
        """
        expanded = []
        managed_attr = self.priority_governance_service.managed_attr
        mid = dto.get(MANAGEMENT_ID_ATTR)
        tenant = dto.get(TENANT_ATTR) or self.tenant_entity.name
        for key, value in (dto.get(managed_attr) or dict()).items():
            _dto = {
                TENANT_ATTR: tenant,
                PRIORITY_ID_ATTR: mid,
                self.governance_entity_type: key,
                managed_attr: value
            }
            expanded.append(_dto)
        return expanded

    def _validate_governance_entity_data_step(self, event: Dict):
        """
        Mandates `governance_entity_type` data validation and persistence.\n
        Response variables are assigned, under the following predicates:
        \nGiven absence: `code` - 404, `content` - persistence reason.
        :parameter event: Dict
        :return: Union[Dict, Type[None]]
        """
        _allowed = ALLOWED_GOVERNANCE_ENTITY_TYPE_ATTRS
        _label = GOVERNANCE_ENTITY_TYPE_ATTR
        gvc_type: str = event.get(_label)

        if gvc_type not in _allowed:
            issue = f'Value of \'{_label}\' parameter must reflect ' \
                    f'one of the following value(s): {_allowed}.'

            self._code = RESPONSE_BAD_REQUEST_CODE
            self._content = f'Bad request. {issue}'
            return None

        self.governance_entity_type = gvc_type

        entity = self.entity.upper()
        _, *subtype = entity.split('-')
        subtype = '_'.join(map(str.upper, subtype))

        _type = f'{gvc_type.upper()}_{subtype}'
        # Derives $ENTITY_LICENSE_PRIORITY, ie RULESET_LICENSE_PRIORITY
        _LOG.info(f'Deriving \'{_type}\' {GOVERNANCE_ATTR.lower()}.')

        self.priority_governance_service.governance_type = _type
        return event


class CommandTenantLicensePriorityHandler(BaseTenantLicensePriorityHandler):

    tenant_entity: Optional[Union[Tenant, Complemented]]
    mid: Optional[str]

    # Post and Patch mutative action variables.
    ruleset_entity: Optional[Ruleset]
    license_service: Optional[LicenseService]

    def __init__(
        self, modular_service: ModularService,
        priority_governance_service: PriorityGovernanceService
    ):
        super().__init__(
            modular_service=modular_service,
            priority_governance_service=priority_governance_service
        )

    def _reset(self):
        super()._reset()
        # Mutative action-applicable attributes.
        self.tenant_entity = None
        self.mid = None

    def _validate_customer_data_step(self, event: dict):
        """
        Designated to validate for optionally issued `customer` data, issued
        by a non-system user.
        Note: meant to be shared across command-handlers.
        :param event: dict
        :return: Optional[dict]
        """
        customer: Optional[str] = event.get(CUSTOMER_ATTR, None)

        if customer is not None and not isinstance(customer, str):
            event = None
            self._code = RESPONSE_BAD_REQUEST_CODE
            self._content = BAD_REQUEST_IMPROPER_TYPES.format(
                f'\'{CUSTOMER_ATTR}\':str'
            )
        return event

    def _access_tenant_entity_step(self, event: Dict):
        """
        Mandates Tenant entity retrieval and lack of persistence.\n
        Response variables are assigned, under the following predicates:
        \nGiven absence: `code` - 404, `content` - persistence reason.
        :parameter event: Dict
        :return: Union[Dict, Type[None]]
        """
        priority_service = self.priority_governance_service
        name = event.get(TENANT_ATTR)
        customer = event.get(CUSTOMER_ATTR)
        _template = ENTITY_TEMPLATE.format(entity=self.entity, id=name)

        entity = None
        if customer and name:
            fetcher = self.modular_service.i_get_customer_tenant
            iterator = fetcher(customer=customer, name=name, active=True)
            entity = next(iterator, None)
        elif name:
            entity = name

        if entity:
            entity = priority_service.get_entity(entity=entity)

        self.tenant_entity = entity

        if not self.tenant_entity:
            event = None
            self._code = RESPONSE_RESOURCE_NOT_FOUND_CODE
            self._content = _template + PERSISTENCE_ERROR

        return event

    def _access_license_relation_entity_step(self, event: Dict):
        """
        Mandates Tenant entity access to a pending licensed ruleset, as well
        as lack of persistence and license-key(s) attachment.\n
        Response variables are assigned, under the following predicates:
        \nGiven absence: `code` - 404, `content` - persistence reason.
        :parameter event: Dict
        :return: Union[Dict, Type[None]]
        """
        # Generalized by license_keys attr.
        license_keys = event.get(LICENSE_KEYS_ATTR)
        if not license_keys:
            _LOG.error('Execution step is missing'
                       f' \'{LICENSE_KEYS_ATTR}\' variable.')
            return None

        ruleset = self.ruleset_entity
        if not ruleset:
            _LOG.error('Execution step is missing \'ruleset\' variable.')
            return None

        entity: Complemented = self.tenant_entity

        _template = ENTITY_TEMPLATE.format(entity=self.entity, id=entity.name)

        for license_key in license_keys:
            head = ENTITY_TEMPLATE.format(entity='License', id=license_key)
            if license_key not in ruleset.license_keys:
                issue = PERSISTENCE_ERROR + f' within \'{ruleset.id}\' Ruleset'
                self._code = RESPONSE_RESOURCE_NOT_FOUND_CODE
                self._content = head + issue
                event = None
                break

            _license = self.license_service.get_license(license_id=license_key)
            if not _license:
                # Unlikely to occur - as consequence of improper license-sync.
                _LOG.error(head + PERSISTENCE_ERROR)
                self._code = RESPONSE_RESOURCE_NOT_FOUND_CODE
                self._content = head + PERSISTENCE_ERROR
                event = None
                break

            if not self.license_service.is_subject_applicable(
                    entity=_license, customer=entity.customer_name,
                    tenant=entity.name
            ):
                _LOG.warning(head + f' is not \'{entity.name}\' tenant'
                                    ' applicable.')
                self._code = RESPONSE_RESOURCE_NOT_FOUND_CODE
                self._content = head + PERSISTENCE_ERROR
                event = None

        return event

    def _persist_priority_management_step(self, event: Dict):
        """
        Mandates Settings-complemented Tenant entity persistence.\n
        Response variables are assigned, under the following predicates:
        \nGiven absence: `code` - 500, `content` - failed to retain reason.
        :parameter event: Dict
        :return: Union[Dict, Type[None]]
        """
        entity: Complemented = self.tenant_entity
        mid = self.mid or event.get(MANAGEMENT_ID_ATTR)
        if not entity:
            _LOG.error('Variable \'tenant_entity\' is  missing.')
            return None

        _template = ENTITY_TEMPLATE.format(entity=self.entity, id=entity.name)
        if self.modular_service.save(entity=entity):
            label = self.priority_governance_service.governance_type
            message = f'{mid} {label}' if mid else f' \'{label}\''
            _LOG.info(_template + message + RETAINED)
        else:
            event = None
            self._content = _template + RETAIN_ERROR

        return event

    def _produce_response_dto(self, event: Optional[Dict] = None) -> \
            Union[str, Dict, List, Type[None]]:
        """
        Mandates derivation of a patch-response data transfer object,
        based on a pending Customer Parent entity.
        :parameter event: Dict
        :return: Union[Dict, List, Type[None]]
        """
        entity: Complemented = self.tenant_entity
        mid: str = self.mid or event.get(MANAGEMENT_ID_ATTR)
        if not entity:
            _LOG.error('Variable \'tenant_entity\' is missing.')
            return None
        if mid in entity.management:
            get_dto = self.priority_governance_service.get_management
            response = get_dto(entity=entity, mid=mid)
            response = self._get_expanded_dto(dto=response or dict())
        else:
            response = []
        return response


class GetTenantLicensePriorityHandler(BaseTenantLicensePriorityHandler):
    def __init__(
        self, modular_service: ModularService,
        priority_governance_service: PriorityGovernanceService
    ):
        super().__init__(
            modular_service=modular_service,
            priority_governance_service=priority_governance_service
        )
        # Declares a subjected query specification
        self.specification: Dict = dict()

    def define_action_mapping(self):
        return {
            TENANTS_LICENSE_PRIORITIES_PATH: {GET_METHOD: self.get}
        }

    def get(self, event):
        action = GET_METHOD.capitalize()
        return self._process_action(event=event, action=action)

    @property
    def attributes_to_log(self) -> Dict[Union[str, Type[None]], Iterable[str]]:
        return {
            VALIDATION: [GOVERNANCE_ENTITY_TYPE_ATTR],
            SPECIFICATION: [
                CUSTOMER_ATTR, TENANT_ATTR, GOVERNANCE_ENTITY_ID_ATTR,
                TENANTS_ATTR
            ]
        }

    @property
    def responsibilities(self) -> Dict[
        str, Iterable[Callable[[Dict], Union[Dict, Type[None]]]]
    ]:
        return {
            VALIDATION: self.validation_responsibility,
            SPECIFICATION: self.specification_responsibilities
        }

    @property
    def validation_responsibility(self):
        return [
            self._validate_governance_entity_data_step
        ]

    @property
    def specification_responsibilities(self):
        return [
            self._obscure_specification_step,
            self._name_specification_step,
            self._governance_entity_id_specification_step,
            self._priority_id_specification_step
        ]

    @property
    def i_query(self) -> Iterator:
        """
        Produces a query iterator-like output, based on the pending
        specification, resetting it afterwards, having installed the query:
         1. Given only tenant-names, retrieves demanded entities.
         2. Given a customer name, fetches every entity belonging to the
          customer, singling out active tenants, provided any.
         3. Otherwise, retrieves every active tenant entity.

         Afterwards for each tenant, produced by said query, respective
         Tenant-Setting based priority-governance is derived, and sub-queried:
          1. Based on a priority-id, which reflects to management-id
          2. Based on a governance entity, i.e. ruleset, within said priority

        :return: Iterator
        """
        specification = self.specification

        tenants: Optional[List[str]] = specification.get(TENANTS_ATTR)
        tenant: Optional[str] = specification.get(TENANT_ATTR)
        customer: Optional[str] = specification.get(CUSTOMER_ATTR)
        # Priority id.
        priority_id: Optional[str] = specification.get(MANAGEMENT_ID_ATTR)
        # Managed entity id, i.e. ruleset-id
        gvc_entity_id: Optional[str] = specification.get(
            GOVERNANCE_ENTITY_ID_ATTR
        )
        self.specification = dict()
        if customer and not tenants:
            # Conceals response view, given a non-system request's been issued.
            query = self.modular_service.i_get_customer_tenant(
                customer=customer, name=tenant, active=True
            )
        elif tenant or tenants:
            if tenant:
                query = self.modular_service.i_get_tenant(iterator=iter([tenant]))
            else:
                # Given `tenants` must be related to `customer`, given any.
                query = self.modular_service.i_get_tenant(iterator=iter(tenants))
                if customer:
                    query = (t for t in query if t.customer_name == customer)
            # Get active only.
            query = (t for t in query if t.is_active)
        else:
            query = self.modular_service.i_get_tenants(active=True)

        for tenant in query:
            entity = self.priority_governance_service.get_entity(entity=tenant)
            if not entity:
                continue

            if priority_id:
                data = self.priority_governance_service.get_management(
                    entity=entity, mid=priority_id
                )
                subquery = iter([data] if data else [])
            else:
                subquery = self.priority_governance_service.i_get_management(
                    entity=entity
                )
            for data in subquery:
                attr = self.priority_governance_service.managed_attr

                managed = data.get(attr, {})
                if gvc_entity_id in managed:
                    # Retrieves managed-entity id data.
                    managed = {gvc_entity_id: managed.get(gvc_entity_id)}
                elif gvc_entity_id:
                    managed = {}

                data[attr] = managed
                if data[attr]:
                    data[TENANT_ATTR] = entity.name
                    for each in self._get_expanded_dto(dto=data):
                        yield each

    def _obscure_specification_step(self, event: Dict) -> \
            Union[Dict, Type[None]]:
        """
        Mandates concealing specification, based on given query.
        Obscures the view, based on:
        * tenant-specific restrictions
        * customer scope from the non-system issuer.
        :parameter event: Dict
        :return: Union[Dict, Type[None]]
        """
        tenants = event.get(TENANTS_ATTR)
        customer = event.get(CUSTOMER_ATTR)
        if isinstance(tenants, list) and tenants:
            self.specification[TENANTS_ATTR] = tenants

        if customer:
            self.specification[CUSTOMER_ATTR] = customer

        return event

    def _name_specification_step(self, event: Dict):
        """
        Mandates Tenant name specification, based on given query.
        Given the aforementioned partition attribute, alters
        specification with a list based on said name.
        :parameter event: Dict
        :return: Union[Dict, Type[None]]
        """
        tenant = event.get(TENANT_ATTR)
        if tenant:
            self.specification[TENANT_ATTR] = tenant
        return event

    def _governance_entity_id_specification_step(self, event: Dict):
        """
        Mandates entity-id specification of a governance entity,
        based on given query. Given the aforementioned partition attribute,
        alters specification with a list based on said name.
        :parameter event: Dict
        :return: Union[Dict, Type[None]]
        """
        # Declares governance_entity_id, based on a respective entity-type.
        gvc_entity_id = event.get(GOVERNANCE_ENTITY_ID_ATTR)
        if gvc_entity_id:
            self.specification[GOVERNANCE_ENTITY_ID_ATTR] = gvc_entity_id
        return event

    def _priority_id_specification_step(self, event: Dict):
        """
        Mandates priority-id specification, based on given query.
        Given the aforementioned partition attribute, alters
        specification with a list based on said name.
        :parameter event: Dict
        :return: Union[Dict, Type[None]]
        """
        priority_id = event.get(MANAGEMENT_ID_ATTR)
        if priority_id:
            self.specification[MANAGEMENT_ID_ATTR] = priority_id
        return event

    def _produce_response_dto(self, event: Optional[Dict] = None) -> \
            Union[str, Dict, List, Type[None]]:
        """
        Mandates derivation of a query-response data transfer object,
        based on a pending source-iterator. Apart from that,
        for each entity, a user-respective latest-login is injected
        into attached customer dto.
        :parameter event: Dict
        :return: Union[Dict, List, Type[None]]
        """
        return [*self.i_query]


class PostTenantLicensePriorityHandler(CommandTenantLicensePriorityHandler):
    def __init__(
        self, modular_service: ModularService, license_service: LicenseService,
        priority_governance_service: PriorityGovernanceService,
        ruleset_service: RulesetService
    ):

        # Declares a subjected query specification
        self.license_service = license_service
        self.ruleset_service = ruleset_service

        super().__init__(
            modular_service=modular_service,
            priority_governance_service=priority_governance_service
        )

    def _reset(self):
        super()._reset()
        # Declares request-pending customer entity
        self.ruleset_entity: Optional[Ruleset] = None

    def define_action_mapping(self):
        return {
            TENANTS_LICENSE_PRIORITIES_PATH: {
                POST_METHOD: self.post_license_priority
            }
        }

    def post_license_priority(self, event):
        action = POST_METHOD.capitalize()
        return self._process_action(event=event, action=action)

    @property
    def attributes_to_log(self) -> Dict[Union[str, Type[None]], Iterable[str]]:
        return {
            VALIDATION: list(self._post_required_map) + [CUSTOMER_ATTR],
            ACCESSIBILITY: [
                TENANT_ATTR, GOVERNANCE_ENTITY_ID_ATTR,
                LICENSE_KEYS_ATTR, MANAGEMENT_ID_ATTR
            ],
            INSTANTIATION: [
                'tenant_entity', 'mid', MANAGEMENT_ID_ATTR
            ],
            PERSISTENCE: ['tenant_entity', 'mid']
        }

    @property
    def responsibilities(self) -> Dict[
        str, Iterable[Callable[[Dict], Union[Dict, Type[None]]]]
    ]:
        return {
            VALIDATION: self.validation_responsibilities,
            ACCESSIBILITY: self.access_responsibilities,
            INSTANTIATION: self.instantiation_responsibilities,
            PERSISTENCE: self.persistence_responsibilities
        }

    @property
    def validation_responsibilities(self):
        return [
            self._validate_payload_types_step,
            self._validate_customer_data_step,
            self._validate_governance_entity_data_step
        ]

    @property
    def access_responsibilities(self):
        return [
            self._access_tenant_entity_step,
            self._access_priority_management_data_step,
            self._access_ruleset_entity_step,
            self._access_license_relation_entity_step
        ]

    @property
    def instantiation_responsibilities(self):
        return [
            self._instantiate_data_within_priority_step,
            self._instantiate_priority_management_step
        ]

    @property
    def persistence_responsibilities(self):
        return [self._persist_priority_management_step]

    @property
    def _post_required_map(self):
        return {
            TENANT_ATTR: str,
            LICENSE_KEYS_ATTR: list,
            GOVERNANCE_ENTITY_TYPE_ATTR: str,
            GOVERNANCE_ENTITY_ID_ATTR: str
        }

    def _validate_payload_types_step(self, event: Dict):
        """
        Mandates payload parameters for the patch event, adhering
        to the respective requirement map.
        :parameter event: Dict
        :return: Union[Dict, Type[None]]
        """
        content = retrieve_invalid_parameter_types(
            event=event, required_param_types=self._post_required_map
        )

        ks: list = event.get(LICENSE_KEYS_ATTR)
        condition = not ks or any(True for k in ks if not isinstance(k, str))

        mid: str = event.get(MANAGEMENT_ID_ATTR, None)

        template = BAD_REQUEST_IMPROPER_TYPES
        issue = None

        if not content and condition:
            issue = f'each of {LICENSE_KEYS_ATTR}: str.'

        elif not content and len(ks) != len(set(ks)):
            issue = f'Values of \'{LICENSE_KEYS_ATTR}\' ' \
                    f'parameter must be unique.'
            template = 'Bad request. {}'

        elif not content and mid is not None and not isinstance(mid, str):
            issue = f'\'{MANAGEMENT_ID_ATTR}\':str'

        content = template.format(issue) if issue else content

        if content:
            event = None
            self._code = RESPONSE_BAD_REQUEST_CODE
            self._content = content

        return event

    def _access_priority_management_data_step(self, event: Dict):
        """
        Mandates priority management data retrieval and lack of persistence.
        Establishes
        Response variables are assigned, under the following predicates:
        Given absence: `code` - 404, `content` - persistence reason.
        Given managed-entity id conflict: `code` - 409, `content` - conflict.
        :parameter event: Dict
        :return: Union[Dict, Type[None]]
        """

        complemented = self.tenant_entity
        if not complemented:
            _LOG.error('Execution step is missing \'tenant_entity\' variable.')
            return None

        _id = complemented.name
        _template = ENTITY_TEMPLATE.format(entity=self.entity, id=_id)

        # Managed entity id, i.e. ruleset-id.
        gvc_id = event[GOVERNANCE_ENTITY_ID_ATTR]

        mgmt = complemented.management or dict()

        _type = event[GOVERNANCE_ENTITY_TYPE_ATTR]

        conflict = f' \'{gvc_id}\' {_type}' + CONFLICT

        # Until the delegation(s) and governance is not put into place.
        # Check for cross priority collision as well.

        if any(_pid for _pid in mgmt if gvc_id in mgmt[_pid]):
            issue = conflict + f' within {MANAGEMENT_ATTR}'
            _LOG.warning(_template + issue)
            self._code = RESPONSE_CONFLICT
            self._content = _template + issue
            return None

        # Separate concern of access within data.
        pid = event.get(MANAGEMENT_ID_ATTR, None)
        if not pid:
            return event

        if event and pid not in mgmt:
            issue = f' priority \'{pid}\' {MANAGEMENT_ID_ATTR}'
            _LOG.warning(_template + issue + PERSISTENCE_ERROR)

            issue, _ = issue.rsplit(' ', 1)
            issue += f' {PRIORITY_ID_ATTR}'
            self._code = RESPONSE_RESOURCE_NOT_FOUND_CODE
            self._content = _template + issue + PERSISTENCE_ERROR
            event = None

        if event:
            # Attaches management-id, if priority has already been given.
            self.mid = pid

        return event

    def _access_ruleset_entity_step(self, event: Dict):
        """
        Mandates Ruleset entity access and lack of persistence.\n
        Response variables are assigned, under the following predicates:
        \nGiven absence: `code` - 404, `content` - persistence reason.
        :parameter event: Dict
        :return: Union[Dict, Type[None]]
        """
        if event[GOVERNANCE_ENTITY_TYPE_ATTR] != RULESET_ATTR:
            return event

        ruleset_service = self.ruleset_service
        rid = event.get(GOVERNANCE_ENTITY_ID_ATTR, '')
        _template = ENTITY_TEMPLATE.format(entity='Ruleset', id=rid)

        self.ruleset_entity = ruleset_service.get_ruleset_by_id(
            ruleset_id=rid
        )

        if not self.ruleset_entity or not self.ruleset_entity.licensed:
            event = None
            self._code = RESPONSE_RESOURCE_NOT_FOUND_CODE
            self._content = _template + PERSISTENCE_ERROR

        return event

    def _instantiate_data_within_priority_step(self, event: Dict):
        """
        Mandates Settings-complemented Tenant entity instantiation of
        ruleset-license-priority-management type, driven by given
        license-keys(s).\n Response variables are assigned, under the
        following predicates: \nGiven absence: `code` - 404, `content` -
        persistence reason.
        :parameter event: Dict
        :return: Union[Dict, Type[None]]
        """
        mid: str = self.mid
        complemented: Complemented = self.tenant_entity
        if not mid:
            # Request has been issued to create a priority.
            return event

        if not complemented:
            return None

        _template = ENTITY_TEMPLATE.format(
            entity=self.entity, id=complemented.name
        )

        pid = event[MANAGEMENT_ID_ATTR]
        mgmt: dict = complemented.management[pid]
        mgmt[event[GOVERNANCE_ENTITY_ID_ATTR]] = event[LICENSE_KEYS_ATTR]
        _type = event[GOVERNANCE_ENTITY_TYPE_ATTR]
        updated = ATTRIBUTE_UPDATED.format(
            f'\'{mid}\' {_type} {MANAGEMENT_ATTR}:\'{pid}\'',
            ', '.join(event[LICENSE_KEYS_ATTR])
        )
        _LOG.info(_template + updated)

        complemented.management[pid] = mgmt
        self.mid = mid
        return event

    def _instantiate_priority_management_step(self, event: Dict):
        """
        Mandates Settings-complemented Tenant entity instantiation of
        ruleset-license-priority-management type, driven by given
        license-keys(s).\n Response variables are assigned, under the
        following predicates: \nGiven absence: `code` - 404, `content` -
        persistence reason.
        :parameter event: Dict
        :return: Union[Dict, Type[None]]
        """
        mid: str = self.mid
        if mid:
            # Request is issued to create a managed data within a priority.
            return event

        complemented = self.tenant_entity

        if not complemented:
            return None

        _template = ENTITY_TEMPLATE.format(
            entity=self.entity, id=complemented.name
        )

        priority_service = self.priority_governance_service

        self.mid = priority_service.create_management(
            entity=complemented, data={
                event[GOVERNANCE_ENTITY_ID_ATTR]: event[LICENSE_KEYS_ATTR]
            }
        )
        label = self.priority_governance_service.governance_type
        message = f' \'{self.mid}\' \'{label}\''
        _LOG.info(_template + message + INSTANTIATED)

        return event


class PatchTenantLicensePriorityHandler(CommandTenantLicensePriorityHandler):

    ruleset_entity: Optional[Ruleset]

    def __init__(
        self, modular_service: ModularService, license_service: LicenseService,
        priority_governance_service: PriorityGovernanceService,
        ruleset_service: RulesetService
    ):

        # Declares a subjected query specification
        self.license_service = license_service
        self.ruleset_service = ruleset_service

        super().__init__(
            modular_service=modular_service,
            priority_governance_service=priority_governance_service
        )

    def _reset(self):
        super()._reset()
        # Declares request-pending customer entity
        self.ruleset_entity = None

    def define_action_mapping(self):
        return {
            TENANTS_LICENSE_PRIORITIES_PATH: {
                PATCH_METHOD: self.patch_license_priority
            }
        }

    def patch_license_priority(self, event):
        action = PATCH_METHOD.capitalize()
        return self._process_action(event=event, action=action)

    @property
    def attributes_to_log(self) -> Dict[Union[str, Type[None]], Iterable[str]]:
        license_keys = [
            LICENSE_KEYS_TO_APPEND_ATTR, LICENSE_KEYS_TO_PREPEND_ATTR,
            LICENSE_KEYS_TO_DETACH_ATTR
        ]
        return {
            VALIDATION: list(self._patch_required_map) + license_keys + [
                CUSTOMER_ATTR
            ],
            ACCESSIBILITY: [
                TENANT_ATTR, GOVERNANCE_ENTITY_ID_ATTR,
                MANAGEMENT_ID_ATTR, *license_keys
            ],
            INSTANTIATION: ['tenant_entity', MANAGEMENT_ID_ATTR],
            PERSISTENCE: ['tenant_entity']
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
        return [
            self._validate_payload_types_step,
            self._validate_customer_data_step,
            self._validate_governance_entity_data_step
        ]

    @property
    def access_responsibilities(self):
        return [
            self._access_tenant_entity_step,
            self._access_priority_management_data_step,
            self._access_ruleset_entity_step,
            self._access_attachable_license_keys_step
        ]

    @property
    def amendment_responsibilities(self):
        return [
            self._amend_data_within_priority_step,
            self._descended_amendment_step
        ]

    @property
    def persistence_responsibilities(self):
        return [self._persist_priority_management_step]

    @property
    def _patch_required_map(self):
        return {
            TENANT_ATTR: str,
            GOVERNANCE_ENTITY_TYPE_ATTR: str,
            GOVERNANCE_ENTITY_ID_ATTR: str,
            MANAGEMENT_ID_ATTR: str
        }

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
        key_attrs = (
            LICENSE_KEYS_TO_PREPEND_ATTR, LICENSE_KEYS_TO_APPEND_ATTR,
            LICENSE_KEYS_TO_DETACH_ATTR
        )

        if not content:
            _applicable = False

            for attr in key_attrs:
                keys: List = event.get(attr)
                cdn = (not isinstance(keys, list) or
                       any(True for k in keys if not isinstance(k, str)))

                if keys is not None and cdn:
                    issue = f'each of \'{attr}\' list: str.'
                    content = BAD_REQUEST_IMPROPER_TYPES.format(issue)
                    break

                elif keys and len(keys) != len(set(keys)):
                    issue = f'each of \'{attr}\' must be unique.'
                    content = BAD_REQUEST_IMPROPER_TYPES.format(issue)
                    break

                if keys:
                    _applicable = True

            if not content and not _applicable:
                stringed = ', '.join(f'\'{attr}\''for attr in key_attrs)
                content = 'Bad request. At least one out of the following ' \
                          f'parameters must be provided: {stringed}.'

        if content:
            event = None
            self._code = RESPONSE_BAD_REQUEST_CODE
            self._content = content

        return event

    def _access_priority_management_data_step(self, event: Dict):
        """
        Mandates priority management data retrieval and lack of persistence.\n
        Response variables are assigned, under the following predicates:
        Given absence: `code` - 404, `content` - persistence reason.
        Given managed-entity id conflict: `code` - 409, `content` - conflict.
        :parameter event: Dict
        :return: Union[Dict, Type[None]]
        """
        complemented = self.tenant_entity
        if not complemented:
            return None

        _id = complemented.name
        _template = ENTITY_TEMPLATE.format(entity=self.entity, id=_id)

        gvc_id = event[GOVERNANCE_ENTITY_ID_ATTR]
        mid = event[MANAGEMENT_ID_ATTR]

        mgmt = complemented.management or dict()

        if mid not in mgmt:
            issue = f' priority \'{mid}\' {MANAGEMENT_ID_ATTR}'
            _LOG.warning(_template + issue + PERSISTENCE_ERROR)

            issue, _ = issue.rsplit(' ', 1)
            issue += f' {PRIORITY_ID_ATTR}'
            self._code = RESPONSE_RESOURCE_NOT_FOUND_CODE
            self._content = _template + issue + PERSISTENCE_ERROR
            event = None

        elif gvc_id not in mgmt[mid]:
            _type = event[GOVERNANCE_ENTITY_TYPE_ATTR]
            scope = f' within \'{mid}\' '
            issue = f' \'{gvc_id}\' {_type}' + PERSISTENCE_ERROR + scope
            _LOG.warning(_template + issue + MANAGEMENT_ID_ATTR)

            self._code = RESPONSE_CONFLICT
            self._content = _template + issue + scope + PRIORITY_ID_ATTR
            event = None

        return event

    def _access_ruleset_entity_step(self, event: Dict):
        """
        Mandates Ruleset entity access and lack of persistence.\n
        Response variables are assigned, under the following predicates:
        \nGiven absence: `code` - 404, `content` - persistence reason.
        :parameter event: Dict
        :return: Union[Dict, Type[None]]
        """
        if event[GOVERNANCE_ENTITY_TYPE_ATTR] != RULESET_ATTR:
            return event

        complemented: Complemented = self.tenant_entity
        ruleset_service = self.ruleset_service

        rid = event.get(GOVERNANCE_ENTITY_ID_ATTR, '')
        pid = event.get(MANAGEMENT_ID_ATTR)
        _template = ENTITY_TEMPLATE.format(entity='Ruleset', id=rid)

        mgmt = (complemented.management or dict()).get(pid, dict())

        retained = rid in mgmt
        self.ruleset_entity = None if not retained else ruleset_service. \
            get_ruleset_by_id(ruleset_id=rid)

        if not self.ruleset_entity or not self.ruleset_entity.licensed:
            if retained:
                # Provide eventually-consistent self-healing.
                _LOG.warning(_template + ' has become obsolete - removing it.')
                mgmt.pop(rid)
                self.mid = rid
                self._persist_priority_management_step(event=event)
            event = None
            self._code = RESPONSE_RESOURCE_NOT_FOUND_CODE
            self._content = _template + PERSISTENCE_ERROR

        return event

    def _access_attachable_license_keys_step(self, event: Dict):
        """
        Mandates Tenant entity access to given license key(s) to prepend
        or append, as well respective relation with the pending ruleset.\n
        Response variables are assigned, under the following predicates:
        \nGiven absence: `code` - 404, `content` - persistence reason.
        :parameter event: Dict
        :return: Union[Dict, Type[None]]
        """
        key_attrs = (
            LICENSE_KEYS_TO_PREPEND_ATTR, LICENSE_KEYS_TO_APPEND_ATTR
        )
        for attr in key_attrs:
            keys = event.get(attr) or []
            if keys:
                is_applicable = self._access_license_relation_entity_step(
                   event={LICENSE_KEYS_ATTR: keys}
                )
                if not is_applicable:
                    event = None
                    break
        return event

    def _amend_data_within_priority_step(self, event: Dict):
        """
        Mandates Settings-complemented Tenant entity instantiation of
        ruleset-license-priority-management type, driven by given
        license-keys(s).\n Response variables are assigned, under the
        following predicates: \nGiven absence: `code` - 404, `content` -
        persistence reason.
        :parameter event: Dict
        :return: Union[Dict, Type[None]]
        """
        complemented: Complemented = self.tenant_entity

        if not complemented:
            return None

        _template = ENTITY_TEMPLATE.format(
            entity=self.entity, id=complemented.name
        )

        mid = event[MANAGEMENT_ID_ATTR]
        gvc_id = event[GOVERNANCE_ENTITY_ID_ATTR]

        mgmt: dict = complemented.management[mid]

        license_key_priority: list = mgmt[gvc_id] or []

        to_detach: list = event.get(LICENSE_KEYS_TO_DETACH_ATTR)

        if to_detach:
            for key in to_detach:
                if key in license_key_priority:
                    license_key_priority.remove(key)

        to_prepend: list = event.get(LICENSE_KEYS_TO_PREPEND_ATTR) or []
        to_append: list = event.get(LICENSE_KEYS_TO_APPEND_ATTR) or []

        for each in to_prepend[::-1]:
            if each not in license_key_priority:
                license_key_priority.insert(0, each)

        for each in to_append:
            if each not in license_key_priority:
                license_key_priority.append(each)

        mgmt[gvc_id] = license_key_priority

        _type = event[GOVERNANCE_ENTITY_TYPE_ATTR]
        updated = ATTRIBUTE_UPDATED.format(
            f'\'{mid}\' {_type} {MANAGEMENT_ATTR}:\'{mid}\'',
            ', '.join(mgmt[gvc_id])
        )
        _LOG.info(_template + updated)

        complemented.management[mid] = mgmt
        return event

    def _descended_amendment_step(self, event: Dict):

        priority_governance_service = self.priority_governance_service
        complemented: Complemented = self.tenant_entity

        if not complemented:
            return None

        _template = ENTITY_TEMPLATE.format(
            entity=self.entity, id=complemented.name
        )

        mid = event[MANAGEMENT_ID_ATTR]
        eid = event[GOVERNANCE_ENTITY_ID_ATTR]
        mgmt = complemented.management or dict()
        managed = mgmt.get(mid, dict())

        if eid in managed and not managed.get(eid):
            managed.pop(eid)
            attr = event[GOVERNANCE_ENTITY_TYPE_ATTR]
            scope = f'\'{mid}\' {MANAGEMENT_ATTR}'
            message = f' \'{eid}\' {attr} removed out of {scope}'
            _LOG.info(_template + message)

        if mid in mgmt and not managed:
            # No managed data is left.
            self.tenant_entity = priority_governance_service.delete_management(
                entity=complemented, mid=mid
            )
            if not self.tenant_entity:
                # Unlikely: as it is verified on the _access step, beforehand.
                issue = f' {mid} {PRIORITY_ID_ATTR} {PERSISTENCE_ERROR}'
                self._code = RESPONSE_RESOURCE_NOT_FOUND_CODE
                self._content = _template + issue
                event = None

        return event


class DeleteTenantLicensePriorityHandler(CommandTenantLicensePriorityHandler):
    def __init__(
        self, modular_service: ModularService,
        priority_governance_service: PriorityGovernanceService
    ):
        super().__init__(
            modular_service=modular_service,
            priority_governance_service=priority_governance_service
        )

    def define_action_mapping(self):
        return {
            TENANTS_LICENSE_PRIORITIES_PATH: {
                DELETE_METHOD: self.delete_license_priority
            }
        }

    def delete_license_priority(self, event):
        action = DELETE_METHOD.capitalize()
        return self._process_action(event=event, action=action)

    @property
    def attributes_to_log(self) -> Dict[Union[str, Type[None]], Iterable[str]]:
        return {
            VALIDATION: list(self._delete_required_map),
            ACCESSIBILITY: [
                TENANT_ATTR, GOVERNANCE_ENTITY_ID_ATTR, MANAGEMENT_ID_ATTR
            ],
            AMENDMENT: ['tenant_entity', 'mid', 'data_id'],
            PERSISTENCE: ['tenant_entity']
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
        return [
            self._validate_payload_types_step,
            self._validate_customer_data_step,
            self._validate_governance_entity_data_step
        ]

    @property
    def access_responsibilities(self):
        return [
            self._access_tenant_entity_step,
            self._access_priority_management_data_step,
            self._access_entity_within_priority_data_step
        ]

    @property
    def amendment_responsibilities(self):
        return [
            self._amend_entity_persistence_step,
            self._amend_scoped_priority_data_step
        ]

    @property
    def persistence_responsibilities(self):
        return [self._persist_priority_management_step]

    @property
    def _delete_required_map(self):
        return {
            TENANT_ATTR: str,
            GOVERNANCE_ENTITY_TYPE_ATTR: str
        }

    def _validate_payload_types_step(self, event: Dict):
        """
        Mandates payload parameters for the delete event, adhering
        to the respective requirement map.
        :parameter event: Dict
        :return: Union[Dict, Type[None]]
        """
        content = retrieve_invalid_parameter_types(
            event=event, required_param_types=self._delete_required_map
        )

        mgmt_id = event.get(MANAGEMENT_ID_ATTR, None)
        gvc_id = event.get(GOVERNANCE_ENTITY_ID_ATTR, None)

        template = BAD_REQUEST_IMPROPER_TYPES
        issue = ''

        if not content:
            if mgmt_id and not isinstance(mgmt_id, str):
                issue = f'{MANAGEMENT_ID_ATTR}:str'

            elif gvc_id and not isinstance(gvc_id, str):
                issue = issue or f'{GOVERNANCE_ENTITY_ID_ATTR}:str'

            if not mgmt_id and gvc_id is not None:
                issue = f'Parameter \'{GOVERNANCE_ENTITY_ID_ATTR}\' must be'
                issue += f' provided with \'{MANAGEMENT_ID_ATTR}\''
                template = 'Bad request. {}.'

        content = template.format(issue) if issue else content
        if content:
            event = None
            self._code = RESPONSE_BAD_REQUEST_CODE
            self._content = content

        return event

    def _access_priority_management_data_step(self, event: Dict):
        """
        Mandates priority management data retrieval and lack of persistence.\n
        Response variables are assigned, under the following predicates:
        \nGiven absence: `code` - 404, `content` - persistence reason.
        :parameter event: Dict
        :return: Union[Dict, Type[None]]
        """
        complemented = self.tenant_entity
        if not complemented:
            return None

        _id = complemented.name
        _template = ENTITY_TEMPLATE.format(entity=self.entity, id=_id)

        # Priority-id.
        mid = event.get(MANAGEMENT_ID_ATTR, None)
        if mid is None:
            return event

        mgmt = complemented.management or dict()

        if mid not in mgmt:
            issue = f' priority \'{mid}\' {MANAGEMENT_ID_ATTR}'
            _LOG.warning(_template + issue + PERSISTENCE_ERROR)

            issue, _ = issue.rsplit(' ', 1)
            issue += f' {PRIORITY_ID_ATTR}'
            self._code = RESPONSE_RESOURCE_NOT_FOUND_CODE
            self._content = _template + issue + PERSISTENCE_ERROR
            event = None

        return event

    def _access_entity_within_priority_data_step(self, event: Dict):
        """
        Mandates priority management data retrieval and lack of persistence.\n
        Response variables are assigned, under the following predicates:
        \nGiven absence: `code` - 404, `content` - persistence reason.
        :parameter event: Dict
        :return: Union[Dict, Type[None]]
        """
        complemented = self.tenant_entity
        if not complemented:
            return None

        _id = complemented.name
        _template = ENTITY_TEMPLATE.format(entity=self.entity, id=_id)

        pid = event.get(MANAGEMENT_ID_ATTR, None)
        if pid is None:
            return event

        mgmt = complemented.management or dict()
        priority_data = mgmt[pid]

        gvc_id = event.get(GOVERNANCE_ENTITY_ID_ATTR, None)
        if gvc_id is None:
            return event

        if gvc_id and gvc_id not in priority_data:
            _type = event[GOVERNANCE_ENTITY_TYPE_ATTR]
            scope = f' within \'{pid}\' '
            issue = f' \'{gvc_id}\' {_type}' + PERSISTENCE_ERROR + scope
            _LOG.warning(_template + issue + MANAGEMENT_ATTR)
            self._code = RESPONSE_RESOURCE_NOT_FOUND_CODE
            self._content = _template + issue + PRIORITY_ID_ATTR
            event = None

        return event

    def _amend_entity_persistence_step(self, event: Dict):
        """
        Designated to amend data, by deleting pending entity, given the
        request has not been issued to amend data within.
        :parameter event: Dict
        :return: Optional[dict]
        """
        if MANAGEMENT_ID_ATTR in event:
            # Request has derived a management or managed-entity to remove.
            return event

        complemented: Complemented = self.tenant_entity
        if not complemented:
            return None

        _id = complemented.name
        _template = ENTITY_TEMPLATE.format(entity=self.entity, id=_id)

        governance_type = event[GOVERNANCE_ENTITY_TYPE_ATTR].capitalize()
        scope = _template + f' of \'{governance_type}\' management'

        _LOG.debug(_template + ' is going to be deleted.')
        if self.modular_service.delete(entity=complemented):
            message = scope + ' has been removed.'
            _LOG.info(message)
            self._code = RESPONSE_OK_CODE
            self._content = message
        else:
            message = scope + ' could not be removed.'
            _LOG.error(message)
            self._content = message

        return None

    def _amend_scoped_priority_data_step(self, event: Dict):
        complemented: Complemented = self.tenant_entity
        if not complemented:
            return None

        _id = complemented.name

        _template = ENTITY_TEMPLATE.format(entity=self.entity, id=_id)
        priority_governance_service = self.priority_governance_service

        mid = event[MANAGEMENT_ID_ATTR]
        eid = event.get(GOVERNANCE_ENTITY_ID_ATTR, None)

        # Issued to remove managed entity data, within priority
        if eid:
            complemented.management[mid].pop(eid)
            attr = event[GOVERNANCE_ENTITY_TYPE_ATTR]
            scope = f'\'{mid}\' {MANAGEMENT_ATTR}'
            message = f' \'{eid}\' {attr} removed out of {scope}'
            _LOG.info(_template + message)

        # Issued to remove priority data or consequence of managed data absence
        if not eid or not complemented.management[mid]:
            self.tenant_entity = priority_governance_service.delete_management(
                entity=complemented, mid=mid
            )
            if not self.tenant_entity:
                # Verified on the _access step, beforehand.
                issue = f' {mid} {PRIORITY_ID_ATTR} {PERSISTENCE_ERROR}'
                self._code = RESPONSE_RESOURCE_NOT_FOUND_CODE
                self._content = _template + issue
                event = None

        return event

    def _produce_response_dto(self, event: Optional[Dict] = None) -> \
            Union[str, Dict, List, Type[None]]:
        """
        Mandates derivation of a patch-response data transfer object,
        based on a pending Customer Parent entity.
        :parameter event: Dict
        :return: Union[Dict, List, Type[None]]
        """
        # Executes given a incomplete removal.
        entity: Complemented = self.tenant_entity
        mid = event.get(MANAGEMENT_ID_ATTR)
        gvc_id = event.get(GOVERNANCE_ENTITY_ID_ATTR)
        data_id = gvc_id or mid

        if not entity:
            _LOG.error('Variables \'tenant_entity\' and '
                       '\'data_id\' are missing.')
            return None

        if data_id != mid and mid in entity.management:
            get_dto = self.priority_governance_service.get_management
            response = get_dto(entity=entity, mid=mid)
            response = self._get_expanded_dto(dto=response or dict())
        else:
            response = f'Priority \'{mid}\' has been removed.'

        return response


class TenantLicensePriorityHandler(AbstractComposedHandler):
    ...


def instantiate_tenant_license_priority_handler(
        modular_service: ModularService, ruleset_service: RulesetService,
        priority_governance_service: PriorityGovernanceService,
        license_service: LicenseService
):
    get_handler = GetTenantLicensePriorityHandler(
        modular_service=modular_service,
        priority_governance_service=priority_governance_service
    )
    post_handler = PostTenantLicensePriorityHandler(
        modular_service=modular_service,
        ruleset_service=ruleset_service, license_service=license_service,
        priority_governance_service=priority_governance_service
    )
    patch_handler = PatchTenantLicensePriorityHandler(
        modular_service=modular_service,
        ruleset_service=ruleset_service, license_service=license_service,
        priority_governance_service=priority_governance_service
    )
    delete_handler = DeleteTenantLicensePriorityHandler(
        modular_service=modular_service,
        priority_governance_service=priority_governance_service
    )

    return TenantLicensePriorityHandler(
        resource_map={
            TENANTS_LICENSE_PRIORITIES_PATH: {
                GET_METHOD: get_handler,
                POST_METHOD: post_handler,
                PATCH_METHOD: patch_handler,
                DELETE_METHOD: delete_handler
            }
        }
    )
