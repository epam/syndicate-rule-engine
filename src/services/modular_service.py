from functools import cached_property
from typing import Union, Type, Any, Set, Callable, Iterator, Dict, Optional, \
    List

from helpers import raise_error_response, RESPONSE_RESOURCE_NOT_FOUND_CODE
from helpers.constants import (
    META_ATTR, ACTIVATION_DATE_ATTR, INHERIT_ATTR, VALUE_ATTR, CLOUDS_ATTR,
    MODULAR_MANAGEMENT_ID_ATTR, MODULAR_CLOUD_ATTR, MODULAR_DISPLAY_NAME_ATTR,
    MODULAR_READ_ONLY_ATTR, MODULAR_DISPLAY_NAME_TO_LOWER, MODULAR_CONTACTS,
    MODULAR_SECRET, MODULAR_IS_DELETED, MODULAR_TYPE, MODULAR_DELETION_DATE,
    MODULAR_PARENT_MAP, ALL_SCOPE, SCOPE_ATTR, TENANT_ENTITY_TYPE
)
from modular_sdk.commons.constants import CUSTODIAN_TYPE, \
    TENANT_PARENT_MAP_CUSTODIAN_TYPE, \
    TENANT_PARENT_MAP_CUSTODIAN_LICENSES_TYPE, \
    TENANT_PARENT_MAP_SIEM_DEFECT_DOJO_TYPE
from helpers.log_helper import get_logger
from helpers.time_helper import utc_iso
from models.modular import BaseModel
from models.modular.parents import ScopeParentMeta
from modular_sdk.commons.exception import ModularException
from models.modular.application import Application
from models.modular.customer import Customer
from models.modular.parents import Parent
from models.modular.tenant_settings import TenantSettings
from models.modular.tenants import Tenant
from services.clients.modular import ModularClient
from pynamodb.expressions.condition import Condition

_LOG = get_logger(__name__)

PUBLIC_COMPLEMENT_ATTR = 'complement'
PROTECTED_COMPLEMENT_ATTR = '_complement'
PROTECTED_ENTITY_ATTR = '_entity'

INITIALIZED_ATTR = '_initialized'
ENTITY_TYPE_ATTR = '_entity_type'
COMPLEMENT_TYPE_ATTR = '_complement_type'
DATA_AGGREGATE_ATTR = '_data_aggregate_attr'

COMPLETE_ENTITY = '{entity} Complement'
PERSISTENCE_ERROR = ' does not exist'
INSTANTIATED = ' has been instantiated'
RETRIEVED = ' has been retrieved'

METHOD_ATTR = 'method'
REFERENCE_ATTR = 'reference'

ENTITY = 'an entity'
ENTITY_TEMPLATE = '{type}:\'{id}\''
FETCHED_TEMPLATE = ' has fetched {}'
FAILED_TO_DERIVE = ' has failed to derive {}'
FAILED_TO_PERSIST = ' has failed to persist {}'
FAILED_TO_CEASE_PERSISTENCE = ' has failed to delete'
FAILED_TO_INSTANTIATE = ' could not instantiate a {}'
REASON = ', due to the following: "{}"'

APPLICATION_ABSENCE = ' related Application absence.'
REMOVED = ' {} being removed'

SAMPLE_PARENT_DESCRIPTION = 'Custodian \'{}\' Parent entity complement.'

DEFAULT_CUSTOMER_INHERIT = False
DEFAULT_APPLICATION_ID = 'APPLICATION_ID_PLACEHOLDER'

# Tenant region attrs
NATIVE_NAME_ATTR = 'native_name'
IS_ACTIVE_ATTR = 'is_active'


class Complemented:
    """
    Mandates Modular Entity infusion.
    """

    def __init__(self, entity: Optional[Union[Customer, Tenant]] = None,
                 complement: Optional[Union[Parent, TenantSettings]] = None):
        """
        Either entity or complement must be provided
        """
        if self._initialize(entity=entity, complement=complement):
            self.__dict__[INITIALIZED_ATTR] = True

    def _initialize(self, entity: Optional[Union[Customer, Tenant]] = None,
                    complement: Optional[Union[Parent, TenantSettings]] = None
                    ) -> bool:
        """
        Constructs a Complemented entity instance, initializing:
         1. Attributes to contain entity data:
          - _entity - maintaining content of either Customer, Tenant
          - _complement - maintaining content of either Parent, TenantSettings
         2. Attribute to contain types:
          - _entity_type - maintaining either Customer, Tenant class
          - _complement_type - maintaining either Parent, TenantSettings class
         3. Attribute `_data_aggregate_attr`, meant to reference data
         aggregation in respect to the complement type.

        :parameter entity: Union[Customer, Tenant]
        :parameter complement: Union[Parent, TenantSettings]
        :return: bool
        """
        if not (entity or complement):
            return False
        self._initialize_inner_attributes()
        if entity:
            _entity_type = entity.__class__
            assert _entity_type in self._infusion_reference
            self.__setattr__(PROTECTED_ENTITY_ATTR, entity.get_json())
            self.__setattr__(ENTITY_TYPE_ATTR, _entity_type)
        if complement:
            _complement_type = complement.__class__
            assert _complement_type in self._infusion_reference.values()
            self.__setattr__(PROTECTED_COMPLEMENT_ATTR, complement.get_json())

            _aggregate_attr_reference = self._data_aggregate_attr_reference
            _aggregate_atr = _aggregate_attr_reference.get(_complement_type)

            if _aggregate_atr is not None:  # no need if properly configured
                self.__setattr__(COMPLEMENT_TYPE_ATTR, _complement_type)
                self.__setattr__(DATA_AGGREGATE_ATTR, _aggregate_atr)
        return True

    def _initialize_inner_attributes(self):
        """
        So that __getattr__ and existing properties do not cause recursion
        """
        self.__setattr__(PROTECTED_ENTITY_ATTR, dict())
        self.__setattr__(ENTITY_TYPE_ATTR, None)
        self.__setattr__(PROTECTED_COMPLEMENT_ATTR, dict())
        self.__setattr__(COMPLEMENT_TYPE_ATTR, None)
        self.__setattr__(DATA_AGGREGATE_ATTR, None)

    @property
    def entity(self) -> Optional[Union[Customer, Tenant]]:
        """
        References the initialized entity, attached to the Parent.
        :return: Type[BaseModel]
        """
        _type = getattr(self, ENTITY_TYPE_ATTR)
        if not _type:
            return
        return _type(**getattr(self, PROTECTED_ENTITY_ATTR))

    @property
    def complement(self) -> Optional[Union[Parent, TenantSettings]]:
        """
        Produces a MaestroCommonDomainModel: Complement aggregated with
        attached custodian:entity data, respectively stored under the
        `meta` attribute.
        :return: Parent
        """
        _type = getattr(self, COMPLEMENT_TYPE_ATTR)
        if not _type:
            return
        return _type(**getattr(self, PROTECTED_COMPLEMENT_ATTR))

    @property
    def initialized(self):
        return self.__dict__.get(INITIALIZED_ATTR, False)

    @cached_property
    def _infusion_reference(self):
        return {
            Customer: Parent, Tenant: TenantSettings
        }

    @cached_property
    def _data_aggregate_attr_reference(self):
        return {
            Parent: META_ATTR, TenantSettings: VALUE_ATTR
        }

    def get_json(self):
        """
        Produces a Data Transfer object of an Entity and complemented
        Parent, adhering to the assigned access priority.
        Note: look out for collisions.
        :return: dict
        """
        # Retrieve content of entities.
        entity_d, complement_d = {}, {}
        entity, complement = self.entity, self.complement
        if entity:
            entity_d.update(entity.get_json())
        if complement:
            complement_d.update(complement.get_json())
        attr = getattr(self, DATA_AGGREGATE_ATTR)
        metadata: dict = complement_d.get(attr, dict())
        entity_d.update(metadata)
        return entity_d

    def __setattr__(self, name: str, value: Any):
        """
        Alters Modular metadata, providing a new key-value pair.
        :parameter name:str, attribute to store into the metadata.
        :parameter value:Any, respective attribute value.
        :return: Type[None]
        """
        if self.initialized:

            complement: dict = getattr(self, PROTECTED_COMPLEMENT_ATTR)
            attr = getattr(self, DATA_AGGREGATE_ATTR)
            if complement and attr:
                metadata: dict = complement.setdefault(attr, dict())
                metadata.update({name: value})
            else:  # entity
                entity: dict = getattr(self, PROTECTED_ENTITY_ATTR)
                entity.update({name: value})
        else:
            super().__setattr__(name, value)

    def __getattr__(self, attribute: str):
        """
        Retrieves shared Parent-Entity attribute adhering
        to the respective `access priority`.
        :parameter attribute: str
        :raises: AttributeError, given object has not been `initialized`
        :return: Any
        """
        if not self.initialized:
            issue = f'\'{self.__class__.__name__}\' object has no attribute '
            issue += '\'{}\''
            raise AttributeError(issue.format(attribute))

        complement: dict = getattr(self, PROTECTED_COMPLEMENT_ATTR)
        attr = getattr(self, DATA_AGGREGATE_ATTR)
        metadata: dict = complement.get(attr, dict())
        entity: dict = getattr(self, PROTECTED_ENTITY_ATTR)

        # Establish attribute reference.
        order = (metadata, entity, complement)
        for each in order:
            if attribute in each:
                break
        else:
            each = dict()
        return each.get(attribute)


class ModularService:
    """
    Mandates persistence of MaestroCommonDomainModel entities:
        - Application, providing bare entity `query`;
        - Customer, providing bare and complemented entity `query`;
        - Tenant, providing bare entity `query`;
        - Parent, providing bare entity `query` and complementing `update`.
        - TenantSettings, providing complementing `update`.
    """

    def __init__(self, client: ModularClient):
        self._client: ModularClient = client

    @property
    def modular_client(self) -> ModularClient:
        return self._client

    # Public Entity related actions
    def get_dto(self, entity: Union[BaseModel, Complemented], *attributes):
        """
        Retrieves Data Transfer Object of an entity, including provided
        iterable name sequence of `attributes`. Given no preference
        all retained attributes are retrieved.
        :parameter entity: Union[BaseModel, Complemented]
        :parameter attributes: Tuple[str]
        :return: dict
        """
        to_include = attributes or None
        to_conceal = self._get_concealed_attributes(entity=entity)
        source = self._get_entity_dto(entity=entity)
        dto = dict()
        for attribute in source:
            approved = to_conceal and attribute not in to_conceal
            demanded = to_include and attribute in to_include
            if (approved or not to_conceal) and (demanded or not to_include):
                dto[attribute] = source[attribute]
        return dto

    def save(self, entity: Union[Parent, TenantSettings, Application, Complemented]):
        """
        Mandates persistence of Maestro Common Domain Model complementary
        entities such as Parent and TenantSettings.
        :parameter entity: Union[Parent, TenantSettings, Complemented]
        :return: bool
        """
        retainer: Callable = self._persistence_map.get(entity.__class__)
        return retainer(entity) if retainer else False

    def delete(self, entity: Union[Parent, TenantSettings, Application, Complemented]):
        """
        Mandates persistence-based removal of Maestro Common Domain Model
        complementary entities such as Parent and TenantSettings.
        :parameter entity: Union[Parent, TenantSettings, Complemented]
        :return: bool
        """
        eraser: Callable = self._erasure_map.get(entity.__class__)
        return eraser(entity) if eraser else False

    @cached_property
    def available_types(self) -> Set[str]:
        return {
            TENANT_ENTITY_TYPE,
            'RULESET_LICENSE_PRIORITY',  # this one is built dynamically :(,
            None  # if none, just prefix is returned
        }

    def entity_type(self, _type: Optional[str] = None) -> str:
        """
        Builds type string for Maestro Parent and key string for
        Maestro TenantSetting associated with Custodian
        """
        assert _type in self.available_types, \
            'Not available type. Add it to the set and tell about it Maestro'
        if _type:
            return f'{CUSTODIAN_TYPE}_{_type}'
        return CUSTODIAN_TYPE

    # Public Customer related actions

    def get_complemented_customer(
            self, customer: Union[str, Customer],
            complement_type: str = None
    ) -> Optional[Complemented]:
        """
        DON'T use this method
        Returns a complemented, based on a given type, Customer - Parent
        object, even if customer's parent does not exist, creating said parent.
        :parameter customer: Union[str, Customer]
        :parameter complement_type: str, defaults to a generic complement
        :return: Optional[Complemented]
        """
        template = COMPLETE_ENTITY.format(entity=complement_type)
        entity = customer if isinstance(customer, Customer) else \
            self._get_customer(customer)
        retrieved = self._fetch_complemented_customer_parent(
            customer=entity, complement_type=complement_type
        ) if entity else None
        if isinstance(retrieved, Customer):
            _LOG.warning(template + PERSISTENCE_ERROR)
            entity = self.create_complemented_customer_parent(retrieved)
            if entity:
                _LOG.info(template + INSTANTIATED)
        elif isinstance(retrieved, Complemented):
            template += f'\'{retrieved.entity.name}\':'
            _LOG.info(template + RETRIEVED)
            entity = retrieved
        return entity

    def customer_inherit(self, customer: Union[str, Customer]) -> bool:
        """
        Returns the value of the given customer's inherit attribute
        retrieved from its parent. If the parent does not exist the default
        value is returned.
        :parameter customer: Union[str, Customer]
        :return: bool
        """
        _parent = self._fetch_customer_bound_parent(customer)
        return (_parent.meta.as_dict() if _parent else {}).get(
            INHERIT_ATTR) or DEFAULT_CUSTOMER_INHERIT

    @staticmethod
    def does_customer_inherit(entity: Union[Complemented, Parent]):
        _a_complement = False
        if isinstance(entity, Complemented):
            entity = entity.complement
            _a_complement = True
        if not isinstance(entity, Parent):
            reason = 'does not contain' if _a_complement else 'is not'
            _LOG.error(f'Given entity {reason} a Parent.')
            entity = None
        return (entity.meta.as_dict() if entity else {}).get(
            INHERIT_ATTR) or DEFAULT_CUSTOMER_INHERIT

    def get_customer(self, customer: Union[str, Customer]) -> \
            Optional[Union[Customer, Complemented]]:
        """
        Returns either a bare Customer, Customer-complemented-Parent or None.
        :parameter customer: Union[str, Customer]
        :return: Union[Customer, Complemented, Type[None]]
        """
        return self._get_customer(customer=customer)

    def i_get_customer(self, iterator: Iterator[Union[str, Customer]]) -> \
            Iterator[Union[Customer, Complemented]]:
        """
        Yields either a bare Customer or Customer-complemented-Parent,
        adhering to the type of given `customer` parameter.
        :parameter iterator: Iterator[Union[str, Customer]]
        :return: Iterator[Union[Customer, Complemented]]
        """
        return self._i_fetch_entity(iterator=iterator, key=Customer)

    def i_get_customers(self) -> Iterator[Customer]:
        """
        Yields each MaestroCommonDomainModel:Customer entity.
        :return: Iterator[Customer]
        """
        return self._client.customer_service().i_get_customer()

    @staticmethod
    def i_get_custodian_customer_name(iterator: Iterator[Application]) -> \
            Iterator[str]:
        """
        Yields Custodian-Customer names out of an Application-iterator.
        :parameter iterator: Iterator[Application]
        :return: Iterator[str]
        """
        for application in iterator:
            if isinstance(application, Application):
                yield application.customer_id

    # Public Tenant related actions
    def get_tenant_parent(self, tenant: Tenant,
                          _type: str) -> Optional[Parent]:
        """
        Returns parent that is linked to the tenant
        :param _type: type from PID
        :param tenant:
        :return:
        """
        # tenant parent map type resembles parent type currently
        assert _type in [TENANT_PARENT_MAP_CUSTODIAN_LICENSES_TYPE,
                         TENANT_PARENT_MAP_CUSTODIAN_TYPE,
                         TENANT_PARENT_MAP_SIEM_DEFECT_DOJO_TYPE], \
            'Not available parent type'
        parent_id = tenant.get_parent_id(_type)
        if not parent_id:
            _LOG.info(f'Tenant {tenant.name} does not have specific '
                      f'"{_type}" parent. Going from another side...')
            return next(self.get_customer_bound_parents(
                customer=tenant.customer_name,
                parent_type=_type,  # parents currently only CUSTODIAN
                is_deleted=False,
                meta_conditions=(Parent.meta[SCOPE_ATTR] == ALL_SCOPE) & Parent.meta[CLOUDS_ATTR].contains(tenant.cloud),
                limit=1
            ), None)  # hopefully there is only one. It must be one
        # parent_id exists
        _LOG.info(f'Tenant {tenant.name} contains {_type} parent')
        parent = self.get_parent(parent_id)
        if parent and not parent.is_deleted:
            meta = ScopeParentMeta.from_dict(parent.meta.as_dict())
            if meta.clouds and tenant.cloud not in meta.clouds:
                return
            return parent  # meta.scope == SPECIFIC_TENANT_SCOPE or ALL_SCOPE

    def get_parent_application(self, parent: Parent) -> Optional[Application]:
        if not parent.application_id:
            return
        application = self.get_application(parent.application_id)
        if not application or application.is_deleted:
            return
        return application

    def get_tenant_application(self, tenant: Tenant, _type: str) -> Optional[Application]:
        """
        Resolved application from tenant
        :param tenant:
        :param _type: parent type, not tenant type
        :return:
        """
        parent = self.get_tenant_parent(tenant, _type)
        if not parent:
            return
        return self.get_parent_application(parent)

    def get_complemented_tenant(
            self, tenant: Union[str, Tenant],
            complement_type: str = TENANT_ENTITY_TYPE
    ) -> Optional[Complemented]:
        """
        Returns a complemented, based on a given type, Tenant - Settings
        instance, creating Tenant-Settings entity, given one does not exist.
        :parameter tenant: Union[str, Tenant]
        :parameter complement_type: str
        :return: Optional[Complemented]
        """
        template = COMPLETE_ENTITY.format(entity=complement_type)
        entity = tenant if isinstance(tenant, Tenant) else self._get_tenant(
            tenant=tenant
        )
        retrieved = self._fetch_complemented_tenant_settings(
            tenant=entity, complement_type=complement_type
        ) if entity else None
        if isinstance(retrieved, Tenant):
            _LOG.warning(template + PERSISTENCE_ERROR)
            entity = self.create_complemented_tenant_settings(
                tenant=retrieved, complement_type=complement_type
            )
            _LOG.info(template + INSTANTIATED)
        elif isinstance(retrieved, Complemented):
            template += f'\'{retrieved.entity.name}\':'
            _LOG.info(template + RETRIEVED)
            entity = retrieved
        return entity

    def get_tenant_bound_setting(self, tenant: Union[str, Tenant],
                                 complemented_type: str = TENANT_ENTITY_TYPE
                                 ) -> TenantSettings:
        """
        Returns tenant-bound setting with given type creating the entity
        if it does not exist yet.
        """
        template = COMPLETE_ENTITY.format(entity=complemented_type)

        name = tenant.name if isinstance(tenant, Tenant) else tenant
        setting = self._fetch_bare_tenant_settings(
            tenant=name, complement_type=complemented_type
        )
        if not setting:
            _LOG.warning(template + PERSISTENCE_ERROR)
            setting = self.create_tenant_settings(
                tenant_name=name,
                key=self.entity_type(complemented_type)
            )
            _LOG.info(template + INSTANTIATED)
        return setting

    @staticmethod
    def get_tenant_regions(tenant: Tenant) -> Set[str]:
        """
        Returns active tenant's regions
        """
        # Maestro's regions in tenants have attribute "is_active" ("act").
        # But currently (22.06.2023) they ignore it. They deem all the
        # regions listed in an active tenant to be active as well. So do we
        tenant_json = tenant.get_json()
        return {
            r.get('native_name') for r in tenant_json.get('regions') or []
            if r.get('is_hidden') is not True
        }

    def get_tenant(self, tenant: Union[str, Tenant]) -> \
            Optional[Union[Tenant, Complemented]]:
        """
        Returns a bare MaestroCommonDomainModel Tenant.
        Given that inquery has been unsuccessful, returns a None.
        :parameter tenant: Union[str, Tenant]
        :return: Optional[Union[Tenant, Complemented]]
        """
        return self._get_tenant(tenant=tenant)

    def i_get_tenant(self, iterator: Iterator[Union[str, Tenant]]) -> \
            Iterator[Union[Tenant, Complemented]]:
        """
        Yields a bare MaestroCommonDomainModel Tenant.
        :parameter iterator: Iterator[Union[str, Customer]]
        :return: Iterator[Union[Customer, Complemented]]
        """
        return self._i_fetch_entity(iterator=iterator, key=Tenant)

    def i_get_customer_tenant(self, customer: str, name: str = None,
                              active: bool = True, limit: Optional[int] = None,
                              last_evaluated_key: Union[str, dict] = None):
        """
        Yields MaestroCommonDomainModel Tenant entities of a particular
        customer. Given a name of a tenants - singles out the said tenants
        :parameter customer: str
        :parameter name: str
        :parameter active: bool
        :parameter limit: Optional[int]
        :parameter last_evaluated_key: Union[str, dict]
        :return: Iterator[Tenant]
        """
        service = self._client.tenant_service()
        return service.i_get_tenant_by_customer(
            customer_id=customer, tenant_name=name, active=active,
            limit=limit, last_evaluated_key=last_evaluated_key
        )

    def i_get_tenants(self, active: bool = True, limit: Optional[int] = None,
                      last_evaluated_key: Union[str, dict] = None
                      ) -> Iterator[Tenant]:
        """
        Yields each MaestroCommonDomainModel Tenant entity
        :parameter active: bool
        :parameter limit: Optional[int]
        :parameter last_evaluated_key: Union[str, dict]
        :return: Iterator[Tenant]
        """
        service = self._client.tenant_service()
        return service.i_scan_tenants(active, limit=limit,
                                      last_evaluated_key=last_evaluated_key)

    def i_get_tenants_by_acc(self, acc: str, active: Optional[bool] = None,
                             limit: int = None,
                             last_evaluated_key: Union[dict, str] = None,
                             attrs_to_get: List[str] = None
                             ) -> Iterator[Tenant]:
        """

        :param acc: cloud identifier
        :param active:
        :param limit:
        :param last_evaluated_key:
        :param attrs_to_get:
        :return:
        """
        return self._client.tenant_service().i_get_by_acc(
            acc, active, limit, last_evaluated_key, attrs_to_get)

    @staticmethod
    def is_tenant_valid(tenant: Optional[Tenant] = None,
                        customer: Optional[str] = None) -> bool:
        if not tenant or (customer and tenant.customer_name != customer
                          or not tenant.is_active):
            return False
        return True

    def assert_tenant_valid(self, tenant: Optional[Tenant] = None,
                            customer: Optional[str] = None):
        if not self.is_tenant_valid(tenant, customer):
            generic = 'No active tenant could be found.'
            template = 'Active tenant \'{tdn}\' not found'
            issue = template.format(tdn=tenant.display_name) if tenant else generic
            raise_error_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE, content=issue
            )

    def i_get_tenant_by_display_name_to_lower(
            self, lower_display_name: str, cloud: str = None,
            active: Optional[bool] = None, limit: int = None,
            last_evaluated_key: Union[dict, str] = None) -> List[Tenant]:
        return self._client.tenant_service().i_get_by_dntl(
            lower_display_name, cloud=cloud, active=active, limit=limit,
            last_evaluated_key=last_evaluated_key
        )

    # Public Parent related actions

    def get_parent(self, parent: str):
        """
        Returns a bare MaestroCommonDomainModel Parent.
        Given that inquery has been unsuccessful, returns a None.
        :parameter parent: str
        :return: Union[Parent, Type[None]]
        """
        return self._get_parent(parent=parent)

    def get_customer_bound_parent(self, customer: Union[str, Customer]):
        """
        Returns either a bare Parent bound to a customer.
        :parameter customer: Union[str, Customer]
        :return: Union[Parent, Type[None]]
        """
        return self._fetch_customer_bound_parent(customer=customer)

    def get_customer_bound_parents(self, customer: Union[str, Customer],
                                   parent_type: Optional[Union[str, List[str]]] = None,
                                   is_deleted: Optional[bool] = None,
                                   meta_conditions: Optional[Condition] = None,
                                   limit: Optional[int] = None
                                   ) -> Iterator[Parent]:
        name = customer.name if isinstance(customer, Customer) else customer
        return self.modular_client.parent_service().i_get_parent_by_customer(
            customer_id=name,
            parent_type=parent_type,
            is_deleted=is_deleted,
            meta_conditions=meta_conditions,
            limit=limit
        )

    def i_get_parent(self, iterator: Iterator[str]) -> \
            Iterator[Parent]:
        """
        Yields a bare Maestro Common Domain Model Parent.
        :parameter iterator: Iterator[str]
        :return: Iterator[Parent]
        """
        return self._i_fetch_entity(iterator=iterator, key=Tenant)

    def i_get_parents(self) -> Iterator[Parent]:
        """
        Yields each MaestroCommonDomainModel Parent entity.
        :return: Iterator[Parent]
        """
        service = self._client.parent_service()
        return iter(service.list())

    def create_parent(self, customer_id: str, parent_type: str,
                      application_id: str, description: Optional[str] = None,
                      meta: Optional[dict] = None) -> \
            Union[Parent, Type[None]]:
        """
        Mandates Maestro Common Domain Model Parent instantiation.
        :parameter customer_id: str
        :parameter application_id: str
        :parameter parent_type: str
        :parameter description: str
        :parameter meta: dict
        :return: Union[Parent, Type[None]]
        """
        service = self._client.parent_service()
        try:
            return service.create(
                application_id=application_id,
                customer_id=customer_id,
                parent_type=parent_type,
                description=description,
                meta=meta
            )
        except (AttributeError, Exception):
            return None

    # Public TenantSettings actions
    def create_tenant_settings(
            self, tenant_name: str, key: str, value: Optional[dict] = None
    ) -> TenantSettings:
        """
        Mandates Maestro Common Domain Model TenantSettings instantiation.
        :parameter tenant_name: str
        :parameter key: str
        :parameter value: dict
        :return: Union[TenantSettings, Type[None]]
        """
        service = self._client.tenant_settings_service()
        return service.create(
            tenant_name=tenant_name, key=key, value=value
        )

    # Public Complemented actions
    def create_complemented_customer_parent(
            self, customer: Union[Customer, str]
    ) -> Optional[Union[Complemented, Parent]]:
        """
        Don't use it, the logicc is wrong
        Mandates Maestro Common Domain Model Parent instantiation,
        driven by a Customer entity, infused into a Complemented entity.
        :parameter customer: Union[Customer, str]
        :return: Optional[Union[Complemented, Parent]]
        If customer object is given as parameters, complemented object is
        returned. If customer's name is given, just parent object is returned
        """
        name = customer.name if isinstance(customer, Customer) else customer
        template = ENTITY_TEMPLATE.format(type='Customer', id=name)

        application = self._get_customer_application(customer=customer)
        if not application:
            absence = APPLICATION_ABSENCE
            content = FAILED_TO_INSTANTIATE.format(
                'Parent') + ' due to' + absence + ' Using a mocked app id'
            _LOG.warning(template + content)
            application_id = DEFAULT_APPLICATION_ID
        else:
            application_id = application.application_id

        parent = self.create_parent(
            application_id=application_id,
            customer_id=name,
            parent_type=self.entity_type(),
            description=SAMPLE_PARENT_DESCRIPTION.format('customer'),
            meta={ACTIVATION_DATE_ATTR: utc_iso()}
        )
        if not parent:
            return
        return parent if not isinstance(customer, Customer) else Complemented(
            entity=customer, complement=parent
        )

    def create_complemented_tenant_settings(
            self, tenant: Tenant, complement_type: str
    ):
        """
        Mandates Maestro Common Domain TenantSettings instantiation, driven by
        a Tenant entity, infused into a Complemented entity, based on a given
        type.
        :parameter tenant: Tenant
        :parameter complement_type: str
        :return: Optional[Complemented]
        """
        settings = self.create_tenant_settings(
            tenant_name=tenant.name,
            key=self.entity_type(complement_type)
        )
        return Complemented(entity=tenant, complement=settings)

    # Public Application related actions

    def get_application(self, application: str) -> Application:
        """
        Returns a bare Maestro Common Domain Model Application.
        Given that inquery has been unsuccessful, returns a None.
        :parameter application: str
        :return: Union[Application, Type[None]]
        """
        return self._get_application(application=application)

    def get_applications(self, customer: Optional[str] = None,
                         _type: Optional[str] = CUSTODIAN_TYPE,
                         deleted: Optional[bool] = False,
                         limit: Optional[int] = None
                         ) -> Iterator[Application]:
        return self.modular_client.application_service().list(
            customer=customer,
            _type=_type,
            deleted=deleted,
            limit=limit
        )

    def get_customer_application(self, customer: Union[str, Customer],
                                 deleted: Optional[bool] = False):
        """
        Returns a bare Maestro Common Domain Model Application,
        related to a given customer, produced by a `fetcher`.
        Given no aforementioned `fetcher` could be established, returns None.
        :param deleted:
        :parameter customer: Union[str]
        :return: Union[Application, Type[None]]
        """
        # TODO remove this method
        return self._get_customer_application(
            customer=customer,
            deleted=deleted
        )

    def i_get_application(self, iterator: Iterator[str]) -> \
            Iterator[Application]:
        """
        Yields a bare Maestro Common Domain Model Application
        :parameter iterator: Iterator[Union[str, Customer]]
        :return: Iterator[Application]
        """
        return self._i_fetch_entity(iterator=iterator, key=Application)

    def create_application(self, customer: str, _type: Optional[str] = None,
                           description: Optional[str] = 'Custodian management application',
                           meta: Optional[dict] = None,
                           secret: Optional[str] = None):
        return self.modular_client.application_service().create(
            customer_id=customer,
            type=_type,
            description=description,
            meta=meta,
            secret=secret
        )

    # Protected entity-common related actions

    def _get_entity_dto(self, entity: Union[BaseModel, Complemented]) -> dict:
        """
        Mediates data transfer object retrieval of a given
        Maestro Common Domain Model entity.
        Object resolution is based on the service-entity priority.
        Given no sub-service is bound to an entity, a default `get_json`
        retriever is used.
        :parameter entity: Union[BaseModel, Complemented]
        :return: Dict
        """
        service_map = self._entity_service_reference_map
        service = service_map.get(entity.__class__)
        payload = []
        try:
            retriever = service.get_dto
            payload.append(entity)
        except (AttributeError, Exception):
            retriever = entity.get_json
        return retriever(*payload)

    def _get_complemented_dto(self, complemented: Complemented) -> dict:
        """
        Unlike Complemented.get_json it also takes into consideration
        complemented entity service's `get_dto` method.
        """
        payload = {}
        entity, complement = complemented.entity, complemented.complement
        if entity:
            payload.update(self.get_dto(entity))
        if complement:
            # it works - that is important :)
            payload.update(complement.get_json().get(
                getattr(complemented, DATA_AGGREGATE_ATTR), dict())
            )
        return payload

    def _get_concealed_attributes(self,
                                  entity: Union[BaseModel, Complemented]):
        """
        Mediates attributes to conceal retrieval for bare and complemented
        entities.
        :parameter entity: Union[BaseModel, Complemented]
        :return: Iterable
        """
        default = self._obscure_bare_entity
        reference_map = self._concealed_attribute_method_map
        obscure: Callable = reference_map.get(entity.__class__, default)
        return obscure(entity) if obscure else tuple()

    def _obscure_bare_entity(self, entity: BaseModel):
        """
        Returns attributes to obscure, based on a provided entity.
        Given no said attributes have been found, returns an empty Iterable.
        :parameter entity: Union[BaseModel, Complemented]
        :return: Iterable
        """
        reference: Dict[Type[BaseModel]] = self._entity_conceal_attribute_map()
        return reference.get(entity.__class__, tuple())

    def _obscure_complemented_entity(self, entity: Complemented):
        """
        Returns Data Transfer Object as a JSON description of a
        given entity model.
        :parameter entity: Union[BaseModel, Complemented]
        :return: Dict
        """
        parent_attributes = self._obscure_bare_entity(entity.complement)
        entity_attributes = self._obscure_bare_entity(entity.entity)
        return entity_attributes + parent_attributes

    def _i_fetch_entity(self, iterator: Iterator, key: Type[Union[
        Customer, Parent, Tenant, Application]]) -> Iterator[
        Union[Customer, Parent, Tenant, Application, Complemented]
    ]:
        """
        Yields Maestro Common Domain Model entities out of a given iterator,
        delegating derivation of said entities to a respective `fetcher`
        methods, described in each `fetcher-map` driven by a provided `key`
        and an item out of the aforementioned iterator.
        :parameter iterator: Iterator
        :parameter key: Type[Union[Customer, Parent, Tenant, Application]]
        :return: Iterator
        """

        for item in iterator:
            if item:
                template = ENTITY_TEMPLATE.format(type=key.__name__, id=item)

                fetcher_map = self._fetcher_map.get(key, lambda: {})
                fetcher: Callable = fetcher_map.get(item.__class__, None)
                entity = fetcher(item) if fetcher else None
                if entity:
                    _LOG.debug(template + FETCHED_TEMPLATE.format(entity))
                    yield entity
                else:
                    _LOG.warning(template + FAILED_TO_DERIVE.format(ENTITY))

    # Protected Customer related actions

    def _get_customer(self, customer: Union[str, Customer]):
        """
        Returns either a bare Customer or Customer-complemented-Parent,
        produced by a `fetcher`, adhering to the type of given `customer`
        parameter.
        Given no aforementioned `fetcher` could be established, returns None.
        :parameter customer: Union[str, Customer]
        :return: Union[Customer, Complemented, Type[None]]
        """
        fetcher: Callable = self._customer_fetcher_map.get(customer.__class__)
        return fetcher(customer=customer) if fetcher else None

    def _fetch_customer(self, customer: str):
        service = self._client.customer_service()
        return service.get(name=customer) if service else None

    # Protected Tenant related actions

    def _get_tenant(self, tenant: Union[str, Tenant]):
        """
        Returns a bare Tenant entity produced by a `fetcher`.
        Given no aforementioned `fetcher` could be established, returns None.
        :parameter tenant: Union[str, Tenant]
        :return: Optional[Union[Tenant, Complemented]]
        """
        fetcher: Callable = self._tenant_fetcher_map.get(tenant.__class__)
        return fetcher(tenant=tenant) if fetcher else None

    def _fetch_tenant(self, tenant: str):
        service = self._client.tenant_service()
        return service.get(tenant_name=tenant) if service else None

    # Protected Customer - Parent related actions

    def _fetch_customer_bound_parent(self, customer: Union[str, Customer]) -> \
            Union[Parent, Type[None]]:
        """
        Mediates Customer related Parent entity retrieval, adhering to
        the respective `fetcher` map.
        :parameter customer: Union[str, Customer]
        :return: Union[Parent, Type[None]]
        """
        reference_map = self._customer_parent_fetcher_map
        fetcher: Callable = reference_map.get(customer.__class__)
        entity: Optional[Parent, Complemented] = fetcher(customer) if fetcher \
            else None
        return getattr(entity, PUBLIC_COMPLEMENT_ATTR, entity)

    def _fetch_bare_customer_parent(
            self, customer: str, complement_type: str = None
    ) -> Union[Parent, Type[None]]:
        """
        Returns a bare Customer related Parent entity.
        :parameter customer: str, index reference.
        :return: Union[Parent, Type[None]]
        """
        id_reference = customer
        type_reference = self.entity_type(complement_type)
        service = self._client.parent_service()
        return None if not service else next(service.i_get_parent_by_customer(
            customer_id=id_reference, parent_type=type_reference
        ), None)

    def _fetch_complemented_customer_parent(
            self, customer: Customer,
            complement_type: str = None
    ) -> Union[Complemented, Customer]:
        """
        Returns a Customer complemented Parent entity, which keeps
        a backward reference to the said Customer.
        :parameter customer: Customer
        :return: Union[Complemented, Type[None]]
        """
        parent = self._fetch_bare_customer_parent(
            customer=customer.name, complement_type=complement_type
        )
        return customer if not parent else Complemented(
            entity=customer, complement=parent
        )

    # Protected Tenant - Tenant-Settings related actions
    def _fetch_bare_tenant_settings(self, tenant: str, complement_type: str
                                    ) -> Optional[TenantSettings]:
        """
        Returns a TenantSettings entity of a given complement type,
        related to a respective Tenant.
        :parameter tenant: str, index reference.
        :parameter complement_type: str
        :return: Optional[TenantSettings]
        """
        name_reference = tenant
        # type reference
        key_reference = self.entity_type(complement_type)
        service = self._client.tenant_settings_service()
        return next(service.i_get_by_tenant(
            tenant=name_reference, key=key_reference
        ), None)

    def _fetch_complemented_tenant_settings(
            self, tenant: Tenant, complement_type: str = TENANT_ENTITY_TYPE
    ) -> Union[Complemented, Tenant]:
        """
        Returns a Tenant complemented entity - adhering to a given type,
        storing backward reference to said Tenant.
        :parameter tenant: Tenant
        :parameter complement_type: str, defaults to a generic entity-type.
        :return: Union[Tenant, Optional[Complemented]]
        """
        settings = self._fetch_bare_tenant_settings(
            tenant=tenant.name, complement_type=complement_type
        )
        return tenant if not settings else Complemented(
            entity=tenant, complement=settings
        )

    def _persist_tenant_settings(self, entity: TenantSettings):
        """
        Mandates Maestro Common Domain Model TenantSettings entity persistence.
        :parameter entity: TenantSettings
        :return: bool, denotes whether entity has been retained
        """
        service, retained = self._client.tenant_settings_service(), True
        try:
            service.save(tenant_setting=entity)
        except (AttributeError, TypeError, Exception) as e:
            _id, data = entity, dict()
            if isinstance(entity, TenantSettings):
                _id = f'{entity.tenant_name}\':\'{entity.key}'
                data.update(entity.get_json())

            template = ENTITY_TEMPLATE.format(type='Tenant', id=_id)
            _LOG.error(
                template + FAILED_TO_PERSIST.format(data) + REASON.format(e)
            )

            retained = False

        return retained

    def _remove_tenant_settings(self, entity: TenantSettings):
        """
        Mandates Maestro Common Domain Model TenantSettings entity erasure.
        :parameter entity: TenantSettings
        :return: bool, denotes whether entity has been retained
        """
        service, erased = self._client.tenant_settings_service(), True
        try:
            service.delete(entity=entity)
        except (AttributeError, TypeError, Exception) as e:
            is_of_type = isinstance(entity, TenantSettings)
            _id = entity.tenant_name if is_of_type else entity
            data = entity.get_json() if is_of_type else dict()

            template = ENTITY_TEMPLATE.format(type='TenantSettings', id=_id)
            _LOG.error(
                template + FAILED_TO_CEASE_PERSISTENCE.format(data)
                + REASON.format(e)
            )

            erased = False

        return erased

    # Protected Parent related actions

    def _get_parent(self, parent: Union[str]) -> Optional[Parent]:
        """
        Returns a bare Parent entity produced by a `fetcher`.
        Given no aforementioned `fetcher` could be established, returns None.
        :parameter parent: Union[str]
        :return: Union[Parent, Type[None]]
        """
        fetcher = self._parent_fetcher_map.get(parent.__class__)
        return fetcher(parent=parent) if fetcher else None

    def _fetch_parent(self, parent: str):
        service = self._client.parent_service()
        return service.get_parent_by_id(parent_id=parent) if service else None

    def _persist_parent(self, parent: Parent):
        """
        Mandates Maestro Common Domain Model Parent entity persistence.
        :parameter parent: Parent
        :return: bool, denotes whether entity has been retained
        """
        service, retained = self._client.parent_service(), True
        try:
            service.save(parent=parent)
        except (AttributeError, TypeError, Exception) as e:
            _id = parent.parent_id if isinstance(parent, Parent) else parent
            data = parent.get_json() if isinstance(parent, Parent) else dict()

            template = ENTITY_TEMPLATE.format(type='Parent', id=_id)
            _LOG.error(
                template + FAILED_TO_PERSIST.format(data) + REASON.format(e)
            )

            retained = False

        return retained

    def _remove_parent(self, parent: Parent) -> bool:
        """
        Mandates Maestro Common Domain Model Parent entity erasure.
        :parameter parent: Parent
        :return: bool, denotes whether entity has been retained
        """
        service, erased = self._client.parent_service(), True
        try:
            service.mark_deleted(parent=parent)
        except ModularException as e:
            if e.code != 200:  # 200 in case already deleted
                erased = False
        return erased

    # Protected Complemented actions
    def _persist_complemented(self, entity: Complemented):
        """
        Delegates persistence of a Complemented Maestro Common Domain Entity.
        :parameter entity: Complemented
        :return: bool, denotes whether entity has been retained
        """
        return self.save(entity=getattr(entity, PUBLIC_COMPLEMENT_ATTR, None))

    def _remove_complemented(self, entity: Complemented):
        """
        Delegates persistence of a Complemented Maestro Common Domain Entity.
        :parameter entity: Complemented
        :return: bool, denotes whether entity has been retained
        """
        return self.delete(
            entity=getattr(entity, PUBLIC_COMPLEMENT_ATTR, None)
        )

    # Protected Application related actions

    def _get_application(self, application: Union[str]
                         ) -> Optional[Application]:
        """
        Returns a bare Application entity produced by a `fetcher`.
        Given no aforementioned `fetcher` could be established, returns None.
        :parameter application: Union[str]
        :return: Union[Application, Type[None]]
        """
        fetcher = self._application_fetcher_map.get(application.__class__)
        return fetcher(application=application) if fetcher else None

    def _fetch_application(self, application: str):
        service = self._client.application_service()
        return service.get_application_by_id(application) if service else None

    def _get_customer_application(
            self, customer: Union[str, Customer], deleted: bool = False
    ) -> Optional[Application]:
        """
        Returns very first Customer related Application entity
        produced by a `fetcher`.
        Given no aforementioned `fetcher` could be established, returns None.
        :parameter customer: Union[str, Customer]
        :return: Union[Application, Type[None]]
        """
        reference = self._customer_application_fetcher_map
        fetcher: Callable = reference.get(customer.__class__)
        return fetcher(customer=customer, deleted=deleted) if fetcher else None

    def _fetch_direct_customer_application(self, customer: str,
                                           deleted: bool = False) -> \
            Optional[Application]:
        service = self._client.application_service()
        return None if not service else next(
            service.i_get_application_by_customer(
                customer_id=customer, deleted=deleted,
                application_type=self.entity_type()
            ), None
        )

    def _fetch_entity_customer_application(self, customer: Customer,
                                           deleted: bool = False) -> \
            Optional[Customer]:
        return self._fetch_direct_customer_application(
            customer=customer.name, deleted=deleted
        )

    def _persist_application(self, application: Application):
        return self.modular_client.application_service().save(application)

    def _persist_tenant(self, tenant: Tenant):
        return self.modular_client.tenant_service().save(tenant)

    def _remove_application(self, application: Application):
        service, erased = self._client.application_service(), True
        try:
            service.mark_deleted(application=application)
        except ModularException as e:
            if e.code != 200:  # 200 in case already deleted
                erased = False
        return erased

    # Protected reference map definitions

    @cached_property
    def _entity_service_reference_map(self):

        _complemented_service = type('ComplementedService', (), {
            'get_dto': staticmethod(lambda c: self._get_complemented_dto(c))
        })

        return {
            Customer: self._client.customer_service(),
            Tenant: self._client.tenant_service(),
            Parent: self._client.parent_service(),
            Application: self._client.application_service(),

            Complemented: _complemented_service
        }

    @cached_property
    def _concealed_attribute_method_map(self):
        return {
            Complemented: self._obscure_complemented_entity
        }

    @cached_property
    def _fetcher_method_map(self):
        return {
            Customer: self._get_customer,
            Tenant: self._get_tenant,
            Parent: self._get_parent,
            Application: self._get_application
        }

    @cached_property
    def _fetcher_map(self):
        return {
            Customer: self._customer_fetcher_map,
            Tenant: self._tenant_fetcher_map,
            Parent: self._parent_fetcher_map,
            Application: self._application_fetcher_map
        }

    @cached_property
    def _customer_fetcher_map(self):
        return {
            str: self._fetch_customer,
            Customer: self._fetch_complemented_customer_parent
        }

    @cached_property
    def _tenant_fetcher_map(self):
        return {
            str: self._fetch_tenant,
            Tenant: self._fetch_complemented_tenant_settings
        }

    @cached_property
    def _parent_fetcher_map(self):
        return {str: self._fetch_parent}

    @cached_property
    def _application_fetcher_map(self):
        return {str: self._fetch_application}

    @cached_property
    def _customer_application_fetcher_map(self):
        return {
            str: self._fetch_direct_customer_application,
            Customer: self._fetch_entity_customer_application
        }

    @cached_property
    def _customer_parent_fetcher_map(self):
        return {
            str: self._fetch_bare_customer_parent,
            Customer: self._fetch_complemented_customer_parent
        }

    @cached_property
    def _persistence_map(self):
        return {
            Parent: self._persist_parent,
            TenantSettings: self._persist_tenant_settings,
            Complemented: self._persist_complemented,
            Application: self._persist_application,
            Tenant: self._persist_tenant
        }

    @cached_property
    def _erasure_map(self):
        return {
            Parent: self._remove_parent,
            TenantSettings: self._remove_tenant_settings,
            Application: self._remove_application,
            Complemented: self._remove_complemented,
        }

    @staticmethod
    def _entity_conceal_attribute_map():
        return {
            Tenant: (MODULAR_MANAGEMENT_ID_ATTR, MODULAR_CLOUD_ATTR,
                     MODULAR_DISPLAY_NAME_ATTR, MODULAR_READ_ONLY_ATTR,
                     MODULAR_DISPLAY_NAME_TO_LOWER, MODULAR_CONTACTS,
                     MODULAR_PARENT_MAP),
            Parent: (MODULAR_DELETION_DATE,),
            Application: (MODULAR_SECRET, MODULAR_IS_DELETED, MODULAR_TYPE,
                          MODULAR_DELETION_DATE)
        }
