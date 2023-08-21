from functools import cached_property
from typing import Union, Callable, Optional, Dict, Set, Iterator

from modular_sdk.commons.constants import TENANT_PARENT_MAP_CUSTODIAN_TYPE, \
    TENANT_PARENT_MAP_CUSTODIAN_LICENSES_TYPE, \
    TENANT_PARENT_MAP_SIEM_DEFECT_DOJO_TYPE, \
    TENANT_PARENT_MAP_CUSTODIAN_ACCESS_TYPE
from pynamodb.expressions.condition import Condition

from helpers.constants import CUSTODIAN_TYPE, \
    TENANT_ENTITY_TYPE, ALL, SCOPE_ATTR, CLOUDS_ATTR
from helpers.constants import STEP_GET_TENANT
from helpers.exception import ExecutorException
from helpers.log_helper import get_logger
from models.modular.application import Application
from models.modular.customer import Customer
from models.modular.parents import Parent
from models.modular.parents import ScopeParentMeta
from models.modular.tenant_settings import TenantSettings
from models.modular.tenants import Tenant
from services.clients.modular import ModularClient
from services.environment_service import EnvironmentService

_LOG = get_logger(__name__)


class ModularService:
    def __init__(self, client: ModularClient):
        self._client: ModularClient = client

    @property
    def modular_client(self) -> ModularClient:
        return self._client

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

    def get_customer(self, customer: Union[str, Customer]):
        """
        Returns either a bare Customer, Customer-complemented-Parent or None.
        :parameter customer: Union[str, Customer]
        :return: Union[Customer, Complemented, Type[None]]
        """
        return self._get_customer(customer=customer)

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

    @property
    def _customer_fetcher_map(self):
        return {
            str: self._fetch_customer,
        }

    def _fetch_customer(self, customer: str):
        service = self.modular_client.customer_service()
        return service.get(name=customer) if service else None

    def get_tenant(self, tenant: str) -> Optional[Tenant]:
        return self.modular_client.tenant_service().get(tenant)

    def get_parent(self, parent_id: str) -> Optional[Parent]:
        return self.modular_client.parent_service().get_parent_by_id(parent_id)

    def get_applications(self, customer: Optional[str] = None,
                         _type: Optional[str] = None,
                         deleted: Optional[bool] = False,
                         limit: Optional[int] = None
                         ) -> Iterator[Application]:
        return self.modular_client.application_service().list(
            customer=customer,
            _type=_type,
            deleted=deleted,
            limit=limit
        )

    def get_customer_bound_parents(self, customer: Union[str, Customer],
                                   parent_type: Optional[str] = None,
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

    def get_tenant_bound_setting(self, tenant: Union[str, Tenant],
                                 complemented_type: str = TENANT_ENTITY_TYPE
                                 ) -> Optional[TenantSettings]:
        """
        Returns tenant-bound setting with given type
        """
        name = tenant.name if isinstance(tenant, Tenant) else tenant
        return self._fetch_bare_tenant_settings(
            tenant=name, complement_type=complemented_type
        )

    def get_tenant_parent(self, tenant: Tenant,
                          _type: str) -> Optional[Parent]:
        """
        Returns parent that is linked to the tenant
        :param _type:
        :param tenant:
        :return:
        """
        # not using `ALLOWED_TENANT_PARENT_MAP_KEYS` to restrict more
        assert _type in [TENANT_PARENT_MAP_SIEM_DEFECT_DOJO_TYPE,
                         TENANT_PARENT_MAP_CUSTODIAN_TYPE,
                         TENANT_PARENT_MAP_CUSTODIAN_LICENSES_TYPE,
                         TENANT_PARENT_MAP_CUSTODIAN_ACCESS_TYPE], \
            'Not available parent type'
        parent_id = tenant.get_parent_id(_type)
        if not parent_id:
            _LOG.info(f'Tenant does not have specific "{_type}" parent. '
                      f'Going from another side...')
            return next(self.get_customer_bound_parents(
                customer=tenant.customer_name,
                parent_type=_type,
                is_deleted=False,
                meta_conditions=(Parent.meta[SCOPE_ATTR] == ALL) & Parent.meta[CLOUDS_ATTR].contains(tenant.cloud),
                limit=1
            ), None)  # hopefully there is only one. It must be one
        # parent_id exists
        _LOG.info(f'Tenant contains {_type} parent')
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

    def get_application(self, application: str) -> Optional[Application]:
        return self.modular_client.application_service().get_application_by_id(
            application
        )

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
        return next(self.modular_client.tenant_settings_service().i_get_by_tenant(
            tenant=name_reference, key=key_reference
        ), None)


class TenantService:
    def __init__(self, modular_service: ModularService,
                 environment_service: EnvironmentService):
        self._modular_service = modular_service
        self._environment_service = environment_service

        self._cache: Dict[str, Tenant] = {}

    def get_tenant(self, name: Optional[str] = None,
                   allow_none=False) -> Optional[Tenant]:
        name = name or self._environment_service.tenant_name()
        if name in self._cache:
            _LOG.info(f'Tenant {name} found in cache. Returning')
            return self._cache[name]
        _LOG.info(f'Tenant {name} not found in cache. Querying')
        tenant = self._modular_service.get_tenant(name)
        if not tenant and not allow_none:
            raise ExecutorException(
                step_name=STEP_GET_TENANT,
                reason=f'Could not get tenant by name: {name}'
            )
        if tenant:
            self._cache[name] = tenant
        return tenant
