import uuid
from typing import Iterable, Optional, Callable, Dict, Union, Type, List, \
    Any

from botocore.exceptions import ClientError
from modular_sdk.models.pynamodb_extension.base_model import \
    LastEvaluatedKey, ResultIterator
from modular_sdk.models.pynamodb_extension.pynamodb_to_pymongo_adapter import \
    Result
from modular_sdk.models.region import RegionModel

from handlers.abstracts.abstract_handler import AbstractComposedHandler
from handlers.abstracts.abstract_modular_entity_handler import \
    ModularService, AbstractModularEntityHandler
from helpers import build_response, RESPONSE_CONFLICT, RESPONSE_CREATED, \
    RESPONSE_FORBIDDEN_CODE, RESPONSE_RESOURCE_NOT_FOUND_CODE, \
    RESPONSE_BAD_REQUEST_CODE
from helpers import validate_params
from helpers.constants import GET_METHOD, POST_METHOD, \
    CUSTOMER_ATTR, TENANT_ATTR, TENANTS_ATTR, LIMIT_ATTR, NEXT_TOKEN_ATTR, \
    CLOUD_IDENTIFIER_ATTR, NAME_ATTR, DISPLAY_NAME_ATTR, CLOUD_ATTR, \
    PRIMARY_CONTACTS_ATTR, SECONDARY_CONTACTS_ATTR, DEFAULT_OWNER_ATTR, \
    TENANT_MANAGER_CONTACTS_ATTR, REGION_ATTR, \
    ALLOWED_CLOUDS, PATCH_METHOD, RULES_TO_EXCLUDE_ATTR, \
    RULES_TO_INCLUDE_ATTR, PARAM_COMPLETE
from helpers.log_helper import get_logger
from helpers.regions import get_region_by_cloud
from helpers.time_helper import utc_iso
from models.modular.tenants import Tenant
from services.environment_service import EnvironmentService

_LOG = get_logger(__name__)

TENANTS_PATH = '/tenants'
TENANTS_REGIONS_PATH = '/tenants/regions'

FORBIDDEN_ACCESS = 'Access to {} entity is forbidden.'

AUTHORIZATION = 'Authorization'
SPECIFICATION = 'Query-Specification'
ACCESSIBILITY = 'Entity-Accessibility'

SPECIFICATION_ATTR = 'specification'

PERSISTENCE_ERROR = ' does not exist'
RETAIN_ERROR = ' could not be persisted'

RETRIEVED = ' has been retrieved'
INSTANTIATED = ' has been instantiated'

ATTRIBUTE_UPDATED = ' {} has been set to {}'
RETAINED = ' has been persisted'


class BaseTenantHandler(AbstractModularEntityHandler):

    @property
    def entity(self):
        return TENANT_ATTR.capitalize()


class GetTenantHandler(BaseTenantHandler):
    specification: Dict[str, Any]

    def __init__(self, modular_service: ModularService):
        super().__init__(modular_service=modular_service)
        self._last_evaluated_key = None

    def _reset(self):
        super()._reset()
        # Declares a subjected query specification
        self.specification = dict()
        self._last_evaluated_key = None

    def define_action_mapping(self):
        return {
            TENANTS_PATH: {
                GET_METHOD: self.get_tenants,
            },
        }

    def post_tenant(self, event: dict):
        return {}

    def get_tenants(self, event):
        action = GET_METHOD.capitalize()
        return self._process_action(event=event, action=action)

    @property
    def attributes_to_log(self) -> Dict[Union[str, Type[None]], Iterable[str]]:
        return {
            SPECIFICATION: [CUSTOMER_ATTR, TENANTS_ATTR, SPECIFICATION_ATTR]
        }

    @property
    def responsibilities(self) -> Dict[
        str, Iterable[Callable[[Dict], Union[Dict, Type[None]]]]
    ]:
        return {
            SPECIFICATION: self.specification_responsibilities
        }

    @property
    def specification_responsibilities(self):
        return [
            self._obscure_specification_step,
            self._name_specification_step,
            self._prepare_last_evaluated_key_step,
            self._cid_specification_step
        ]

    @property
    def i_query(self) -> Union[ResultIterator, Result]:
        """
        Produces a query iterator-like output, based on the pending
        specification, resetting it afterwards, having installed the query:
         1. Given only a tenants-name, retrieves demanded entity.
         2. Given a customer name, fetches every entity belonging to the
        customer, singling out active tenants, provided any.
         3. Otherwise, retrieves every active tenant entity.
        :return: Iterator
        """
        specification = self.specification

        customer: Optional[str] = specification.get(CUSTOMER_ATTR)
        scope: Optional[list] = specification.get(TENANTS_ATTR)
        cloud_identifier: Optional[str] = specification.get(
            CLOUD_IDENTIFIER_ATTR)
        complete: bool = specification.get(PARAM_COMPLETE)
        limit: Optional[int] = specification.get(LIMIT_ATTR)
        lek: Optional[str] = specification.get(NEXT_TOKEN_ATTR)
        params = dict(active=True, limit=limit, last_evaluated_key=lek)

        if cloud_identifier:
            tenant = next(self.modular_service.i_get_tenants_by_acc(
                cloud_identifier), None)
            if not tenant or customer and tenant.customer_name != customer or scope and tenant.name not in scope:
                query = iter([])
            else:
                query = iter([tenant])
        elif scope:
            # TODO here can be multiple values but limit won't work
            query = self.modular_service.i_get_tenant(iterator=iter(scope))
        elif customer:
            query = self.modular_service.i_get_customer_tenant(
                **{'customer': customer, **params}
            )
        else:
            query = self.modular_service.i_get_tenants(**params)
        if complete:
            query = self.modular_service.i_get_tenant(query)

        self.specification = dict()
        return query

    def _obscure_specification_step(self, event: Dict) -> \
            Union[Dict, Type[None]]:
        """
        Mandates concealing specification, based on given query.
        Given a non-system customer has issued one, obscures
        the view to the said customer tenants.
        :parameter event: Dict
        :return: Union[Dict, Type[None]]
        """
        customer = event.get(CUSTOMER_ATTR)

        if customer:
            self.specification[CUSTOMER_ATTR] = customer
        self.specification[PARAM_COMPLETE] = event.get(PARAM_COMPLETE)
        return event

    def _name_specification_step(self, event: Dict):
        """
        Mandates Tenant name specification, based on given query.
        Given the aforementioned partition attribute, alters
        specification with the appropriate name iterator.
        :parameter event: Dict
        :return: Union[Dict, Type[None]]
        """
        tenants: list = event.get(TENANTS_ATTR)
        if tenants:
            self.specification[TENANTS_ATTR] = tenants
        return event

    def _cid_specification_step(self, event: Dict):
        """
        Mandates Tenant name specification, based on given query.
        Given the aforementioned partition attribute, alters
        specification with the appropriate name iterator.
        :parameter event: Dict
        :return: Union[Dict, Type[None]]
        """
        cid: str = event.get(CLOUD_IDENTIFIER_ATTR)
        if cid:
            self.specification[CLOUD_IDENTIFIER_ATTR] = cid
        return event

    def _prepare_last_evaluated_key_step(self, event: Dict):
        """
        Retrieves limit and last evaluated key from event and prepares them
        """
        limit = int(event.get(LIMIT_ATTR)) if event.get(LIMIT_ATTR) else None
        old_lek = LastEvaluatedKey.deserialize(
            event.get(NEXT_TOKEN_ATTR) or None)
        self.specification[LIMIT_ATTR] = limit
        self.specification[NEXT_TOKEN_ATTR] = old_lek.value
        return event

    def last_evaluated_key(self) -> LastEvaluatedKey:
        return LastEvaluatedKey(self._last_evaluated_key)

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
        i_query = self.i_query
        dto = [self.modular_service.get_dto(each) for each in i_query]
        self._last_evaluated_key = getattr(i_query, 'last_evaluated_key', None)
        return dto


class PostTenantHandler:
    def __init__(self, modular_service: ModularService,
                 environment_service: EnvironmentService):
        self._modular_service = modular_service
        self._environment_service = environment_service

    def define_action_mapping(self) -> dict:
        return {
            TENANTS_PATH: {
                POST_METHOD: self.post_tenant,
            },
        }

    def post_tenant(self, event: dict) -> dict:
        validate_params(event, (CUSTOMER_ATTR,))
        name = event[NAME_ATTR]
        display_name = event[DISPLAY_NAME_ATTR]
        cloud = event[CLOUD_ATTR]
        acc = event[CLOUD_IDENTIFIER_ATTR]
        customer = event.get(CUSTOMER_ATTR)
        contacts = {
            PRIMARY_CONTACTS_ATTR: event.get(PRIMARY_CONTACTS_ATTR),
            SECONDARY_CONTACTS_ATTR: event.get(SECONDARY_CONTACTS_ATTR),
            TENANT_MANAGER_CONTACTS_ATTR: event.get(
                TENANT_MANAGER_CONTACTS_ATTR),
            DEFAULT_OWNER_ATTR: event.get(DEFAULT_OWNER_ATTR)

        }
        by_name = self._modular_service.get_tenant(name)
        if by_name:
            return build_response(code=RESPONSE_CONFLICT,
                                  content=f'Tenant `{name}` already exists')
        by_acc = next(self._modular_service.i_get_tenants_by_acc(acc), None)
        if by_acc:
            return build_response(
                code=RESPONSE_CONFLICT,
                content=f'Cloud id `{acc}` already exists in db'
            )
        tenant_service = self._modular_service.modular_client.tenant_service()

        if hasattr(tenant_service, 'create'):
            _LOG.warning('Tenant service can create a tenant')
            item = tenant_service.create(
                tenant_name=name,
                display_name=display_name,
                customer_name=customer,
                cloud=cloud,
                acc=acc,
                contacts=contacts
            )
            try:
                tenant_service.save(item)
            except ClientError as e:
                _LOG.info(f'Expected client error occurred trying '
                          f'to save tenant: {e}')
                return build_response(
                    code=RESPONSE_FORBIDDEN_CODE,
                    content='You cannot activate a new tenant on the '
                            'current env'
                )
            return build_response(code=RESPONSE_CREATED,
                                  content=self._modular_service.get_dto(item))
        # no create method
        if not self._environment_service.is_docker():
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE,
                content='Currently the action is not available'
            )
        # kludge for on-prem
        item = Tenant(
            name=name,
            display_name=display_name,
            display_name_to_lower=display_name.lower(),
            read_only=False,
            is_active=True,
            customer_name=customer,
            cloud=cloud,
            activation_date=utc_iso(),
            project=acc,
            contacts=contacts
        )
        item.save()
        return build_response(code=RESPONSE_CREATED,
                              content=self._modular_service.get_dto(item))


class TenantRegionHandler:
    def __init__(self, modular_service: ModularService,
                 environment_service: EnvironmentService):
        self._modular_service = modular_service
        self._environment_service = environment_service

    def define_action_mapping(self) -> dict:
        return {
            TENANTS_REGIONS_PATH: {
                POST_METHOD: self.post_tenant_region,
            },
        }

    def post_tenant_region(self, event: dict) -> dict:
        tenant_name: str = event[TENANT_ATTR]
        region_nn: str = event[REGION_ATTR]
        tenant = self._modular_service.get_tenant(tenant_name)
        if not tenant:
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content=f'Tenant {tenant_name} not found'
            )
        if region_nn in self._modular_service.get_tenant_regions(tenant):
            return build_response(
                code=RESPONSE_CONFLICT,
                content=f'Region: {region_nn} already active for tenant'
            )
        if tenant.cloud not in ALLOWED_CLOUDS:
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE,
                content=f'Tenant {tenant_name} belongs to {tenant.cloud}. '
                        f'Allowed clouds: {", ".join(ALLOWED_CLOUDS)}'
            )
        if region_nn not in get_region_by_cloud(tenant.cloud):
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE,
                content=f'Region {region_nn} does not belong to '
                        f'{tenant.cloud} cloud.'
            )
        region_service = self._modular_service.modular_client.region_service()
        if hasattr(region_service, 'create_light'):
            try:
                region = region_service.get_region_by_native_name(
                    region_nn, tenant.cloud)
                if region:
                    _LOG.info(f'Region with native name: {region_nn} already '
                              f'exists in table. Adding it to tenant.')
                else:
                    _LOG.info(
                        f'Region with native name: {region_nn} not found in '
                        f'table. Creating it and adding to tenant')
                    region = region_service.create_light(
                        maestro_name=region_nn.upper(),
                        native_name=region_nn,
                        cloud=tenant.cloud,
                    )
                    region_service.save(region)
                region_service.activate_region_in_tenant(tenant, region)
                self._modular_service.modular_client.tenant_service().save(
                    tenant)
            except ClientError as e:
                _LOG.info(f'Expected client error occurred trying '
                          f'to save tenant: {e}')
                return build_response(
                    code=RESPONSE_FORBIDDEN_CODE,
                    content='You cannot activate tenant region on the current env'
                )
            return build_response(code=RESPONSE_CREATED,
                                  content=self._modular_service.get_dto(
                                      tenant))
        # no create_light
        if not self._environment_service.is_docker():
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE,
                content='Currently the action is not available'
            )
        region = region_service.get_region_by_native_name(
            region_nn, tenant.cloud)
        if not region:
            region = RegionModel(
                maestro_name=region_nn.upper(),
                native_name=region_nn,
                cloud=tenant.cloud,
                region_id=str(uuid.uuid4()),
                is_active=True,
            )
        tenant.regions.append(region)
        tenant.save()
        return build_response(code=RESPONSE_CREATED,
                              content=self._modular_service.get_dto(tenant))


class PatchTenantHandler:
    """
    Updates only TenantSettings, not the Tenants table itself
    """

    def __init__(self, modular_service: ModularService):
        self._modular_service = modular_service

    def define_action_mapping(self) -> dict:
        return {
            TENANTS_PATH: {
                PATCH_METHOD: self.patch_tenant,
            },
        }

    def patch_tenant(self, event: dict) -> dict:
        tenant_name = event[TENANT_ATTR]
        customer_name = event.get(CUSTOMER_ATTR)
        rules_to_exclude_new = event.get(RULES_TO_EXCLUDE_ATTR)
        rules_to_include_new = event.get(RULES_TO_INCLUDE_ATTR)
        # send_results = event.get(SEND_SCAN_RESULT_ATTR)

        tenant = self._modular_service.get_tenant(tenant_name)
        self._modular_service.assert_tenant_valid(tenant, customer_name)
        complemented = self._modular_service.get_complemented_tenant(tenant)

        rules_to_exclude = set(complemented.rules_to_exclude or [])

        if rules_to_include_new:
            rules_to_exclude -= rules_to_include_new

        if rules_to_exclude_new:
            rules_to_exclude |= rules_to_exclude_new
        complemented.rules_to_exclude = list(rules_to_exclude)
        self._modular_service.save(complemented)

        return build_response(
            content=self._modular_service.get_dto(complemented))


class TenantHandler(AbstractComposedHandler):
    ...


def instantiate_tenant_handler(modular_service: ModularService,
                               environment_service: EnvironmentService):
    patch_handler = PatchTenantHandler(modular_service=modular_service)
    return TenantHandler(
        resource_map={
            TENANTS_PATH: {
                GET_METHOD: GetTenantHandler(modular_service=modular_service),
                POST_METHOD: PostTenantHandler(
                    modular_service=modular_service,
                    environment_service=environment_service),
                PATCH_METHOD: patch_handler
            },
            TENANTS_REGIONS_PATH: {
                POST_METHOD: TenantRegionHandler(
                    modular_service=modular_service,
                    environment_service=environment_service
                )
            }
        }
    )
