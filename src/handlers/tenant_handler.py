from http import HTTPStatus
from itertools import islice
from typing import TYPE_CHECKING
import uuid

from botocore.exceptions import ClientError
from modular_sdk.models.parent import Parent
from modular_sdk.models.region import RegionModel
from modular_sdk.models.tenant import Tenant

from handlers import AbstractHandler, Mapping
from helpers import NextToken
from helpers.constants import (
    CustodianEndpoint,
    DEFAULT_OWNER_ATTR,
    HTTPMethod,
    PRIMARY_CONTACTS_ATTR,
    SECONDARY_CONTACTS_ATTR,
    TENANT_MANAGER_CONTACTS_ATTR,
    TS_EXCLUDED_RULES_KEY,
)
from helpers.lambda_response import ResponseFactory
from helpers.lambda_response import build_response
from helpers.log_helper import get_logger
from helpers.regions import get_region_by_cloud
from helpers.time_helper import utc_iso
from services import SP
from services import modular_helpers
from services.license_service import License, LicenseService
from validators.swagger_request_models import (
    BaseModel,
    MultipleTenantsGetModel,
    TenantExcludedRulesPutModel,
    TenantGetActiveLicensesModel,
    TenantPostModel,
    TenantRegionPostModel,
)
from validators.utils import validate_kwargs

if TYPE_CHECKING:
    from modular_sdk.services.parent_service import ParentService
    from modular_sdk.services.application_service import ApplicationService
    from modular_sdk.services.tenant_service import TenantService
    from modular_sdk.services.region_service import RegionService
    from modular_sdk.services.tenant_settings_service import TenantSettingsService
    from services.license_service import LicenseService

_LOG = get_logger(__name__)


class TenantHandler(AbstractHandler):

    def __init__(self, tenant_service: 'TenantService',
                 parent_service: 'ParentService',
                 application_service: 'ApplicationService',
                 region_service: 'RegionService',
                 tenant_settings_service: 'TenantSettingsService',
                 license_service: 'LicenseService'):
        self._ts = tenant_service
        self._rs = region_service
        self._ps = parent_service
        self._aps = application_service
        self._tss = tenant_settings_service
        self._license_service = license_service

    @classmethod
    def build(cls) -> 'AbstractHandler':
        return cls(
            tenant_service=SP.modular_client.tenant_service(),
            parent_service=SP.modular_client.parent_service(),
            application_service=SP.modular_client.application_service(),
            region_service=SP.modular_client.region_service(),
            tenant_settings_service=SP.modular_client.tenant_settings_service(),
            license_service=SP.license_service
        )

    def get_dto(self, tenant: Tenant) -> dict:
        dct = self._ts.get_dto(tenant)
        dct.pop('contacts', None)
        dct.pop('parent_map', None)
        dct.pop('read_only', None)
        return dct

    @property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.TENANTS: {
                HTTPMethod.GET: self.query,
                # HTTPMethod.POST: self.post
            },
            CustodianEndpoint.TENANTS_TENANT_NAME: {
                HTTPMethod.GET: self.get,
                # HTTPMethod.DELETE: self.delete
            },
            # CustodianEndpoint.TENANTS_TENANT_NAME_REGIONS: {
            #     HTTPMethod.POST: self.add_region
            # },
            CustodianEndpoint.TENANTS_TENANT_NAME_ACTIVE_LICENSES: {
                HTTPMethod.GET: self.get_tenant_active_license
            },
            CustodianEndpoint.TENANTS_TENANT_NAME_EXCLUDED_RULES: {
                HTTPMethod.PUT: self.set_excluded_rules,
                HTTPMethod.GET: self.get_excluded_rules
            }
        }

    @validate_kwargs
    def get(self, event: BaseModel, tenant_name: str):
        # TODO api implement get by account id
        tenant = self._ts.get(tenant_name)
        if not tenant:
            _LOG.info('Tenant was not found by name. Looking by account id')
            tenant = next(self._ts.i_get_by_acc(
                acc=tenant_name,
                limit=1,
            ), None)
        if not tenant or event.customer and tenant.customer_name != event.customer:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                'Tenant is not found'
            ).exc()
        return build_response(self.get_dto(tenant))

    @validate_kwargs
    def query(self, event: MultipleTenantsGetModel):
        cursor = self._ts.i_get_tenant_by_customer(
            customer_id=event.customer,
            active=event.active,
            cloud=event.cloud.value if event.cloud else None,
            limit=event.limit,
            last_evaluated_key=NextToken.deserialize(event.next_token).value
        )
        items = list(cursor)
        return ResponseFactory(HTTPStatus.OK).items(
            it=map(self.get_dto, items),
            next_token=NextToken(cursor.last_evaluated_key)
        ).build()

    @validate_kwargs
    def post(self, event: TenantPostModel):
        by_name = self._ts.get(event.name)
        if by_name:
            raise ResponseFactory(HTTPStatus.CONFLICT).message(
                'Name already used').exc()
        by_acc = next(self._ts.i_get_by_acc(event.account_id, limit=1), None)
        if by_acc:
            raise ResponseFactory(HTTPStatus.CONFLICT).message(
                'Tenant with such account id already used'
            ).exc()
        # modular sdk does not have create() method. Going manually

        item = Tenant(
            name=event.name,
            display_name=event.display_name,
            display_name_to_lower=event.display_name.lower(),
            read_only=False,
            is_active=True,
            customer_name=event.customer,
            cloud=event.cloud,
            activation_date=utc_iso(),
            project=event.account_id,
            contacts={
                PRIMARY_CONTACTS_ATTR: event.primary_contacts,
                SECONDARY_CONTACTS_ATTR: event.secondary_contacts,
                TENANT_MANAGER_CONTACTS_ATTR: event.tenant_manager_contacts,
                DEFAULT_OWNER_ATTR: event.default_owner

            }
        )
        try:
            item.save()
        except ClientError:
            _LOG.exception('Cannot save tenant to DB')
            raise ResponseFactory(HTTPStatus.FORBIDDEN).message(
                'Cannot create tenant'
            ).exc()
        return build_response(content=self.get_dto(item),
                              code=HTTPStatus.CREATED)

    @validate_kwargs
    def delete(self, event: BaseModel, tenant_name: str):
        item = self._ts.get(tenant_name)
        if not item or event.customer and item.customer_name != event.customer:
            return build_response(code=HTTPStatus.NO_CONTENT)
        try:
            item.delete()
        except ClientError:
            _LOG.exception('Cannot delete tenant to DB')
            raise ResponseFactory(HTTPStatus.FORBIDDEN).message(
                'Cannot delete tenant'
            ).exc()
        return build_response(code=HTTPStatus.NO_CONTENT)

    @validate_kwargs
    def add_region(self, event: TenantRegionPostModel, tenant_name: str):
        tenant = self._ts.get(tenant_name)
        if not tenant:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                'Tenant not found').exc()
        if event.region in modular_helpers.get_tenant_regions(tenant):
            return build_response(
                code=HTTPStatus.CONFLICT,
                content=f'Region: {event.region} already active for tenant'
            )
        if event.region not in get_region_by_cloud(tenant.cloud):
            return build_response(
                code=HTTPStatus.BAD_REQUEST,
                content='Region does not belong to tenant`s cloud'
            )
        region = self._rs.get_region_by_native_name(event.region, tenant.cloud)
        if not region:
            region = RegionModel(
                maestro_name=event.region.upper(),
                native_name=event.region,
                cloud=tenant.cloud,
                region_id=str(uuid.uuid4()),
                is_active=True,
            )
        tenant.regions.append(region)
        tenant.save()
        return build_response(code=HTTPStatus.CREATED,
                              content=self.get_dto(tenant))

    def get_complemented_license_dto(self, parent: Parent,
                                     lic: License) -> dict:
        dct = self._license_service.dto(lic)
        dct['scope'] = parent.scope
        return dct

    @validate_kwargs
    def get_tenant_active_license(self, event: TenantGetActiveLicensesModel, 
                                  tenant_name: str):
        """
        Generally, only the license manager can determine whether the license
        is allowed for a specific tenant. So license_key is enough for Rule
        Engine to execute a scan for a tenant. But on top of that Rule
        Engine & Maestro have another mechanism of "activation" licenses for
        tenants. First, we add a license to Custodian installation (which is
        already enough). Second, we additionally link this license to
        ALL/SPECIFIC tenants. This endpoint resolves that linkage and returns
        a list of licenses that are active for a specific tenant
        """
        tenant = self._ts.get(tenant_name)
        if not tenant or event.customer and tenant.customer_name != event.customer:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                'Tenant is not found'
            ).exc()
        it = self._license_service.iter_tenant_licenses(tenant,
                                                        limit=event.limit)
        it = islice(it, event.limit)
        return build_response(
            content=(self.get_complemented_license_dto(*i) for i in it)
        )

    @validate_kwargs
    def get_excluded_rules(self, event: BaseModel, tenant_name: str):
        tenant = self._ts.get(tenant_name)
        if not tenant or event.customer and tenant.customer_name != event.customer:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                'Tenant is not found'
            ).exc()
        item = self._tss.get(
            tenant_name=tenant.name,
            key=TS_EXCLUDED_RULES_KEY
        )
        if not item:
            return build_response({'rules': [], 'tenant_name': tenant.name})
        return build_response({
            'rules': item.value.as_dict().get('rules') or [],
            'tenant_name': tenant.name
        })

    @validate_kwargs
    def set_excluded_rules(self, event: TenantExcludedRulesPutModel, 
                           tenant_name: str):
        tenant = self._ts.get(tenant_name)
        if not tenant or event.customer and tenant.customer_name != event.customer:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                'Tenant is not found'
            ).exc()
        data = {'rules': list(event.rules)}
        item = self._tss.create(
            tenant_name=tenant.name,
            key=TS_EXCLUDED_RULES_KEY,
            value=data
        )
        self._tss.save(item)
        data['tenant_name'] = tenant.name
        return build_response(data)

