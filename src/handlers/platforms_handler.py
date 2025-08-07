from http import HTTPStatus

from modular_sdk.commons.constants import ApplicationType, ParentScope, \
    ParentType
from modular_sdk.modular import Modular
from modular_sdk.services.application_service import ApplicationService
from modular_sdk.services.tenant_service import TenantService

from handlers import AbstractHandler, Mapping
from helpers.constants import Endpoint, HTTPMethod
from helpers.lambda_response import ResponseFactory, build_response
from helpers.log_helper import get_logger
from services import SP
from services.rbac_service import TenantsAccessPayload
from services import modular_helpers
from services.abs_lambda import ProcessedEvent
from services.platform_service import K8STokenKubeconfig, Platform, \
    PlatformService
from validators.swagger_request_models import (
    BaseModel,
    PlatformK8SPostModel,
    PlatformK8sQueryModel,
)
from validators.utils import validate_kwargs

_LOG = get_logger(__name__)


class PlatformsHandler(AbstractHandler):
    def __init__(self, modular_client: Modular,
                 platform_service: PlatformService):
        self._modular_client = modular_client
        self._ps = platform_service

    @classmethod
    def build(cls) -> 'PlatformsHandler':
        return cls(
            modular_client=SP.modular_client,
            platform_service=SP.platform_service
        )

    @property
    def aps(self) -> ApplicationService:
        return self._modular_client.application_service()

    @property
    def ts(self) -> TenantService:
        return self._modular_client.tenant_service()

    @property
    def mapping(self) -> Mapping:
        return {
            Endpoint.PLATFORMS_K8S: {
                HTTPMethod.POST: self.post_k8s,
                HTTPMethod.GET: self.list_k8s,
            },
            Endpoint.PLATFORMS_K8S_ID: {
                HTTPMethod.GET: self.get_k8s,
                HTTPMethod.DELETE: self.delete_k8s
            }
        }

    @validate_kwargs
    def post_k8s(self, event: PlatformK8SPostModel, _pe: ProcessedEvent,
                 _tap: TenantsAccessPayload):
        tenant = self.ts.get(event.tenant_name)
        if tenant and not _tap.is_allowed_for(tenant.name):
            tenant = None
        modular_helpers.assert_tenant_valid(tenant, event.customer)

        application = self.aps.build(
            customer_id=tenant.customer_name,
            type=ApplicationType.K8S_KUBE_CONFIG.value,
            description='Custodian auto created k8s application',
            created_by=_pe['cognito_user_id'],
            meta={}
        )
        if event.endpoint:
            _LOG.info('K8s endpoint and ca were given creating kubeconfig')
            cl = self._modular_client.assume_role_ssm_service()
            secret_name = cl.safe_name(
                name=application.customer_id,
                prefix='m3.custodian.k8s',
                date=True
            )
            secret = cl.put_parameter(
                name=secret_name,
                value=K8STokenKubeconfig(
                    endpoint=str(event.endpoint),
                    ca=event.certificate_authority,
                    token=event.token
                ).build_config()
            )
            application.secret = secret
        platform = self._ps.create(
            tenant=tenant,
            application=application,
            name=event.name,
            type_=event.type,
            created_by=_pe['cognito_user_id'],
            region=event.region.value if event.region else None,
            description=event.description
        )
        self._ps.save(platform)
        return build_response(content=self._ps.dto(platform))

    @validate_kwargs
    def delete_k8s(self, event: BaseModel, platform_id: str):
        platform = self._ps.get_nullable(hash_key=platform_id)
        if not platform:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content=self._ps.not_found_message(platform_id)
            )
        self._ps.fetch_application(platform)
        if platform.application.secret:
            cl = self._modular_client.assume_role_ssm_service()
            cl.delete_parameter(platform.application.secret)
        self._ps.delete(platform)
        return build_response(code=HTTPStatus.NO_CONTENT)

    @validate_kwargs
    def get_k8s(self, event: BaseModel, platform_id: str):
        platform = self._ps.get_nullable(hash_key=platform_id)
        if not platform:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                self._ps.not_found_message(platform_id)
            ).exc()
        return build_response(content=self._ps.dto(platform))

    @validate_kwargs
    def list_k8s(self, event: PlatformK8sQueryModel):
        ps = self._modular_client.parent_service()
        it = ps.query_by_scope_index(
            customer_id=event.customer,
            tenant_or_cloud=event.tenant_name,
            scope=ParentScope.SPECIFIC,
            type_=ParentType.PLATFORM_K8S,
            is_deleted=False
        )
        return build_response(content=(
            self._ps.dto(item) for item in map(Platform, it)
        ))
