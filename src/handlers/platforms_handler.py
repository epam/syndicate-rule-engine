from enum import Enum
from http import HTTPStatus
from typing import Iterator, Generator, Tuple, Optional

from modular_sdk.commons.constants import ParentScope, \
    ApplicationType, ParentType
from modular_sdk.models.application import Application
from modular_sdk.models.parent import Parent
from modular_sdk.services.application_service import ApplicationService
from modular_sdk.services.impl.maestro_credentials_service import \
    K8SServiceAccountApplicationMeta, K8SServiceAccountApplicationSecret
from modular_sdk.services.parent_service import ParentService

from handlers.abstracts.abstract_handler import AbstractHandler
from helpers import build_response
from helpers.constants import HTTPMethod, PlatformType
from helpers.log_helper import get_logger
from services import SP
from services.modular_service import ModularService
from validators.request_validation import PlatformK8sNativePost, \
    PlatformK8sDelete, PlatformK8sEksPost, PreparedEvent, PlatformK8sQuery
from validators.utils import validate_kwargs

_LOG = get_logger(__name__)


class PlatformsHandler(AbstractHandler):
    def __init__(self, modular_service: ModularService):
        self._modular_service = modular_service

    @classmethod
    def build(cls) -> 'PlatformsHandler':
        return cls(
            modular_service=SP.modular_service(),
        )

    @property
    def ps(self) -> ParentService:
        return self._modular_service.modular_client.parent_service()

    @property
    def aps(self) -> ApplicationService:
        return self._modular_service.modular_client.application_service()

    def define_action_mapping(self) -> dict:
        return {
            '/platforms/k8s': {
                HTTPMethod.GET: self.list_k8s
            },
            '/platforms/k8s/native': {
                HTTPMethod.POST: self.post_k8s_native,
            },
            '/platforms/k8s/eks': {
                HTTPMethod.POST: self.post_k8s_eks,
            },
            '/platforms/k8s/native/{id}': {
                HTTPMethod.DELETE: self.delete_k8s_native
            },
            '/platforms/k8s/eks/{id}': {
                HTTPMethod.DELETE: self.delete_k8s_eks
            }
        }

    def dto(self, parent: Parent,
            application: Optional[Application] = None) -> dict:
        native = self.get_type(parent) == PlatformType.NATIVE
        data = {
            'id': parent.parent_id,
            'name': parent.meta.as_dict().get('name'),
            'tenant_name': parent.tenant_name,
            'has_token': bool(application and bool(application.secret) and native),
            'type': PlatformType.NATIVE if native else PlatformType.EKS,
            'description': parent.description
        }
        if not native:
            data['region'] = parent.meta.as_dict().get('region')
        else:
            data['endpoint'] = application.meta.as_dict().get('endpoint')
        return data

    @validate_kwargs
    def post_k8s_eks(self, event: PlatformK8sEksPost):
        tenant_item = self._modular_service.get_tenant(event.tenant_name)
        self._modular_service.assert_tenant_valid(tenant_item, event.customer)
        application = self._modular_service.get_application(
            event.application_id)
        if not application or application.type not in (
                ApplicationType.AWS_ROLE.value,
                ApplicationType.AWS_CREDENTIALS.value):
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content=f'Application with AWS credentials and ID '
                        f'{event.application_id} not found'
            )
        parent = self.ps._create(
            customer_id=tenant_item.customer_name,
            application_id=application.application_id,
            type_=ParentType.PLATFORM_K8S.value,
            description=event.description or 'Custodian created eks cluster',
            meta={'name': event.name, 'region': event.region,
                  'type': PlatformType.EKS.value},
            scope=ParentScope.SPECIFIC,
            tenant_name=tenant_item.name
        )
        self.ps.save(parent)
        return build_response(content=self.dto(parent, application))

    @validate_kwargs
    def post_k8s_native(self, event: PlatformK8sNativePost) -> dict:
        tenant_item = self._modular_service.get_tenant(event.tenant_name)
        self._modular_service.assert_tenant_valid(tenant_item, event.customer)
        application = self.aps.create(
            customer_id=tenant_item.customer_name,
            type=ApplicationType.K8S_SERVICE_ACCOUNT.value,
            description='Custodian auto created k8s application',
            meta=K8SServiceAccountApplicationMeta(
                endpoint=str(event.endpoint),
                ca=event.certificate_authority
            ).dict()
        )
        if event.token:
            _LOG.info('K8s service account token was given. Saving to app')
            cl = self._modular_service.modular_client.assume_role_ssm_service()
            secret_name = cl.safe_name(
                name=application.customer_id,
                prefix='m3.custodian.k8s',
                date=True
            )
            secret = cl.put_parameter(
                name=secret_name,
                value=K8SServiceAccountApplicationSecret(event.token).dict()
            )
            if not secret:
                _LOG.warning('Something went wrong trying to same token '
                             'to ssm. Keeping application.secret empty')
            application.secret = secret
        parent = self.ps._create(
            customer_id=tenant_item.customer_name,
            application_id=application.application_id,
            type_=ParentType.PLATFORM_K8S.value,
            description=event.description or 'Custodian created native k8s',
            meta={'name': event.name, 'type': PlatformType.NATIVE.value},
            scope=ParentScope.SPECIFIC,
            tenant_name=tenant_item.name
        )
        self.ps.save(parent)
        self.aps.save(application)
        return build_response(content=self.dto(parent, application))

    def _with_applications(self, it: Iterator[Parent]
                           ) -> Generator[Tuple[Parent, Optional[Application]], None, None]:
        """
        Yields parent and its application. Skips applications for EKS parents
        because in such situation they don't contain data
        :param it:
        :return:
        """
        _cache = {}
        for parent in it:
            if self.get_type(parent) == PlatformType.EKS:
                yield parent, None
                continue
            aid = parent.application_id
            if aid in _cache:
                yield parent, _cache[aid]
                continue
            application = self.aps.get_application_by_id(aid)
            _cache[aid] = application
            yield parent, application

    @staticmethod
    def get_type(platform: Parent) -> PlatformType:
        return PlatformType[platform.meta.as_dict().get('type')]

    @validate_kwargs
    def list_k8s(self, event: PlatformK8sQuery) -> dict:
        it = self.ps.query_by_scope_index(
            customer_id=event.customer,
            tenant_or_cloud=event.tenant_name,
            scope=ParentScope.SPECIFIC,
            type_=ParentType.PLATFORM_K8S,
            is_deleted=False
        )
        return build_response(content=(
            self.dto(pair[0], pair[1]) for pair in self._with_applications(it)
        ))

    @validate_kwargs
    def delete_k8s_native(self, event: PlatformK8sDelete) -> dict:
        parent = self.ps.get_parent_by_id(event.id)
        if not parent or parent.is_deleted or self.get_type(
                parent) != PlatformType.NATIVE:
            _LOG.debug(f'Parent {parent} not found or already deleted')
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content=f'Native platform {event.id} not found'
            )

        application = self.aps.get_application_by_id(parent.application_id)
        if application.secret:
            cl = self._modular_service.modular_client.assume_role_ssm_service()
            cl.delete_parameter(application.secret)
        self.ps.mark_deleted(parent)
        self.aps.mark_deleted(application)
        return build_response(code=HTTPStatus.NO_CONTENT)

    @validate_kwargs
    def delete_k8s_eks(self, event: PlatformK8sDelete) -> dict:
        parent = self.ps.get_parent_by_id(event.id)
        if not parent or parent.is_deleted or self.get_type(
                parent) != PlatformType.EKS:
            _LOG.debug(f'Parent {parent} not found or already deleted')
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content=f'EKS platform {event.id} not found'
            )
        self.ps.mark_deleted(parent)
        return build_response(code=HTTPStatus.NO_CONTENT)
