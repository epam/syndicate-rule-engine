from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, cast, overload

from modular_sdk.commons.constants import ParentType

from helpers.log_helper import get_logger
from services.clients import Boto3ClientFactory
from services.clients.eks_client import EKSClient
from services.clients.sts import TokenGenerator
from services.platform_service import (
    K8STokenKubeconfig,
    Kubeconfig,
    PlatformService,
    PlatformType,
)


if TYPE_CHECKING:
    from modular_sdk.modular import ModularServiceProvider

    from services.platform_service import Platform


_LOG = get_logger(__name__)


class K8sCredentialsService:
    """
    Service to get Kubeconfig for K8s platforms.
    """

    __slots__ = ('_msp', '_psp')

    def __init__(
        self,
        modular_service_provider: ModularServiceProvider,
        platform_service: PlatformService,
    ) -> None:
        self._msp = modular_service_provider
        self._psp = platform_service

    @classmethod
    def build(
        cls,
        modular_service_provider: ModularServiceProvider | None = None,
        platform_service: PlatformService | None = None,
    ) -> K8sCredentialsService:
        from services import SP

        return cls(
            modular_service_provider=modular_service_provider
            or SP.modular_client,
            platform_service=platform_service or SP.platform_service,
        )

    @overload
    def get_kubeconfig(
        self,
        *,
        platform_id: str,
        token: str | None = None,
    ) -> Kubeconfig | K8STokenKubeconfig | None: ...

    @overload
    def get_kubeconfig(
        self,
        *,
        platform: Platform,
        token: str | None = None,
    ) -> Kubeconfig | K8STokenKubeconfig | None: ...

    def get_kubeconfig(
        self,
        *,
        platform_id: str | None = None,
        platform: Platform | None = None,
        token: str | None = None,
    ) -> Kubeconfig | K8STokenKubeconfig | None:
        # TODO: not good practice to import from executor, because it`s specific scope of executor, but we need to use AWS_DEFAULT_REGION here
        from executor.helpers.constants import (
            AWS_DEFAULT_REGION,
        )

        app_service = self._msp.application_service()

        platform = self._resolve_platform(platform_id, platform)
        if not platform:
            _LOG.warning('Platform not found')
            return None
        app = app_service.get_application_by_id(platform.parent.application_id)

        kubeconfig: dict[str, Any] | None = None
        if app and app.secret:
            _LOG.debug(
                'Getting kubeconfig from SSM for application '
                f'{app.application_id} with secret {app.secret}'
            )
            kubeconfig = cast(
                dict[str, Any],
                self._msp.assume_role_ssm_service().get_parameter(app.secret),
            )

        if kubeconfig and token:
            _LOG.debug(
                'Kubeconfig and custom token are provided. Combining both'
            )
            config = Kubeconfig(kubeconfig)
            session = str(int(time.time()))
            user = f'user-{session}'
            context = f'context-{session}'
            cluster = next(
                config.cluster_names()
            )  # always should be 1 at least

            config.add_user(user, token)
            config.add_context(context, cluster, user)
            config.current_context = context
            return config
        elif kubeconfig:
            _LOG.debug('Only kubeconfig is provided')
            config = Kubeconfig(kubeconfig)
            return config
        if platform.type != PlatformType.EKS:
            _LOG.warning('No kubeconfig provided and platform is not EKS')
            return
        _LOG.debug(
            'Kubeconfig and token are not provided. Using management creds for EKS'
        )
        tenant = self._msp.tenant_service().get(platform.tenant_name)
        if not tenant:
            _LOG.warning(f'Tenant {platform.tenant_name} not found')
            return
        parent = self._msp.parent_service().get_linked_parent_by_tenant(
            tenant=tenant,
            type_=ParentType.AWS_MANAGEMENT,
        )
        # TODO: get tenant credentials here somehow
        if not parent:
            _LOG.warning('Parent AWS_MANAGEMENT not found')
            return
        application = app_service.get_application_by_id(parent.application_id)
        if not application:
            _LOG.warning('Management application is not found')
            return
        creds = self._msp.maestro_credentials_service().get_by_application(
            application, tenant
        )
        if not creds:
            _LOG.warning(
                f'No credentials in application: {application.application_id}'
            )
            return
        cl = EKSClient.build()
        cl.client = Boto3ClientFactory(EKSClient.service_name).build(
            region_name=platform.region,
            aws_access_key_id=creds.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=creds.AWS_SECRET_ACCESS_KEY,
            aws_session_token=creds.AWS_SESSION_TOKEN,
        )
        cluster = cl.describe_cluster(platform.name)
        if not cluster:
            _LOG.error(
                f'No cluster with name: {platform.name} '
                f'in region: {platform.region}'
            )
            return
        sts = Boto3ClientFactory('sts').from_keys(
            aws_access_key_id=creds.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=creds.AWS_SECRET_ACCESS_KEY,
            aws_session_token=creds.AWS_SESSION_TOKEN,
            region_name=AWS_DEFAULT_REGION,
        )
        token_config = K8STokenKubeconfig(
            endpoint=cluster['endpoint'],
            ca=cluster['certificateAuthority']['data'],
            token=TokenGenerator(sts).get_token(platform.name),
            insecure_skip_tls_verify=False,
        )
        return token_config

    def _resolve_platform(
        self,
        platform_id: str | None = None,
        platform: Platform | None = None,
    ) -> Platform | None:
        if platform_id is not None and platform is not None:
            raise ValueError('Both platform_id and platform are provided')
        if platform_id is None and platform is None:
            raise ValueError('Either platform_id or platform must be provided')
        if platform_id:
            platform = self._psp.get_nullable(hash_key=platform_id)
            if not platform:
                _LOG.warning(f'Platform {platform_id} not found')
                return None
            return platform
        return platform
