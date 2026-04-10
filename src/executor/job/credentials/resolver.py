"""Credential resolution for job execution (tenant, job, platform)."""

import time

from botocore.exceptions import ClientError
from modular_sdk.commons.constants import (
    ENV_KUBECONFIG,
    ParentType,
)
from modular_sdk.models.tenant import Tenant

from executor.helpers.constants import AWS_DEFAULT_REGION
from executor.services import BSP
from helpers.constants import BatchJobEnv, Cloud, PlatformType, TS_EXCLUDED_RULES_KEY
from helpers.log_helper import get_logger
from models.job import Job
from services import SP
from services.clients import Boto3ClientFactory
from services.clients.eks_client import EKSClient
from services.clients.sts import StsClient, TokenGenerator
from services.platform_service import K8STokenKubeconfig, Kubeconfig, Platform

_LOG = get_logger(__name__)


def get_tenant_credentials(tenant: Tenant) -> dict | None:
    """
    If dict is returned it means that we should export that dict to envs
    and start the scan even if the dict is empty
    """

    def _get_parent():
        parent_service = SP.modular_client.parent_service()
        tenant_service = SP.modular_client.tenant_service()

        disabled = next(
            parent_service.get_by_tenant_scope(
                customer_id=tenant.customer_name,
                type_=ParentType.CUSTODIAN_ACCESS,
                tenant_name=tenant.name,
                disabled=True,
                limit=1,
            ),
            None,
        )
        if disabled:
            _LOG.info('Disabled parent is found. Returning None')
            return None

        specific = next(
            parent_service.get_by_tenant_scope(
                customer_id=tenant.customer_name,
                type_=ParentType.CUSTODIAN_ACCESS,
                tenant_name=tenant.name,
                disabled=False,
                limit=1,
            ),
            None,
        )
        if specific:
            _LOG.info('Specific parent is found. Returning it')
            return specific

        if tenant.linked_to:
            _LOG.debug('Trying to get parent_tenant')
            parent_tenant = next(
                tenant_service.i_get_by_dntl(
                    dntl=tenant.linked_to.lower(),
                    cloud=tenant.cloud,
                    limit=1,
                ),
                None,
            )

            if parent_tenant:
                _LOG.info('Getting parent linked to parent_tenant')
                return parent_service.get_linked_parent_by_tenant(
                    tenant=parent_tenant,
                    type_=ParentType.CUSTODIAN_ACCESS,
                )

        _LOG.info('Getting parent with scope ALL')
        return parent_service.get_linked_parent_by_tenant(
            tenant=tenant,
            type_=ParentType.CUSTODIAN_ACCESS,
        )

    mcs = SP.modular_client.maestro_credentials_service()
    application_service = SP.modular_client.application_service()
    credentials = None
    application = None

    _LOG.info('Trying to get creds from `CUSTODIAN_ACCESS` parent')
    parent = _get_parent()

    if parent:
        application = application_service.get_application_by_id(
            parent.application_id,
        )

    if application:
        _creds = mcs.get_by_application(application, tenant)
        if _creds:
            credentials = _creds.dict()
    if credentials is None and BatchJobEnv.ALLOW_MANAGEMENT_CREDS.as_bool():
        _LOG.info(
            'Trying to get creds from maestro management parent & application'
        )
        _creds = mcs.get_by_tenant(tenant=tenant)
        if _creds:
            credentials = _creds.dict()
    if credentials is None:
        _LOG.info('Trying to get creds from instance profile')
        match tenant.cloud:
            case Cloud.AWS:
                try:
                    aid = StsClient.build().get_caller_identity()['Account']
                    _LOG.debug('Instance profile found')
                    if aid == tenant.project:
                        _LOG.info(
                            'Instance profile credentials match to tenant id'
                        )
                        credentials = {}
                except (Exception, ClientError) as e:
                    _LOG.warning(f'No instance credentials found: {e}')
            case Cloud.AZURE:
                try:
                    from c7n_azure.session import Session

                    aid = Session().subscription_id
                    _LOG.info('subscription id found')
                    if aid == tenant.project:
                        _LOG.info('Subscription id matches to tenant id')
                        credentials = {}
                except BaseException:
                    _LOG.warning('Could not find azure subscription id')
    if credentials is not None:
        credentials = mcs.complete_credentials_dict(
            credentials=credentials, tenant=tenant
        )
    return credentials


def get_job_credentials(job: Job, cloud: Cloud) -> dict | None:
    _LOG.info('Trying to resolve credentials from job')
    if not job.credentials_key:
        _LOG.info('No credentials key found for job')
        return
    creds = BSP.credentials_service.get_credentials_from_ssm(
        job.credentials_key, remove=True
    )
    if creds is None:
        _LOG.info('No credentials found for job')
        return
    if cloud is Cloud.GOOGLE:
        creds = BSP.credentials_service.google_credentials_to_file(creds)
    return creds


def get_platform_credentials(job: Job, platform: Platform) -> dict | None:
    """
    Credentials for platform (k8s) only.
    Returns None if credentials cannot be resolved.
    """
    token = None
    if job.credentials_key:
        token = BSP.credentials_service.get_credentials_from_ssm(
            job.credentials_key
        )

    app = SP.modular_client.application_service().get_application_by_id(
        platform.parent.application_id
    )
    kubeconfig = {}
    if app.secret:
        kubeconfig = (
            SP.modular_client.assume_role_ssm_service().get_parameter(
                app.secret
            )
            or {}
        )

    if kubeconfig and token:
        _LOG.debug('Kubeconfig and custom token are provided. Combining both')
        config = Kubeconfig(kubeconfig)
        session = str(int(time.time()))
        user = f'user-{session}'
        context = f'context-{session}'
        cluster = next(config.cluster_names())

        config.add_user(user, token)
        config.add_context(context, cluster, user)
        config.current_context = context
        return {ENV_KUBECONFIG: str(config.to_temp_file())}
    elif kubeconfig:
        _LOG.debug('Only kubeconfig is provided')
        config = Kubeconfig(kubeconfig)
        return {ENV_KUBECONFIG: str(config.to_temp_file())}
    if platform.type != PlatformType.EKS:
        _LOG.warning('No kubeconfig provided and platform is not EKS')
        return
    _LOG.debug(
        'Kubeconfig and token are not provided. Using management creds for EKS'
    )
    tenant = SP.modular_client.tenant_service().get(platform.tenant_name)
    parent = SP.modular_client.parent_service().get_linked_parent_by_tenant(
        tenant=tenant, type_=ParentType.AWS_MANAGEMENT
    )
    if not parent:
        _LOG.warning('Parent AWS_MANAGEMENT not found')
        return
    application = (
        SP.modular_client.application_service().get_application_by_id(
            parent.application_id
        )
    )
    if not application:
        _LOG.warning('Management application is not found')
        return
    creds = SP.modular_client.maestro_credentials_service().get_by_application(
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
    return {ENV_KUBECONFIG: str(token_config.to_temp_file())}


def get_rules_to_exclude(tenant: Tenant) -> set[str]:
    """
    Takes into consideration rules that are excluded for that specific tenant
    and for its customer
    """
    _LOG.info('Querying excluded rules')
    excluded = set()
    ts = SP.modular_client.tenant_settings_service().get(
        tenant_name=tenant.name, key=TS_EXCLUDED_RULES_KEY
    )
    if ts:
        _LOG.info('Tenant setting with excluded rules is found')
        excluded.update(ts.value.as_dict().get('rules') or ())
    cs = SP.modular_client.customer_settings_service().get_nullable(
        customer_name=tenant.customer_name, key=TS_EXCLUDED_RULES_KEY
    )
    if cs:
        _LOG.info('Customer setting with excluded rules is found')
        excluded.update(cs.value.get('rules') or ())
    return excluded
