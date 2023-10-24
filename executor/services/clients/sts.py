import base64
from datetime import datetime
from time import time
from typing import TypedDict

from botocore.client import BaseClient

from helpers.log_helper import get_logger
from services.clients import Boto3ClientWrapper

_LOG = get_logger(__name__)
TOKEN_PREFIX = 'k8s-aws-v1.'
CLUSTER_NAME_HEADER = 'x-k8s-aws-id'


class _AssumeRoleCredentials(TypedDict):
    AccessKeyId: str
    SecretAccessKey: str
    SessionToken: str
    Expiration: datetime


class AsuumeRoleResult(TypedDict):
    Credentials: _AssumeRoleCredentials
    AssumedRoleUser: dict
    PackedPolicySize: int
    SourceIdentity: str


class StsClient(Boto3ClientWrapper):
    service_name = 'sts'

    @classmethod
    def build(cls):
        return cls()

    def assume_role(self, role_arn: str, duration: int = 3600,
                    role_session_name: str = None) -> AsuumeRoleResult:
        role_session_name = role_session_name or f'Custodian-scan-{time()}'
        params = {
            'RoleArn': role_arn,
            'RoleSessionName': role_session_name,
            'DurationSeconds': duration
        }
        return self.client.assume_role(**params)


class TokenGenerator:
    """
    From python AWS CLI
    """

    def __init__(self, sts_client: BaseClient):
        self._sts_client = sts_client
        self._register_cluster_name_handlers(self._sts_client)

    def get_token(self, cluster_name: str):
        """
        Generate a presigned url token to pass to kubectl
        """
        url = self._get_presigned_url(cluster_name)
        token = TOKEN_PREFIX + base64.urlsafe_b64encode(
            url.encode('utf-8')).decode('utf-8').rstrip('=')
        return token

    def _get_presigned_url(self, cluster_name: str):
        return self._sts_client.generate_presigned_url(
            'get_caller_identity',
            Params={'ClusterName': cluster_name},
            ExpiresIn=60,
            HttpMethod='GET',
        )

    def _register_cluster_name_handlers(self, sts_client):
        sts_client.meta.events.register(
            'provide-client-params.sts.GetCallerIdentity',
            self._retrieve_cluster_name
        )
        sts_client.meta.events.register(
            'before-sign.sts.GetCallerIdentity',
            self._inject_cluster_name_header
        )

    def _retrieve_cluster_name(self, params, context, **kwargs):
        if 'ClusterName' in params:
            context['eks_cluster'] = params.pop('ClusterName')

    def _inject_cluster_name_header(self, request, **kwargs):
        if 'eks_cluster' in request.context:
            request.headers[
                CLUSTER_NAME_HEADER] = request.context['eks_cluster']
