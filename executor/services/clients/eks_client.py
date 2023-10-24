from typing import TypedDict, Optional

from botocore.exceptions import ClientError

from services.clients import Boto3ClientWrapper


class _CertificateAuthority(TypedDict):
    data: str


class Cluster(TypedDict):
    name: str
    arn: str
    certificateAuthority: _CertificateAuthority
    endpoint: str
    # and other params


class EKSClient(Boto3ClientWrapper):
    service_name = 'eks'

    @classmethod
    def build(cls):
        return cls()

    def describe_cluster(self, name: str) -> Optional[Cluster]:
        try:
            return self._client.describe_cluster(name=name)['cluster']
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                return
            raise e
