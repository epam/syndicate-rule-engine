from typing import Optional, List

from pynamodb.exceptions import DoesNotExist
from pynamodb.expressions.condition import Condition
from pynamodb.indexes import GlobalSecondaryIndex

from helpers.constants import CLOUD_IDENTIFIER_ATTR, TRUSTED_ROLE_ARN, ENABLED, \
    CLOUD_ATTR
from helpers.log_helper import get_logger
from models.credentials_manager import CredentialsManager

_ATTRS_TO_GET = {CLOUD_IDENTIFIER_ATTR, TRUSTED_ROLE_ARN, ENABLED, CLOUD_ATTR}

MA_SSM_CLIENT_ID, MA_DYNAMODB_CLIENT_ID = 'client_id', 'clientId'
MA_SSM_API_KEY = 'api_key'
MA_SSM_TENANT_ID, MA_DYNAMODB_TENANT_ID = 'tenant_id', 'tenantId'

MA_SSM_ACCESS_KEY_ID = 'accessKeyId'
MA_SSM_SECRET_ACCESS_KEY = 'secretAccessKey'
MA_SSM_SESSION_TOKEN = 'sessionToken'  # not sure, it's a guess
MA_SSM_DEFAULT_REGION = 'defaultRegion'
MA_DYNAMODB_ROLE_NAME = 'roleName'
MA_DYNAMODB_ACCOUNT_NUMBER = 'accountNumber'

MA_AWS, MA_AZURE, MA_GOOGLE = 'AWS', 'AZURE', 'GOOGLE'

_LOG = get_logger(__name__)


class CredentialsManagerService:
    """
    Manage Credentials Manager
    """

    @staticmethod
    def get_credentials_configuration(cloud_identifier: str, cloud: str
                                      ) -> Optional[CredentialsManager]:
        return CredentialsManager.get_nullable(hash_key=cloud_identifier,
                                               range_key=cloud.lower())

    @classmethod
    def inquire(
            cls,
            cloud: Optional[str] = None,
            cloud_identifier: Optional[str] = None,
            customer: Optional[str] = None, tenants: Optional[List[str]] = None
    ):
        if cloud:
            cloud = cloud.lower()
        index_payload = dict()

        if customer:
            index_payload.update(
                index=CredentialsManager.customer_cloud_identifier_index,
                hash_key=customer
            )

        if tenants is not None:

            if len(tenants) > 1 and customer:

                fc = None
                for _tenant in tenants:
                    _fc = CredentialsManager.tenant == _tenant
                    fc = (fc | _fc) if fc is not None else _fc

                # Inherits the customer-based payload.
                index_payload.update(filter_condition=fc)

            elif len(tenants) == 1:
                tenant = tenants[0]
                index_payload.update(
                    index=CredentialsManager.tenant_cloud_identifier_index,
                    hash_key=tenant
                )

            # Otherwise, query is not tenant/customer biased.

        # Establish a default output.
        output = iter([])

        if index_payload:
            index_payload.update(cloud=cloud, cid=cloud_identifier)
            output = cls._index_inquery(**index_payload)

        elif cloud and cloud_identifier:
            entity = cls.get_credentials_configuration(
                cloud=cloud, cloud_identifier=cloud_identifier
            )
            if entity:
                output = iter([entity])

        # todo swap hash(cloud) and sort(cid) keys.
        elif cloud:
            fc = CredentialsManager.cloud == cloud
            output = CredentialsManager.scan(filter_condition=fc)

        elif cloud_identifier:
            output = CredentialsManager.query(hash_key=cloud_identifier)
        else:
            output = CredentialsManager.scan()

        return output

    @staticmethod
    def _index_inquery(
            index: GlobalSecondaryIndex, hash_key: str,
            cloud: Optional[str] = None, cid: Optional[str] = None,
            filter_condition: Optional[Condition] = None
    ):
        # Note: consider using a compound attr as sort-key, i.e. '$cloud#$cid'.
        if cloud:
            cloud = cloud.lower()
        fc = filter_condition
        if cid:
            fc = fc & (CredentialsManager.cloud_identifier == cid)

        rkc = CredentialsManager.cloud == cloud if cloud else None
        return index.query(
            hash_key=hash_key,
            range_key_condition=rkc,
            filter_condition=fc
        )

    @staticmethod
    def save(credentials_manager: CredentialsManager):
        credentials_manager.save()

    @staticmethod
    def create_credentials_configuration(
            configuration_data: dict) -> CredentialsManager:
        credentials_manager = CredentialsManager(**configuration_data)
        return credentials_manager

    @staticmethod
    def credentials_configuration_exists(cloud: str,
                                         cloud_identifier: str) -> bool:
        try:
            CredentialsManager.get(
                hash_key=cloud_identifier,
                range_key=cloud.lower(),
                attributes_to_get=[CredentialsManager.cloud_identifier]
                # no use since we pay the same as when query the whole obj. God bless Dynamodb
            )
            return True
        except DoesNotExist:
            return False

    @staticmethod
    def remove_entity(credentials_manager: CredentialsManager):
        credentials_manager.delete()

    @staticmethod
    def get_credentials_manager_dto(
            credentials_manager: CredentialsManager
    ):
        data = credentials_manager.get_json()
        return {k: v for k, v in data.items() if k in _ATTRS_TO_GET}
