from handlers.abstracts.abstract_credentials_manager_handler import \
    AbstractCredentialsManagerHandler
from helpers.constants import GET_METHOD, POST_METHOD, PATCH_METHOD, \
    DELETE_METHOD, TRUSTED_ROLE_ARN, \
    CLOUD_IDENTIFIER_ATTR, CREDENTIALS_MANAGER_ACTION, CLOUD_ATTR, ENABLED, \
    TENANT_ATTR, CUSTOMER_ATTR
from helpers.log_helper import get_logger
from models.credentials_manager import CredentialsManager
from services.credentials_manager_service import CredentialsManagerService
from services.user_service import CognitoUserService
from typing import Optional, List
from services.modular_service import ModularService

_LOG = get_logger(__name__)


class CredentialsManagerHandler(AbstractCredentialsManagerHandler):
    """
    Manage Credentials Manager API
    """

    def __init__(self, credential_manager_service: CredentialsManagerService,
                 user_service: CognitoUserService,
                 modular_service: ModularService):
        super().__init__(
            credential_manager_service=credential_manager_service,
            user_service=user_service,
            modular_service=modular_service
        )

    def define_action_mapping(self):
        return {
            '/accounts/credential_manager': {
                GET_METHOD: self.get_credentials_manager,
                POST_METHOD: self.add_credentials_manager,
                PATCH_METHOD: self.update_credentials_manager,
                DELETE_METHOD: self.remove_credentials_manager
            }
        }

    def get_configuration_object(
        self, cloud: str, cloud_identifier: str,
        customer: Optional[str] = None, tenants: Optional[List[str]] = None
    ):
        head = f'CredentialsManager:{cloud}#{cloud_identifier}'

        entity = self.credential_manager_service.get_credentials_configuration(
            cloud=cloud,
            cloud_identifier=cloud_identifier
        )

        if not entity:
            _LOG.warning(head + ' does not exist.')

        elif customer and entity.customer != customer:
            _customer = entity.customer
            _LOG.warning(head + f' customer restriction - '
                                f'\'{customer}\' != \'{_customer}\'')
            entity = None

        elif tenants and entity.tenant not in tenants:
            _tenant = entity.tenant
            _tenants = ', '.join(map("'{}'".format, tenants))
            _LOG.warning(head + f' tenant {_tenant} must be reflected '
                                f'within the granted ones - {_tenants}.')
            entity = None

        return entity

    def configuration_object_exist(self, cloud, cloud_identifier):
        return self.credential_manager_service.credentials_configuration_exists(
            cloud=cloud,
            cloud_identifier=cloud_identifier)

    def create_configuration_object(self, configuration_data):
        return self.credential_manager_service.create_credentials_configuration(
            configuration_data=configuration_data)

    def save_configuration_object(self,
                                  credentials_manager: CredentialsManager):
        return self.credential_manager_service.save(
            credentials_manager=credentials_manager)

    def delete_configuration_object(self, entity: CredentialsManager):
        return self.credential_manager_service.remove_entity(entity)

    def get_configuration_object_dto(self, entity):
        return self.credential_manager_service.\
            get_credentials_manager_dto(entity)

    @property
    def entity_name(self):
        return CREDENTIALS_MANAGER_ACTION

    @property
    def hash_key_attr_name(self):
        return CLOUD_IDENTIFIER_ATTR

    @property
    def range_key_attr_name(self):
        return CLOUD_ATTR

    @property
    def default_params_mapping(self):
        return {
            ENABLED: True
        }

    @property
    def required_params_list(self):
        return [
            CLOUD_IDENTIFIER_ATTR,
            TRUSTED_ROLE_ARN,
            CLOUD_ATTR,

            # Account-based payload
            TENANT_ATTR,
            CUSTOMER_ATTR
        ]

    @property
    def full_params_list(self):
        return [
            # Request-based payload
            CLOUD_ATTR,
            CLOUD_IDENTIFIER_ATTR,
            TRUSTED_ROLE_ARN,
            ENABLED,

            # Account-based payload
            TENANT_ATTR,
            CUSTOMER_ATTR
        ]

    @property
    def update_params_list(self):
        return [
            TRUSTED_ROLE_ARN,
            ENABLED
        ]

    @property
    def param_type_mapping(self):
        """Validates Request-based payload."""
        # Obsolete, as of 3.2.0
        return {
            CLOUD_ATTR: str,
            CLOUD_IDENTIFIER_ATTR: str,
            TRUSTED_ROLE_ARN: str,
            ENABLED: bool
        }

    def get_credentials_manager(self, event):
        return self._basic_get_handler(
            event=event)

    def add_credentials_manager(self, event):
        return self._basic_create_handler(
            event=event)

    def update_credentials_manager(self, event):
        return self._basic_update_handler(
            event=event)

    def remove_credentials_manager(self, event):
        return self._basic_delete_handler(
            event=event)

    @staticmethod
    def is_accessible(
        granted_customer: str, granted_tenants: List[str],
        target_customer: str, target_tenant: str
    ):
        """
        Predicates whether a subject customer-tenant scope is accessible,
        based on the granted customer and tenants.
        :param granted_customer: str
        :param granted_tenants: List[str]
        :param target_tenant: str
        :param target_customer: str
        :return: bool
        """

        grant_access = True

        if granted_customer and target_customer != granted_customer:
            _LOG.warning(
                f'Customer restriction - '
                f'granted \'{granted_customer}\' customer must reflect'
                f' \'{target_customer}\'.'
            )
            grant_access = False

        elif granted_tenants and target_tenant not in granted_tenants:
            _tenants = ', '.join(map("'{}'".format, granted_tenants))
            _LOG.warning(
                'Tenant restriction - '
                f'tenant {target_tenant} must be reflected '
                f'within the granted ones - {_tenants}.'
            )
            grant_access = False

        return grant_access
