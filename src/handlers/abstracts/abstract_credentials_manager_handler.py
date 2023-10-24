from abc import abstractmethod
from http import HTTPStatus
from typing import Optional, List

from botocore.exceptions import ClientError

from handlers.abstracts.abstract_handler import AbstractHandler
from helpers import build_response
from helpers.constants import CLOUD_IDENTIFIER_ATTR, CLOUD_ATTR, \
    TRUSTED_ROLE_ARN, TENANTS_ATTR, CUSTOMER_ATTR, TENANT_ATTR
from helpers.log_helper import get_logger
from services import SERVICE_PROVIDER
from services.credentials_manager_service import CredentialsManagerService
from services.modular_service import ModularService
from services.user_service import CognitoUserService

CREDENTIALS = 'Credentials'

_LOG = get_logger(__name__)


class AbstractCredentialsManagerHandler(AbstractHandler):

    def __init__(self, credential_manager_service: CredentialsManagerService,
                 user_service: CognitoUserService,
                 modular_service: ModularService):
        self.credential_manager_service = credential_manager_service
        self.user_service = user_service
        self.modular_service = modular_service
        self.sts_client = SERVICE_PROVIDER.sts_client()

    @abstractmethod
    def get_configuration_object(
        self, cloud: str, cloud_identifier: str,
        customer: Optional[str] = None, tenants: Optional[List[str]] = None
    ):
        """
        Restricts access to a derived CredentialsManager entity,
        adhering either to the customer or tenant scope.
        :param cloud: str, cloud value of the configuration
        :param cloud_identifier: str, cid valid of the configuration
        :param customer: Optional[str]
        :param tenants: Optional[List[str]]
        :return: Optional[CredentialsManager]
        """
        raise NotImplementedError

    @staticmethod
    @abstractmethod
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


    @abstractmethod
    def configuration_object_exist(self, *args, **kwargs):
        raise NotImplementedError()

    @abstractmethod
    def create_configuration_object(self, *args, **kwargs):
        raise NotImplementedError()

    @abstractmethod
    def save_configuration_object(self, *args, **kwargs):
        raise NotImplementedError()

    @abstractmethod
    def delete_configuration_object(self, *args, **kwargs):
        raise NotImplementedError()

    @property
    @abstractmethod
    def entity_name(self):
        raise NotImplementedError()

    @property
    @abstractmethod
    def hash_key_attr_name(self):
        raise NotImplementedError()

    @property
    @abstractmethod
    def range_key_attr_name(self):
        raise NotImplementedError()

    @abstractmethod
    def get_configuration_object_dto(self, *args, **kwargs):
        raise NotImplementedError()

    @property
    @abstractmethod
    def default_params_mapping(self):
        raise NotImplementedError()

    @property
    @abstractmethod
    def required_params_list(self):
        raise NotImplementedError()

    @property
    @abstractmethod
    def full_params_list(self):
        raise NotImplementedError()

    @property
    @abstractmethod
    def update_params_list(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def param_type_mapping(self):
        raise NotImplementedError()

    def _basic_get_handler(self, event: dict):
        _LOG.debug(f'Event: {event}')

        _LOG.debug(f'Get {self.entity_name} action')

        cloud_identifier = event.get(self.hash_key_attr_name)
        cloud = event.get(self.range_key_attr_name)

        # AbstractHandler input.
        customer = event.get(CUSTOMER_ATTR)

        # RestrictionService input.
        tenants = event.get(TENANTS_ATTR)

        entities = []
        _log = f'Fetching entities by fields: cloud={cloud}, '\
               f'cloud_identifier={cloud_identifier}'

        if customer:
            _log += f'. Obscuring the output view by \'{customer}\' customer'
        if tenants:
            _tenants = ','.join(map("'{}'".format, tenants))
            _log += f'. Restricting by {_tenants} tenant(s) scope'

        _LOG.debug(f'{_log}.')

        if cloud and cloud_identifier:
            entity = self.get_configuration_object(
                cloud=cloud, cloud_identifier=cloud_identifier,
                customer=customer, tenants=tenants
            )
            if entity:
                entities.append(entity)

        else:
            entities = self.credential_manager_service.inquire(
                cloud=cloud,
                cloud_identifier=cloud_identifier,
                customer=customer, tenants=tenants
            )

        return build_response(
            code=HTTPStatus.OK,
            content=(
                self.get_configuration_object_dto(entity)
                for entity in entities
            )
        )

    def _basic_create_handler(self, event: dict):
        """
        When create credentials manager need to specify next required fields:
        :cloud_identifier
        :trusted_role_arn
        :cloud
        (required_params_list)

        Status of 'enabled' will be set to True automatically (check
        default_params_mapping)

        :tenant
        :customer
        Are obtained from a derived account entity.
        """

        _LOG.debug(f'Creating {self.entity_name}: {event}')

        event = self.validations(event,  # todo refactor this validations
                                 check_on_existence=True,
                                 reverse_existence=False,
                                 if_cloud_aws_validate_trusted_role=True,
                                 validate_account_with_specified_id=True,
                                 assume_role=True)

        ready_for_save_data = self._replace_dict_params(
            event=event,
            full_params_list=self.full_params_list,
            default_params_mapping=self.default_params_mapping
        )

        _LOG.debug(f'{self.entity_name} data: {ready_for_save_data}')

        result = self._save_entity(
            entity_data=ready_for_save_data,
            entity_name=self.entity_name,
            create_func=self.create_configuration_object,
            save_func=self.save_configuration_object,
            get_dto_func=self.get_configuration_object_dto
        )

        return build_response(code=HTTPStatus.CREATED, content=result)

    def _basic_update_handler(self, event: dict):
        """
        1. Checks user (permissions)
        2. Validate data from event
        3. Get element from DB to update
        4. Change field to new in _replace_obj_params()
        """

        _LOG.debug(f'Update {self.entity_name} action')

        # AbstractHandler input.
        customer = event.get(CUSTOMER_ATTR)

        # RestrictionService input.
        tenants = event.get(TENANTS_ATTR)

        _LOG.debug('Validate event param types')

        cloud_identifier = event.get(self.hash_key_attr_name)
        cloud = event.get(self.range_key_attr_name)
        role_arn = event.get(TRUSTED_ROLE_ARN)
        if role_arn:
            self.try_to_assume_role(role_arn)

        _LOG.debug(f'{self.entity_name} to update: '
                   f'{cloud} and {cloud_identifier}')

        entity = self.get_configuration_object(
            cloud=cloud.lower(), cloud_identifier=cloud_identifier,
            customer=customer, tenants=tenants
        )

        if not entity:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content=f'Credentials configuration for {cloud} cloud and '
                        f'{cloud_identifier} does not exist.'
            )

        entity = self._replace_obj_params(
            event=event,
            entity=entity,
            full_params_list=self.update_params_list,
            entity_name=self.entity_name,
            save_func=self.save_configuration_object
        )

        return build_response(
            code=HTTPStatus.OK,
            content=self.get_configuration_object_dto(entity)
        )

    def _basic_delete_handler(self, event: dict):
        """
        1. Checks user (permissions)
        2. Validate data from event
        3. Get entity from DB
        4. Delete entity
        """

        _LOG.debug(f'Delete {self.entity_name} action')

        # AbstractHandler input.
        customer = event.get(CUSTOMER_ATTR)

        # RestrictionService input.
        tenants = event.get(TENANTS_ATTR)

        cloud_identifier = event.get(self.hash_key_attr_name)
        cloud = event.get(self.range_key_attr_name)

        entity = self.get_configuration_object(
            cloud=cloud.lower(), cloud_identifier=cloud_identifier,
            customer=customer, tenants=tenants
        )

        if not entity:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content=f'Credentials configuration for {cloud} cloud and '
                        f'{cloud_identifier} does not exist.'
            )

        self.delete_configuration_object(entity)

        return build_response(
            code=HTTPStatus.OK,
            content=f'Credentials configuration for {cloud} cloud and '
                    f'{cloud_identifier} cloud identifier has been deleted')

    def validations(self,
                    event,
                    check_on_existence=False,
                    reverse_existence=False,
                    assume_role=False,
                    validate_account_with_specified_id=False,
                    if_cloud_aws_validate_trusted_role=False):
        """
        This method check validation on next:
        1. Correct cloud attribute in event
        2. req_params_list - check that all required params are in event
        3. check_on_existence - check if object in DB -> if yes -> error
        4. reverse_existence - if not in DB -> error
        5. Check if validate_account_with_specified_id exists in CaasAccount table
            * Attaching `tenant`, `customer` attributes to the event variable.
        6. try to assume role
        7. if cloud - aws, validate trusted role in event
        """

        cloud = event.get(CLOUD_ATTR)
        if cloud:
            event[CLOUD_ATTR] = cloud.lower()

        if check_on_existence:
            if self.check_on_existence(event):
                debug_message = f"{self.entity_name.capitalize()} with " \
                                f"cloud '{event.get(CLOUD_ATTR)}', and " \
                                f"cloud identifier" \
                                f" '{event.get(CLOUD_IDENTIFIER_ATTR)}' " \
                                f"already exists"

                _LOG.warning(debug_message)
                return build_response(
                    code=HTTPStatus.CONFLICT,
                    content=debug_message)

        if reverse_existence:
            if not self.check_on_existence(event):
                debug_message = f'{self.entity_name.capitalize()} with next ' \
                                f'fields: {event.get(CLOUD_ATTR)}' \
                                f'{event.get(CLOUD_IDENTIFIER_ATTR)} ' \
                                f'does not exist'

                _LOG.warning(debug_message)
                return build_response(
                    code=HTTPStatus.NOT_FOUND,
                    content=debug_message)

        if validate_account_with_specified_id:
            cloud = event.get(CLOUD_ATTR)

            if event.get(CLOUD_IDENTIFIER_ATTR):
                cloud_identifier = event.get(CLOUD_IDENTIFIER_ATTR)
                tenant = next(
                    self.modular_service.i_get_tenants_by_acc(cloud_identifier),
                    None
                )

                persistence_issue = \
                    f"Tenant:\'{cloud_identifier}\' within " \
                    f"\'{cloud.upper()}\' " \
                    f"cloud does not exist."

                if not tenant:
                    return build_response(
                        code=HTTPStatus.NOT_FOUND,
                        content=persistence_issue
                    )

                if cloud and tenant.cloud.upper() != cloud.upper():
                    message = f"Account:\'{cloud_identifier}\'" \
                              f" does not exist within \'{cloud.upper()}\'" \
                              f" cloud."
                    return build_response(
                        code=HTTPStatus.NOT_FOUND,
                        content=message
                    )

                if not self.is_accessible(
                    granted_customer=event.get(CUSTOMER_ATTR),
                    granted_tenants=event.get(TENANTS_ATTR),
                    target_customer=tenant.customer_name,
                    target_tenant=tenant.name
                ):
                    # Generic, non-leaking response.
                    return build_response(
                        code=HTTPStatus.NOT_FOUND,
                        content=persistence_issue
                    )

                # Retain customer and tenant attributes, for instantiation.
                event[TENANT_ATTR] = tenant.name
                event[CUSTOMER_ATTR] = tenant.customer_name

        if if_cloud_aws_validate_trusted_role:
            if event.get(CLOUD_ATTR):
                cloud = event.get(CLOUD_ATTR).lower()
                if cloud == 'aws':
                    if not event.get(TRUSTED_ROLE_ARN):
                        message = f"When specified cloud is 'aws', " \
                                  f"than parameter '{TRUSTED_ROLE_ARN}' should " \
                                  f"be in request"
                        return build_response(
                            code=HTTPStatus.NOT_FOUND,
                            content=message)

        if assume_role:
            self.try_to_assume_role(role_arn=event.get(TRUSTED_ROLE_ARN))

        return event

    def check_on_existence(self, event):
        cloud_identifier = event.get(self.hash_key_attr_name)
        cloud = event.get(self.range_key_attr_name)

        if self.configuration_object_exist(cloud, cloud_identifier):
            return True

        return False

    def try_to_assume_role(self, role_arn):
        try:
            self.sts_client.assume_role(role_arn=role_arn).get(CREDENTIALS)
        except (ClientError, Exception) as e:
            message = f"Can't assume role with specified {TRUSTED_ROLE_ARN} " \
                      f"'{role_arn}'"
            _LOG.warning(f'{message}, due to - {e}')
            return build_response(
                code=HTTPStatus.BAD_REQUEST,
                content=message
            )

