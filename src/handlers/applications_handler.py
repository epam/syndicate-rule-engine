from typing import Optional

from modular_sdk.commons.constants import DEFECT_DOJO_TYPE, CUSTODIAN_TYPE
from modular_sdk.services.impl.maestro_credentials_service import \
    CustodianApplicationMeta, DefectDojoApplicationMeta, \
    DefectDojoApplicationSecret
from pydantic import HttpUrl

from handlers.abstracts.abstract_handler import AbstractHandler
from helpers import build_response, RESPONSE_BAD_REQUEST_CODE, \
    RESPONSE_RESOURCE_NOT_FOUND_CODE, \
    RESPONSE_FORBIDDEN_CODE, RESPONSE_CONFLICT, RESPONSE_NO_CONTENT
from helpers import setdefault
from helpers.constants import CUSTOMER_ATTR, USERNAME_ATTR, \
    CLOUD_ATTR, \
    ACCESS_APPLICATION_ID_ATTR, TENANT_LICENSE_KEY_ATTR, LICENSE_KEY_ATTR, \
    DESCRIPTION_ATTR, PASSWORD_ATTR, APPLICATION_ID_ATTR, \
    CUSTODIAN_LICENSES_TYPE, URL_ATTR, AUTO_RESOLVE_ACCESS_ATTR, \
    RESULTS_STORAGE_ATTR, API_KEY_ATTR
from helpers.constants import POST_METHOD, GET_METHOD, DELETE_METHOD, \
    CLOUD_TO_APP_TYPE, PATCH_METHOD
from helpers.log_helper import get_logger
from models.modular.application import Application, \
    CustodianLicensesApplicationMeta
from services import SERVICE_PROVIDER
from services.clients.lambda_func import LambdaClient, \
    LICENSE_UPDATER_LAMBDA_NAME
from services.clients.modular import ModularClient
from services.environment_service import EnvironmentService
from services.license_manager_service import LicenseManagerService
from services.license_service import LicenseService
from services.modular_service import ModularService
from services.rbac.iam_cache_service import CachedIamService
from services.user_service import CognitoUserService

_LOG = get_logger(__name__)


class ApplicationsHandler(AbstractHandler):
    def __init__(self, modular_service: ModularService,
                 modular_client: ModularClient,
                 user_service: CognitoUserService,
                 cached_iam_service: CachedIamService,
                 environment_service: EnvironmentService,
                 license_manager_service: LicenseManagerService,
                 license_service: LicenseService,
                 lambda_client: LambdaClient):
        self._modular_service = modular_service
        self._modular_client = modular_client
        self._user_service = user_service
        self._iam_service = cached_iam_service
        self._environment_service = environment_service
        self._license_manager_service = license_manager_service
        self._license_service = license_service
        self._lambda_client = lambda_client

    @classmethod
    def build(cls) -> 'ApplicationsHandler':
        return cls(
            modular_service=SERVICE_PROVIDER.modular_service(),
            modular_client=SERVICE_PROVIDER.modular_client(),
            user_service=SERVICE_PROVIDER.user_service(),
            cached_iam_service=SERVICE_PROVIDER.iam_cache_service(),
            environment_service=SERVICE_PROVIDER.environment_service(),
            license_manager_service=SERVICE_PROVIDER.license_manager_service(),
            license_service=SERVICE_PROVIDER.license_service(),
            lambda_client=SERVICE_PROVIDER.lambda_func()
        )

    def define_action_mapping(self) -> dict:
        return {
            '/applications/dojo': {
                POST_METHOD: self.dojo_post,
                GET_METHOD: self.dojo_list,
            },
            '/applications/dojo/{application_id}': {
                PATCH_METHOD: self.dojo_patch,
                DELETE_METHOD: self.dojo_delete,
                GET_METHOD: self.dojo_get
            },
            '/applications/access': {
                POST_METHOD: self.access_post,
                GET_METHOD: self.access_list,
            },
            '/applications/access/{application_id}': {
                PATCH_METHOD: self.access_patch,
                DELETE_METHOD: self.access_delete,
                GET_METHOD: self.access_get
            },
            '/applications': {
                POST_METHOD: self.post,
                GET_METHOD: self.list,
            },
            '/applications/{application_id}': {
                PATCH_METHOD: self.patch,
                DELETE_METHOD: self.delete,
                GET_METHOD: self.get
            }
        }

    def set_dojo_api_key(self, application: Application, api_key: str):
        """
        Modifies the incoming application with secret
        :param application:
        :param api_key:
        """
        assume_role_ssm = self._modular_client.assume_role_ssm_service()
        secret_name = application.secret
        if not secret_name:
            secret_name = assume_role_ssm.safe_name(
                name=application.customer_id,
                prefix='m3.custodian.dojo',
            )
        _LOG.debug('Saving dojo api key to SSM')
        secret = assume_role_ssm.put_parameter(
            name=secret_name,
            value=DefectDojoApplicationSecret(api_key=api_key).dict()
        )
        if not secret:
            _LOG.warning('Something went wrong trying to save api key '
                         'to ssm. Keeping application.secret empty')
        else:
            _LOG.debug('Dojo api key was saved to SSM')
        application.secret = secret

    def dojo_post(self, event: dict) -> dict:
        customer: str = event[CUSTOMER_ATTR]
        description = event.get(DESCRIPTION_ATTR)
        url: Optional[HttpUrl] = event.get(URL_ATTR)
        api_key: str = event.get(API_KEY_ATTR)

        meta = DefectDojoApplicationMeta.from_dict({})
        meta.update_host(
            host=url.host,
            port=int(url.port) if url.port else None,
            protocol=url.scheme,
            stage=url.path
        )
        application = self._modular_service.create_application(
            customer=customer,
            description=description,
            _type=DEFECT_DOJO_TYPE,
            meta=meta.dict(),
        )
        self.set_dojo_api_key(application, api_key)
        self._modular_service.save(application)
        return build_response(content=self._modular_service.get_dto(application))

    def dojo_list(self, event: dict) -> dict:
        res = self._modular_service.get_applications(
            customer=event.get(CUSTOMER_ATTR),
            _type=DEFECT_DOJO_TYPE
        )
        return build_response(
            content=(self._modular_service.get_dto(app) for app in res)
        )

    def dojo_get(self, event: dict) -> dict:
        customer = event.get(CUSTOMER_ATTR)
        application_id = event.get(APPLICATION_ID_ATTR)
        item = self.get_application(application_id, customer, DEFECT_DOJO_TYPE)
        if not item:
            return build_response(content=[])
        return build_response(content=[self._modular_service.get_dto(item)])

    def dojo_patch(self, event: dict) -> dict:
        customer: str = event[CUSTOMER_ATTR]
        application_id = event.get(APPLICATION_ID_ATTR)
        description = event.get(DESCRIPTION_ATTR)
        url: Optional[HttpUrl] = event.get(URL_ATTR)
        api_key: str = event.get(API_KEY_ATTR)

        application = self.get_application(application_id, customer,
                                           DEFECT_DOJO_TYPE)

        if not application:
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content=f'Defect Dojo application with id {application_id} '
                        f'not found in customer {customer}'
            )
        if description:
            application.description = description
        meta = DefectDojoApplicationMeta.from_dict(application.meta.as_dict())
        if url:
            meta.update_host(
                host=url.host,
                port=int(url.port) if url.port else None,
                protocol=url.scheme,
                stage=url.path
            )
            application.meta = meta.dict()
        if api_key:
            self.set_dojo_api_key(application, api_key)
        self._modular_service.save(application)
        return build_response(content=self._modular_service.get_dto(application))

    def dojo_delete(self, event: dict) -> dict:
        customer = event.get(CUSTOMER_ATTR)
        application_id = event.get(APPLICATION_ID_ATTR)
        application = self.get_application(application_id,
                                           customer, DEFECT_DOJO_TYPE)
        if not application:
            return build_response(
                code=RESPONSE_NO_CONTENT,
            )

        erased = self._modular_service.delete(application)
        if not erased:
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE,
                content='Could not remove the application. '
                        'Probably it\'s used by some parents.'
            )
        # erased
        if application.secret:
            _LOG.info(f'Removing application secret: {application.secret}')
            assume_role_ssm = self._modular_client.assume_role_ssm_service()
            if not assume_role_ssm.delete_parameter(application.secret):
                _LOG.warning(f'Could not remove secret: {application.secret}')
        # modular sdk does not remove the app, just sets is_deleted
        self._modular_service.save(application)
        return build_response(code=RESPONSE_NO_CONTENT)

    def access_post(self, event: dict) -> dict:
        customer: str = event[CUSTOMER_ATTR]
        description = event.get(DESCRIPTION_ATTR)
        username: str = event.get(USERNAME_ATTR)
        password: str = event.get(PASSWORD_ATTR)
        url: Optional[HttpUrl] = event.get(URL_ATTR)
        auto_resolve_access: bool = event.get(AUTO_RESOLVE_ACCESS_ATTR)
        results_storage: str = event.get(RESULTS_STORAGE_ATTR)

        existing = next(self._modular_service.get_applications(
            customer=customer,
            _type=CUSTODIAN_TYPE,
            limit=1,
            deleted=False
        ), None)
        if existing:
            return build_response(
                code=RESPONSE_CONFLICT,
                content=f'Access application already '
                        f'exists in customer {customer}'
            )
        application = self._modular_service.create_application(
            customer=customer,
            _type=CUSTODIAN_TYPE,
        )

        if username:
            self.validate_username(username, customer)
        meta = CustodianApplicationMeta.from_dict({})
        if auto_resolve_access:
            meta.update_host(
                host=self._environment_service.api_gateway_host(),
                stage=self._environment_service.api_gateway_stage()
            )
        else:  # url
            meta.update_host(
                host=url.host,
                port=int(url.port) if url.port else None,
                protocol=url.scheme,
                stage=url.path
            )
        if username:  # means password is given as well
            meta.username = username
            self.set_user_password(application, password)

        if results_storage:
            meta.results_storage = results_storage
        application.description = description

        application.meta = meta.dict()
        _LOG.info('Saving application item')
        self._modular_service.save(application)

        return build_response(
            content=self._modular_service.get_dto(application)
        )

    def access_list(self, event: dict) -> dict:
        res = self._modular_service.get_applications(
            customer=event.get(CUSTOMER_ATTR),
            _type=CUSTODIAN_TYPE
        )
        return build_response(
            content=(self._modular_service.get_dto(app) for app in res)
        )

    def access_patch(self, event: dict) -> dict:
        application_id = event.get(APPLICATION_ID_ATTR)
        customer: str = event[CUSTOMER_ATTR]
        description = event.get(DESCRIPTION_ATTR)
        username: str = event.get(USERNAME_ATTR)
        password: str = event.get(PASSWORD_ATTR)
        url: Optional[HttpUrl] = event.get(URL_ATTR)
        auto_resolve_access: bool = event.get(AUTO_RESOLVE_ACCESS_ATTR)
        results_storage: str = event.get(RESULTS_STORAGE_ATTR)

        application = self.get_application(application_id, customer,
                                           CUSTODIAN_TYPE)

        if not application:
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content=f'Custodian access application not found in customer '
                        f'{customer}'
            )
        if username:
            self.validate_username(username, customer)
        meta = CustodianApplicationMeta.from_dict(application.meta.as_dict())
        if url:
            meta.update_host(
                host=url.host,
                port=int(url.port) if url.port else None,
                protocol=url.scheme,
                stage=url.path
            )
        if auto_resolve_access:
            meta.update_host(
                host=self._environment_service.api_gateway_host(),
                stage=self._environment_service.api_gateway_stage()
            )
        if username:
            meta.username = username
            self.set_user_password(application, password)
        if description:
            application.description = description
        if results_storage:
            meta.results_storage = results_storage

        application.meta = meta.dict()
        _LOG.info('Saving application item')
        self._modular_service.save(application)

        return build_response(
            content=self._modular_service.get_dto(application)
        )

    def access_delete(self, event: dict) -> dict:
        customer = event.get(CUSTOMER_ATTR)
        application_id = event.get(APPLICATION_ID_ATTR)
        application = self.get_application(application_id,
                                           customer, CUSTODIAN_TYPE)
        if not application:
            return build_response(
                code=RESPONSE_NO_CONTENT,
            )

        erased = self._modular_service.delete(application)
        if not erased:
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE,
                content='Could not remove the application. '
                        'Probably it\'s used by some parents.'
            )
        # erased
        if application.secret:
            _LOG.info(f'Removing application secret: {application.secret}')
            assume_role_ssm = self._modular_client.assume_role_ssm_service()
            if not assume_role_ssm.delete_parameter(application.secret):
                _LOG.warning(f'Could not remove secret: {application.secret}')
        # modular sdk does not remove the app, just sets is_deleted
        self._modular_service.save(application)
        return build_response(code=RESPONSE_NO_CONTENT)

    def access_get(self, event: dict) -> dict:
        customer = event.get(CUSTOMER_ATTR)
        application_id = event.get(APPLICATION_ID_ATTR)
        item = self.get_application(application_id, customer, CUSTODIAN_TYPE)
        if not item:
            return build_response(content=[])
        return build_response(content=[self._modular_service.get_dto(item)])

    def get_application(self, application_id: str, customer: str, _type: str
                        ) -> Optional[Application]:
        application = self._modular_service.get_application(application_id)
        if not application or application.is_deleted or \
                application.customer_id != customer or \
                application.type != _type:
            return
        return application

    def list(self, event: dict) -> dict:
        res = self._modular_service.get_applications(
            customer=event.get(CUSTOMER_ATTR),
            _type=CUSTODIAN_LICENSES_TYPE
        )
        return build_response(
            content=(self._modular_service.get_dto(app) for app in res)
        )

    def get(self, event: dict) -> dict:
        customer = event.get(CUSTOMER_ATTR)
        application_id = event.get(APPLICATION_ID_ATTR)
        item = self.get_application(application_id, customer,
                                    CUSTODIAN_LICENSES_TYPE)
        if not item:
            return build_response(content=[])
        return build_response(content=[self._modular_service.get_dto(item)])

    def validate_username(self, username: str, customer: str):
        """
        May raise CustodianException
        :param customer:
        :param username:
        :return:
        """
        _exists = self._user_service.is_user_exists(username)
        if not _exists:
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content=f'User {username} not found in customer: {customer}'
            )
        _customer_match = \
            self._user_service.get_user_customer(username) == customer

        # two identical responses but there will be an error in
        # get_user_customer(), if user does not exist
        if not _customer_match:
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content=f'User {username} not found in customer: {customer}'
            )

    def post(self, event: dict) -> dict:
        customer: str = event[CUSTOMER_ATTR]
        description = event.get(DESCRIPTION_ATTR)
        cloud: str = event.get(CLOUD_ATTR)
        access_application_id = event.get(ACCESS_APPLICATION_ID_ATTR)
        tenant_license_key = event.get(TENANT_LICENSE_KEY_ATTR)
        application = self._modular_service.create_application(
            customer=customer,
            _type=CUSTODIAN_LICENSES_TYPE,
        )

        meta = CustodianLicensesApplicationMeta()
        application.description = description

        if cloud:  # either "AWS" or "AZURE" or "GOOGLE", validated by pydentic
            # either access_application_id or tenant_license_key or both
            if access_application_id:
                self.set_access_application_id(
                    meta=meta,
                    cloud=cloud,
                    access_application_id=access_application_id,
                    customer=customer
                )
            if tenant_license_key:
                license_key = self.activate_license(
                    tenant_license_key=tenant_license_key,
                    customer=customer
                )
                # If activate_customer has succeeded, license_sync must
                # be successful as well, I hope..
                self._execute_license_sync([license_key])
                # existing_license_key = meta.license_key(cloud)
                # if existing_license_key and existing_license_key \
                #         not in meta.cloud_to_license_key().values():
                #     self._license_service.remove_for_customer(
                #         existing_license_key, customer
                #     )
                meta.update_license_key(cloud, license_key)

        application.meta = meta.dict()
        _LOG.info('Saving application item')
        self._modular_service.save(application)

        return build_response(
            content=self._modular_service.get_dto(application)
        )

    def patch(self, event: dict) -> dict:
        application_id = event.get(APPLICATION_ID_ATTR)
        customer: str = event[CUSTOMER_ATTR]
        description = event.get(DESCRIPTION_ATTR)
        cloud: str = event.get(CLOUD_ATTR)
        access_application_id = event.get(ACCESS_APPLICATION_ID_ATTR)
        tenant_license_key = event.get(TENANT_LICENSE_KEY_ATTR)
        application = self.get_application(application_id, customer,
                                           CUSTODIAN_LICENSES_TYPE)
        if not application:
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content=
                f'Custodian application not found in customer {customer}'
            )
        meta = CustodianLicensesApplicationMeta(**application.meta.as_dict())
        if description:
            application.description = description
        if cloud:  # either "AWS" or "AZURE" or "GOOGLE", validated by pydentic
            # either access_application_id or tenant_license_key or both
            if access_application_id:
                self.set_access_application_id(
                    meta=meta,
                    cloud=cloud,
                    access_application_id=access_application_id,
                    customer=customer
                )
            if tenant_license_key:
                license_key = self.activate_license(
                    tenant_license_key=tenant_license_key,
                    customer=customer
                )
                # If activate_customer has succeeded, license_sync must
                # be successful as well, I hope..
                self._execute_license_sync([license_key])
                existing_license_key = meta.license_key(cloud)
                if existing_license_key and existing_license_key \
                        not in meta.cloud_to_license_key().values():
                    self._license_service.remove_for_customer(
                        existing_license_key, customer
                    )
                meta.update_license_key(cloud, license_key)

        application.meta = meta.dict()
        _LOG.info('Saving application item')
        self._modular_service.save(application)

        return build_response(
            content=self._modular_service.get_dto(application)
        )

    def delete(self, event: dict) -> dict:
        customer = event.get(CUSTOMER_ATTR)
        application_id = event.get(APPLICATION_ID_ATTR)
        application = self.get_application(application_id, customer,
                                           CUSTODIAN_LICENSES_TYPE)
        if not application:
            return build_response(
                code=RESPONSE_NO_CONTENT,
            )

        erased = self._modular_service.delete(application)
        if not erased:
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE,
                content='Could not remove the application. '
                        'Probably it\'s used by some parents.'
            )
        # erased
        if application.secret:  # should not have, but still
            _LOG.info(f'Removing application secret: {application.secret}')
            assume_role_ssm = self._modular_client.assume_role_ssm_service()
            if not assume_role_ssm.delete_parameter(application.secret):
                _LOG.warning(f'Could not remove secret: {application.secret}')
        meta = CustodianLicensesApplicationMeta(
            **application.meta.as_dict()
        )
        for cloud, license_key in meta.cloud_to_license_key().items():
            if not license_key:
                continue
            self._license_service.remove_for_customer(
                license_key, customer
            )
            meta.update_license_key(cloud, None)
        # modular sdk does not remove the app, just sets is_deleted
        application.meta = meta.dict()
        self._modular_service.save(application)
        return build_response(code=RESPONSE_NO_CONTENT)

    def set_user_password(self, application: Application, password: str):
        """
        Modifies the incoming application with secret
        :param application:
        :param password:
        """
        assume_role_ssm = self._modular_client.assume_role_ssm_service()
        secret_name = assume_role_ssm.safe_name(
            name=application.customer_id,
            prefix='m3.custodian.application',
            date=False
        )
        _LOG.debug('Saving password to SSM')
        secret = assume_role_ssm.put_parameter(
            name=secret_name,
            value=password
        )
        if not secret:
            _LOG.warning('Something went wrong trying to same password '
                         'to ssm. Keeping application.secret empty')
        _LOG.debug('Password was saved to SSM')
        application.secret = secret

    def set_access_application_id(self, meta: CustodianLicensesApplicationMeta,
                                  cloud: str,
                                  access_application_id: str,
                                  customer: str):
        """
        Just keep repeating logic in a separate block
        :param meta:
        :param cloud:
        :param access_application_id:
        :param customer:
        :return:
        """
        _access_app = self._modular_service.get_application(
            access_application_id)
        if not _access_app or _access_app.customer_id != customer:
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content=f'Application {access_application_id}'
                        f' not found within {customer}'
            )
        if _access_app.type not in CLOUD_TO_APP_TYPE.get(cloud, set()):
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE,
                content=f'Application \'{access_application_id}\' '
                        f'has type {_access_app.type} that is not '
                        f'supported for cloud {cloud}'
            )
        _LOG.info(f'Updating access application id for cloud: {cloud}')
        meta.update_access_application_id(cloud, access_application_id)

    def activate_license(self, tenant_license_key: str, customer: str) -> str:
        _response = self._license_manager_service.activate_customer(
            customer, tenant_license_key
        )
        if not _response:
            _message = f'License manager does not allow to activate ' \
                       f'tenant license \'{tenant_license_key}\'' \
                       f' for customer \'{customer}\''
            _LOG.warning(_message)
            return build_response(code=RESPONSE_FORBIDDEN_CODE,
                                  content=_message)
        license_key = _response.get(LICENSE_KEY_ATTR)
        license_obj = self._license_service.get_license(license_key)
        if not license_obj:
            _LOG.info(f'License object with id \'{license_key}\' does '
                      f'not exist yet. Creating.')
            license_obj = self._license_service.create({
                LICENSE_KEY_ATTR: license_key})
        _d = setdefault(license_obj.customers, customer, {})
        _d[TENANT_LICENSE_KEY_ATTR] = tenant_license_key
        _LOG.info('Going to save license object')
        license_obj.save()
        return license_key

    def _execute_license_sync(self, license_keys: list):
        """
        Returns a response from an asynchronously invoked
        sync-concerned lambda, `license-updater`.
        :return:Dict[code=202]
        """
        _LOG.info('Invoking license updater lambda')
        response = self._lambda_client.invoke_function_async(
            LICENSE_UPDATER_LAMBDA_NAME, event={
                LICENSE_KEY_ATTR: license_keys
            }
        )
        _LOG.info(f'License updater lambda was invoked: {response}')
