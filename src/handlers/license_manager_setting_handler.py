from http import HTTPStatus

from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    PublicFormat,
    load_pem_private_key,
)

from handlers import AbstractHandler, Mapping
from helpers.constants import (
    ALG_ATTR,
    CustodianEndpoint,
    HTTPMethod,
    KID_ATTR,
    VALUE_ATTR,
)
from helpers.lambda_response import ResponseFactory, build_response
from helpers.log_helper import get_logger
from services import SP
from services.clients.ssm import AbstractSSMClient
from services.license_manager_service import LicenseManagerService
from services.clients.lm_client import LmTokenProducer
from services.setting_service import Setting, SettingsService
from validators.swagger_request_models import (
    BaseModel,
    LicenseManagerClientSettingDeleteModel,
    LicenseManagerClientSettingPostModel,
    LicenseManagerConfigSettingPostModel,
)
from validators.utils import validate_kwargs 

_LOG = get_logger(__name__)

PEM_ATTR = 'PEM'


class LicenseManagerClientHandler(AbstractHandler):
    """
    Manages License Manager Client.
    """

    def __init__(self, settings_service: SettingsService,
                 license_manager_service: LicenseManagerService,
                 ssm_client: AbstractSSMClient):
        self.settings_service = settings_service
        self.license_manager_service = license_manager_service
        self._ssm_client = ssm_client

    @property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.SETTINGS_LICENSE_MANAGER_CLIENT: {
                HTTPMethod.GET: self.get,
                HTTPMethod.POST: self.post,
                HTTPMethod.DELETE: self.delete,
            }
        }

    @classmethod
    def build(cls):
        return cls(
            settings_service=SP.settings_service,
            license_manager_service=SP.license_manager_service,
            ssm_client=SP.ssm
        )

    @validate_kwargs
    def get(self, event: BaseModel):
        configuration: dict = self.settings_service.get_license_manager_client_key_data() or {}
        if not configuration:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                'Configuration is not found'
            ).exc()

        kid = configuration.get(KID_ATTR)
        alg = configuration.get(ALG_ATTR)
        name = LmTokenProducer.derive_client_private_key_id(kid=kid)
        data = self._ssm_client.get_secret_value(name)
        pem = data['value']
        key = load_pem_private_key(pem.encode(), None)
        return build_response(content=self.get_dto(
            kid=kid,
            alg=alg,
            public_key=self.get_public(key)
        ))

    @staticmethod
    def get_public(key) -> bytes:
        return key.public_key().public_bytes(
            Encoding.PEM, PublicFormat.SubjectPublicKeyInfo
        )

    @validate_kwargs
    def post(self, event: LicenseManagerClientSettingPostModel):
        # Validation is taken care of, on the gateway/abstract-handler layer.
        kid = event.key_id
        alg = event.algorithm
        raw_prk: str = event.private_key

        # Decoding is taking care of within the validation layer.

        if self.settings_service. \
                get_license_manager_client_key_data(value=False):
            return build_response(
                code=HTTPStatus.CONFLICT,
                content='License Manager Client-Key already exists.'
            )

        try:
            prk = load_pem_private_key(raw_prk.encode(), None)
        except (ValueError, Exception):
            raise ResponseFactory(HTTPStatus.BAD_REQUEST).message(
                'Invalid private key'
            ).exc()
        name = LmTokenProducer.derive_client_private_key_id(kid=kid)
        self._ssm_client.create_secret(
            secret_name=name,
            secret_value={
                VALUE_ATTR: prk.private_bytes(
                    Encoding.PEM,
                    PrivateFormat.PKCS8,
                    NoEncryption()
                ).decode('utf-8')
            }
        )
        _LOG.info('Private key was saved to SSM')
        setting = self.settings_service.create_license_manager_client_key_data(
            kid=kid, alg=alg
        )

        _LOG.info(f'Persisting License Manager Client-Key data:'
                  f' {setting.value}.')

        self.settings_service.save(setting=setting)

        return build_response(
            code=HTTPStatus.CREATED,
            content=self.get_dto(
                alg=alg,
                kid=kid,
                public_key=self.get_public(prk)
            )
        )

    @staticmethod
    def get_dto(alg: str, kid: str, public_key: str | bytes) -> dict:
        if isinstance(public_key, bytes):
            public_key = public_key.decode()
        return {
            'algorithm': alg,
            'b64_encoded': False,
            'format': 'PEM',
            'key_id': kid,
            'public_key': public_key
        }

    @validate_kwargs
    def delete(self, event: LicenseManagerClientSettingDeleteModel):
        requested_kid = event.key_id

        head = 'License Manager Client-Key'
        unretained = ' does not exist'
        code = HTTPStatus.NOT_FOUND
        # Default 404 error-response.
        content = head + unretained

        setting = self.settings_service.get_license_manager_client_key_data(value=False)

        if not setting:
            return build_response(
                code=code,
                content=head + unretained
            )

        configuration = setting.value
        kid = configuration.get(KID_ATTR)
        alg = configuration.get(ALG_ATTR)

        if not (kid and alg):
            _LOG.warning(head + ' does not contain \'kid\' or \'alg\' data.')
            return build_response(code=code, content=content)

        if kid != requested_kid:
            _LOG.warning(
                head + f' does not contain {requested_kid} \'kid\' data.')
            return build_response(code=code, content=content)

        name = LmTokenProducer.derive_client_private_key_id(kid=kid)
        self._ssm_client.delete_parameter(name)
        self.settings_service.delete(setting=setting)
        return build_response(code=HTTPStatus.NO_CONTENT)


class LicenseManagerConfigHandler(AbstractHandler):
    """
    Manages License Manager access-configuration.
    """

    def __init__(self, settings_service: SettingsService):
        self.settings_service = settings_service

    @property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.SETTINGS_LICENSE_MANAGER_CONFIG: {
                HTTPMethod.GET: self.get,
                HTTPMethod.POST: self.post,
                HTTPMethod.DELETE: self.delete,
            }
        }

    @classmethod
    def build(cls):
        return cls(settings_service=SP.settings_service)

    @validate_kwargs
    def get(self, event: BaseModel):
        configuration: dict = self.settings_service.get_license_manager_access_data()
        if not configuration:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                'Setting not found').exc()
        return build_response(content=configuration)

    @validate_kwargs
    def post(self, event: LicenseManagerConfigSettingPostModel):
        if self.settings_service.get_license_manager_access_data():
            return build_response(
                code=HTTPStatus.CONFLICT,
                content='License Manager config-data already exists.'
            )
        # TODO check access ?
        setting = self.settings_service. \
            create_license_manager_access_data_configuration(
            host=event.host,
            port=event.port,
            protocol=event.protocol,
            stage=event.stage,
        )

        _LOG.info(f'Persisting License Manager config-data: {setting.value}.')
        self.settings_service.save(setting=setting)
        return build_response(code=HTTPStatus.CREATED, content=setting.value)

    @validate_kwargs
    def delete(self, event: BaseModel):
        configuration: Setting = self.settings_service.get_license_manager_access_data(value=False)
        if not configuration:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content='License Manager config-data does not exist.'
            )
        _LOG.info(f'Removing License Manager config-data:'
                  f' {configuration.value}.')
        self.settings_service.delete(setting=configuration)
        return build_response(code=HTTPStatus.NO_CONTENT)
