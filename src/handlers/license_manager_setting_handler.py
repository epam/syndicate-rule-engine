from handlers.abstracts.abstract_handler import AbstractHandler
from helpers import build_response, RESPONSE_OK_CODE, RESPONSE_CONFLICT, \
    RESPONSE_RESOURCE_NOT_FOUND_CODE, RESPONSE_INTERNAL_SERVER_ERROR, \
    RESPONSE_BAD_REQUEST_CODE
from helpers.constants import GET_METHOD, POST_METHOD, DELETE_METHOD, \
    PORT_ATTR, HOST_ATTR, VERSION_ATTR, KEY_ID_ATTR, PRIVATE_KEY_ATTR, \
    ALGORITHM_ATTR, FORMAT_ATTR, KID_ATTR, ALG_ATTR, PUBLIC_KEY_ATTR, \
    VALUE_ATTR, PROTOCOL_ATTR, STAGE_ATTR
from helpers.log_helper import get_logger
from services.key_management_service import KeyManagementService, IKey
from services.license_manager_service import LicenseManagerService
from services.setting_service import SettingsService, Setting

LM_SETTINGS_PATH = '/settings/license-manager'
CONFIG_PATH = '/config'
CLIENT_PATH = '/client'

_LOG = get_logger(__name__)

UNSUPPORTED_ALG_TEMPLATE = 'Algorithm:\'{alg}\' is not supported.'
KEY_OF_ENTITY_TEMPLATE = '{key}:\'{kid}\' of \'{uid}\' {entity}'

UNRESOLVABLE_ERROR = 'Request has run into an issue, which could not' \
                     ' be resolved.'

PEM_ATTR = 'PEM'


class LicenseManagerClientHandler(AbstractHandler):
    """
    Manages License Manager Client.
    """

    def __init__(
        self, settings_service: SettingsService,
        key_management_service: KeyManagementService,
        license_manager_service: LicenseManagerService
    ):
        self.settings_service = settings_service
        self.key_management_service = key_management_service
        self.license_manager_service = license_manager_service

    def define_action_mapping(self):
        return {
            LM_SETTINGS_PATH+CLIENT_PATH: {
                GET_METHOD: self.get,
                POST_METHOD: self.post,
                DELETE_METHOD: self.delete,
            }
        }

    def get(self, event: dict):
        _LOG.info(f'{GET_METHOD} License Manager Client-Key event: {event}')

        fmt = event.get(FORMAT_ATTR) or PEM_ATTR

        configuration: dict = self.settings_service. \
                                  get_license_manager_client_key_data() or {}

        kid = configuration.get(KID_ATTR)
        alg = configuration.get(ALG_ATTR)

        response = None

        if kid and alg:
            prk_kid = self.license_manager_service.derive_client_private_key_id(
                kid=kid
            )
            _LOG.info(f'Going to retrieve private-key by \'{prk_kid}\'.')
            prk = self.key_management_service.get_key(kid=prk_kid, alg=alg)
            if not prk:
                message = KEY_OF_ENTITY_TEMPLATE.format(
                    key='PrivateKey', kid=prk_kid, uid=alg,
                    entity='algorithm'
                )
                _LOG.warning(message + ' could not be retrieved.')
            else:
                _LOG.info(f'Going to derive a public-key of \'{kid}\'.')
                puk: IKey = self._derive_puk(prk=prk.key)
                if puk:
                    managed = self.key_management_service. \
                        instantiate_managed_key(
                            kid=kid, key=puk, alg=alg
                        )
                    _LOG.info(f'Going to export the \'{kid}\' public-key.')
                    response = self._response_dto(
                        exported_key=managed.export_key(frmt=fmt),
                        value_attr=PUBLIC_KEY_ATTR
                    )

        return build_response(
            code=RESPONSE_OK_CODE,
            content=response or []
        )

    def post(self, event: dict):
        # Validation is taken care of, on the gateway/abstract-handler layer.
        _LOG.info(
            f'{POST_METHOD} License Manager Client-Key event: {event}'
        )
        kid = event.get(KEY_ID_ATTR)
        alg = event.get(ALGORITHM_ATTR)
        raw_prk = event.get(PRIVATE_KEY_ATTR)
        frmt = event.get(FORMAT_ATTR)

        # Decoding is taking care of within the validation layer.

        if self.settings_service.\
                get_license_manager_client_key_data(value=False):
            return build_response(
                code=RESPONSE_CONFLICT,
                content='License Manager Client-Key already exists.'
            )

        prk = self.key_management_service.import_key(
            alg=alg, key_value=raw_prk
        )

        puk = self._derive_puk(prk=prk)
        if not puk:
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE,
                content='Improper private-key.'
            )

        if not prk:
            return build_response(
                content=UNSUPPORTED_ALG_TEMPLATE.format(alg=alg),
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE
            )

        prk = self.key_management_service.instantiate_managed_key(
            alg=alg, key=prk,
            kid=self.license_manager_service.derive_client_private_key_id(
                kid=kid
            )
        )

        message = KEY_OF_ENTITY_TEMPLATE.format(
            key='PublicKey', kid=prk.kid, uid=prk.alg, entity='algorithm'
        )

        _LOG.info(message + ' has been instantiated.')

        if not self.key_management_service.save_key(
            kid=prk.kid, key=prk.key, frmt=frmt
        ):
            return build_response(
                content=UNRESOLVABLE_ERROR,
                code=RESPONSE_INTERNAL_SERVER_ERROR
            )

        managed_puk = self.key_management_service.instantiate_managed_key(
            kid=kid, alg=alg, key=puk
        )

        setting = self.settings_service.create_license_manager_client_key_data(
            kid=kid, alg=alg
        )

        _LOG.info(f'Persisting License Manager Client-Key data:'
                  f' {setting.value}.')

        self.settings_service.save(setting=setting)

        return build_response(
            code=RESPONSE_OK_CODE,
            content=self._response_dto(
                exported_key=managed_puk.export_key(frmt=frmt),
                value_attr=PUBLIC_KEY_ATTR
            )
        )

    def delete(self, event: dict):
        _LOG.info(f'{DELETE_METHOD} License Manager Client-Key event: {event}')

        requested_kid = event.get(KEY_ID_ATTR)

        head = 'License Manager Client-Key'
        unretained = ' does not exist'
        code = RESPONSE_RESOURCE_NOT_FOUND_CODE
        # Default 404 error-response.
        content = head + unretained

        setting = self.settings_service. \
            get_license_manager_client_key_data(value=False)

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
            _LOG.warning(head + f' does not contain {requested_kid} \'kid\' data.')
            return build_response(code=code, content=content)

        is_key_data_removed = False

        prk_kid = self.license_manager_service.derive_client_private_key_id(
            kid=kid
        )

        _LOG.info(f'Going to retrieve private-key by \'{prk_kid}\'.')
        prk = self.key_management_service.get_key(kid=prk_kid, alg=alg)

        prk_head = KEY_OF_ENTITY_TEMPLATE.format(
            key='PrivateKey', kid=prk_kid, uid=alg, entity='algorithm'
        )
        if not prk:
            _LOG.warning(prk_head + ' could not be retrieved.')
        else:
            if not self.key_management_service.delete_key(kid=prk.kid):
                _LOG.warning(prk_head + ' could not be removed.')
            else:
                is_key_data_removed = True

        if self.settings_service.delete(setting=setting):
            committed = 'completely ' if is_key_data_removed else ''
            committed += 'removed'
            code = RESPONSE_OK_CODE
            content = head + f' has been {committed}'

        return build_response(code=code, content=content)

    @staticmethod
    def _response_dto(exported_key: dict, value_attr: str):
        if VALUE_ATTR in exported_key:
            exported_key[value_attr] = exported_key.pop(VALUE_ATTR)
        return exported_key

    @staticmethod
    def _derive_puk(prk: IKey):
        try:
            puk = prk.public_key()
        except (Exception, ValueError) as e:
            message = 'Public-Key could not be derived out of a ' \
                      f'private one, due to: "{e}".'
            _LOG.warning(message)
            puk = None
        return puk


class LicenseManagerConfigHandler(AbstractHandler):
    """
    Manages License Manager access-configuration.
    """

    def __init__(
        self, settings_service: SettingsService
    ):
        self.settings_service = settings_service

    def define_action_mapping(self):
        return {
            LM_SETTINGS_PATH+CONFIG_PATH: {
                GET_METHOD: self.get,
                POST_METHOD: self.post,
                DELETE_METHOD: self.delete,
            }
        }

    def get(self, event: dict):
        _LOG.info(f'{GET_METHOD} License Manager access-config event: {event}')

        configuration: dict = self.settings_service.\
            get_license_manager_access_data()
        return build_response(
            code=RESPONSE_OK_CODE,
            content=configuration or []
        )

    def post(self, event: dict):
        _LOG.info(
            f'{POST_METHOD} License Manager access-config event: {event}'
        )
        if self.settings_service.get_license_manager_access_data():
            return build_response(
                code=RESPONSE_CONFLICT,
                content='License Manager config-data already exists.'
            )
        # TODO check access ?
        setting = self.settings_service.\
            create_license_manager_access_data_configuration(
                host=event[HOST_ATTR],
                port=event.get(PORT_ATTR),
                protocol=event.get(PROTOCOL_ATTR),
                stage=event.get(STAGE_ATTR)
            )

        _LOG.info(f'Persisting License Manager config-data: {setting.value}.')
        self.settings_service.save(setting=setting)
        return build_response(
            code=RESPONSE_OK_CODE, content=setting.value
        )

    def delete(self, event: dict):
        _LOG.info(f'{DELETE_METHOD} License Manager access-config event:'
                  f' {event}')

        configuration: Setting = \
            self.settings_service.get_license_manager_access_data(
                value=False
            )
        if not configuration:
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content='License Manager config-data does not exist.'
            )
        _LOG.info(f'Removing License Manager config-data:'
                  f' {configuration.value}.')
        self.settings_service.delete(setting=configuration)
        return build_response(
            code=RESPONSE_OK_CODE,
            content='License Manager config-data has been removed.'
        )
