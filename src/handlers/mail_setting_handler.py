from handlers.abstracts.abstract_handler import AbstractHandler
from helpers import build_response, RESPONSE_OK_CODE, RESPONSE_CONFLICT, \
    RESPONSE_RESOURCE_NOT_FOUND_CODE, RESPONSE_BAD_REQUEST_CODE
from helpers.constants import GET_METHOD, POST_METHOD, DELETE_METHOD, \
    USERNAME_ATTR, PORT_ATTR, HOST_ATTR, PASSWORD_ATTR, DEFAULT_SENDER_ATTR, \
    USE_TLS_ATTR, MAX_EMAILS_ATTR
from helpers.log_helper import get_logger
from services.clients.smtp import SMTPClient
from services.clients.ssm import SSMClient
from services.setting_service import SettingsService, Setting

MAIL_SETTINGS_PATH = '/settings/mail'

_LOG = get_logger(__name__)

PASSWORD_ALIAS_ATTR = 'password_alias'
DISCLOSE_ATTR = 'disclose'


class MailSettingHandler(AbstractHandler):
    """
    Manages Mail configuration credentials.
    """

    def __init__(
        self, settings_service: SettingsService,
        smtp_client: SMTPClient, ssm_client: SSMClient
    ):
        self.settings_service = settings_service
        self.smtp_client = smtp_client
        self.ssm_client = ssm_client

    def define_action_mapping(self):
        return {
            MAIL_SETTINGS_PATH: {
                GET_METHOD: self.get,
                POST_METHOD: self.post,
                DELETE_METHOD: self.delete,
            }
        }

    def get(self, event: dict):
        _LOG.info(f'{GET_METHOD} Mail Configuration event: {event}')
        disclose: bool = event.get(DISCLOSE_ATTR)

        configuration: dict = self.settings_service.get_mail_configuration()
        if configuration and disclose:
            alias = configuration.get(PASSWORD_ATTR)
            password = self.ssm_client.get_secret_value(secret_name=alias)
            if not password:
                message = f'Password:\'{alias}\' could not be retrieved.'
                _LOG.error(message)
                return build_response(
                    code=RESPONSE_RESOURCE_NOT_FOUND_CODE, content=message
                )
            configuration[PASSWORD_ATTR] = password

        return build_response(
            code=RESPONSE_OK_CODE,
            content=configuration or []
        )

    def post(self, event: dict):
        # Validation is taken care of, on the gateway/abstract-handler layer.

        username = event.get(USERNAME_ATTR)
        password = event.get(PASSWORD_ATTR)
        _LOG.info(f'{POST_METHOD} Mail Configuration event: {event}')
        if self.settings_service.get_mail_configuration():
            return build_response(
                code=RESPONSE_CONFLICT,
                content='Mail configuration already exists.'
            )

        self.smtp_client.host = event.get(HOST_ATTR)
        self.smtp_client.port = event.get(PORT_ATTR)

        issue = ''
        with self.smtp_client as client:
            if event.get(USE_TLS_ATTR) and not client.tls():
                issue = 'TLS could not be established.'

            if not issue and not client.authenticate(
                username=username, password=password
            ):
                issue = 'Improper mail credentials.'

        if issue:
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE, content=issue
            )

        name = event[PASSWORD_ALIAS_ATTR]
        _LOG.info(f'Persisting password parameter under a \'{name}\' secret')
        self.ssm_client.create_secret(
            secret_name=name, secret_value=password
        )

        payload = {
            each: event[each] for each in self._model_required_map()
            if each in event
        }
        setting = self.settings_service.create_mail_configuration(**payload)
        _LOG.info(f'Persisting mail-configuration data: {payload}.')
        self.settings_service.save(setting=setting)
        return build_response(
            code=RESPONSE_OK_CODE, content=setting.value
        )

    def delete(self, event: dict):
        _LOG.info(f'{DELETE_METHOD} Mail Configuration event: {event}')
        configuration: Setting = self.settings_service.get_mail_configuration(
            value=False
        )
        if not configuration:
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content='Mail configuration does not exist.'
            )
        name = configuration.value.get(PASSWORD_ATTR)
        _LOG.info(f'Removing mail-settings data: {configuration}.')
        self.settings_service.delete(setting=configuration)
        _LOG.info(f'Removing parameter data of \'{name}\' secret.')
        self.ssm_client.delete_parameter(
            secret_name=name
        )
        return build_response(
            code=RESPONSE_OK_CODE,
            content='Mail configuration has been removed.'
        )

    @staticmethod
    def _model_required_map():
        return {
            USERNAME_ATTR: str,
            PASSWORD_ALIAS_ATTR: str,
            PORT_ATTR: int,
            HOST_ATTR: str,
            MAX_EMAILS_ATTR: int,
            DEFAULT_SENDER_ATTR: str,
            USE_TLS_ATTR: bool
        }

    @staticmethod
    def _session_required_map():
        return {
            HOST_ATTR: str,
            PORT_ATTR: int
        }
