from functools import cached_property
from http import HTTPStatus

from handlers import AbstractHandler, Mapping
from helpers.constants import (
    CustodianEndpoint,
    HTTPMethod,
    PASSWORD_ATTR,
)
from helpers.lambda_response import ResponseFactory
from helpers.lambda_response import build_response
from helpers.log_helper import get_logger
from services import SP
from services.clients.smtp import SMTPClient
from services.clients.ssm import SSMClient
from services.setting_service import Setting, SettingsService
from validators.swagger_request_models import (
    BaseModel,
    MailSettingGetModel,
    MailSettingPostModel,
)
from validators.utils import validate_kwargs

_LOG = get_logger(__name__)

PASSWORD_ALIAS_ATTR = 'password_alias'
DISCLOSE_ATTR = 'disclose'


class MailSettingHandler(AbstractHandler):
    """
    Manages Mail configuration credentials.
    """

    def __init__(self, settings_service: SettingsService,
                 smtp_client: SMTPClient, ssm_client: SSMClient):
        self.settings_service = settings_service
        self.smtp_client = smtp_client
        self.ssm_client = ssm_client

    @cached_property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.SETTINGS_MAIL: {
                HTTPMethod.GET: self.get,
                HTTPMethod.POST: self.post,
                HTTPMethod.DELETE: self.delete,
            }
        }

    @classmethod
    def build(cls):
        return cls(
            settings_service=SP.settings_service,
            smtp_client=SMTPClient(),
            ssm_client=SP.ssm
        )

    @validate_kwargs
    def get(self, event: MailSettingGetModel):
        configuration: dict = self.settings_service.get_mail_configuration()
        if not configuration:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                'No mail configuration').exc()
        if event.disclose:
            alias = configuration.get(PASSWORD_ATTR)
            password = self.ssm_client.get_secret_value(secret_name=alias)
            configuration[PASSWORD_ATTR] = password

        return build_response(content=configuration)

    @validate_kwargs
    def post(self, event: MailSettingPostModel):
        # Validation is taken care of, on the gateway/abstract-handler layer.

        username = event.username
        password = event.password
        if self.settings_service.get_mail_configuration():
            return build_response(
                code=HTTPStatus.CONFLICT,
                content='Mail configuration already exists.'
            )

        self.smtp_client.host = event.host
        self.smtp_client.port = event.port

        issue = ''
        with self.smtp_client as client:
            if event.use_tls and not client.tls():
                issue = 'TLS could not be established.'

            if not issue and not client.authenticate(
                    username=username, password=password
            ):
                issue = 'Improper mail credentials.'

        if issue:
            return build_response(
                code=HTTPStatus.BAD_REQUEST, content=issue
            )

        name = event.password_alias
        _LOG.info(f'Persisting password parameter under a \'{name}\' secret')
        self.ssm_client.create_secret(
            secret_name=name, secret_value=password
        )

        setting = self.settings_service.create_mail_configuration(
            username=event.username,
            password_alias=event.password_alias,
            default_sender=event.default_sender,
            host=event.host,
            port=event.port,
            use_tls=event.use_tls,
            max_emails=event.max_emails
        )
        self.settings_service.save(setting=setting)
        return build_response(
            code=HTTPStatus.CREATED, content=setting.value
        )

    @validate_kwargs
    def delete(self, event: BaseModel):
        configuration: Setting = self.settings_service.get_mail_configuration(
            value=False
        )
        if not configuration:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content='Mail configuration does not exist.'
            )
        name = configuration.value.get(PASSWORD_ATTR)
        _LOG.info(f'Removing mail-settings data: {configuration}.')
        self.settings_service.delete(setting=configuration)
        _LOG.info(f'Removing parameter data of \'{name}\' secret.')
        self.ssm_client.delete_parameter(
            secret_name=name
        )
        return build_response(code=HTTPStatus.NO_CONTENT)
