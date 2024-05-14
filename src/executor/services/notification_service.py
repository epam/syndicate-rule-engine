import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

from jinja2 import Environment, BaseLoader
from modular_sdk.models.tenant import Tenant

from helpers.log_helper import get_logger
from services.clients.s3 import S3Client
from services.clients.ssm import AbstractSSMClient
from services.setting_service import SettingsService

DEFAULT_SENDER_ATTR = 'default_sender'
USE_TLS_ATTR = 'use_tls'
MAX_EMAILS_ATTR = 'max_emails'

CUSTODIAN_FOLDER_NAME = 'custodian'

_LOG = get_logger(__name__)


class NotificationService:
    def __init__(self, setting_service: SettingsService,
                 ssm_client: AbstractSSMClient, s3_client: S3Client):
        self.setting_service = setting_service
        self.ssm = ssm_client
        self.s3_client = s3_client

        self._settings = None
        self._password = None
        self._client, self._num_emails = None, 0
        self._close_session()

    @property
    def setting(self) -> dict:
        if not self._settings:
            self._settings = \
                self.setting_service.get_mail_configuration() or {}
        return self._settings

    @property
    def host(self):
        return self.setting.get('host') or 'localhost'

    @property
    def port(self):
        return self.setting.get('port') or 25

    @property
    def username(self):
        return self.setting.get('username')

    @property
    def password(self):
        if not self._password:
            self._password = self.ssm.get_secret_value(
                self.setting.get('password')
            )
        return self._password

    @property
    def default_sender(self):
        return self.setting.get(DEFAULT_SENDER_ATTR)

    @property
    def use_tls(self):
        return self.setting.get(USE_TLS_ATTR) is not False  # True default

    @property
    def max_emails(self):
        return self.setting.get(MAX_EMAILS_ATTR) or 1

    def __del__(self):
        self._close_session()

    @staticmethod
    def build_message(sender_email: str, recipients: list, subject: str,
                      text: str = None, html: str = None, attachment=None,
                      attachment_filename='attachment') -> str:
        multipart_content_subtype = 'alternative' if text and html else 'mixed'
        msg = MIMEMultipart(multipart_content_subtype)
        msg['Subject'] = subject
        msg['From'] = sender_email
        msg['To'] = ', '.join(recipients)

        if text:
            part = MIMEText(text, 'plain')
            msg.attach(part)
        if html:
            part = MIMEText(html, 'html')
            msg.attach(part)

        # Add attachments
        if attachment:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(open('temp_workbook.csv', 'rb').read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment',
                            filename=f'{attachment_filename}.csv')
            msg.attach(part)
        return msg.as_string()

    def _get_template(self, filename: str) -> Optional[str]:
        bucket_name = self.setting_service.get_template_bucket()  # move todo
        if not bucket_name:
            _LOG.warning('Template bucket name is not set in CaaSSettings')
            return
        content = self.s3_client.get_object(
            bucket=bucket_name,
            key=f'{CUSTODIAN_FOLDER_NAME}/{filename}',
        )
        if not content:
            _LOG.warning('Application Service is not configured properly: '
                         f'cannot get `{filename}` from `{bucket_name}`')
        return content.read().decode()

    def _get_findings_template(self) -> Optional[str]:
        return self._get_template('findings.html')

    def _get_schedule_deactivate_template(self):
        return self._get_template('schedule_deactivate.html')

    def _init_session(self):
        self._num_emails = 0
        _LOG.info('Going to init SMTP connection')
        client = smtplib.SMTP(host=self.host, port=self.port)
        if self.use_tls:
            _LOG.info('Starting tls connection')
            client.starttls()
        if self.username and self.password:
            _LOG.info(f'Username \'{self.username}\' and password were '
                      f'given. Logging in..')
            client.login(self.username, self.password)
        self._client = client

    def _close_session(self):
        if isinstance(self._client, smtplib.SMTP):
            _LOG.info('Going to close SMTP connection')
            try:
                self._client.quit()
            except smtplib.SMTPException:
                pass
        self._client = None
        self._num_emails = 0

    def send_rescheduling_notice_notification(
            self, recipients: list, subject: str, tenant: Tenant,
            scheduled_job_name: str,
            ruleset_list: list[str], customer: str, sender_email: str = None):
        sender_email = sender_email or self.default_sender or self.username
        _LOG.info(f'Going to push rescheduling-notice notification from'
                  f' {sender_email} to {recipients}. Subject: \'{subject}\'')
        template_content = self._get_schedule_deactivate_template()
        if template_content:
            _body = self._render_template(
                template_content=template_content,
                data={'scheduled_job_name': scheduled_job_name,
                      'account_name': tenant.name,
                      'account_id': tenant.project,
                      'customer': customer,
                      'ruleset_list': ruleset_list})
            return self.send_email(
                sender_email=sender_email, recipients=recipients,
                message=self.build_message(
                    sender_email=sender_email, recipients=recipients,
                    subject=subject, html=_body
                )
            )

    def send_email(self, sender_email, recipients, message):
        if not self._client:
            try:
                self._init_session()
            except (smtplib.SMTPException, ConnectionError) as e:
                _LOG.error(f'An error occurred while initializing '
                           f'connection to SMTP server: {e}')
                return False
        try:
            response = self._client.sendmail(sender_email, recipients, message)
            _LOG.info(f'The mail was sent. Response received after '
                      f'sending: {response}')
        except smtplib.SMTPException as e:
            _LOG.error(f'An error occurred while sending an email: {e}')
            self._close_session()
            return False
        return True

    @staticmethod
    def _render_template(template_content: str, data: dict):
        env = Environment(loader=BaseLoader()).from_string(template_content)
        result = env.render(**data)
        return result
