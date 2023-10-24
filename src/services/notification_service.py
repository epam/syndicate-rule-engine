import base64
import io
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Union

from jinja2 import Environment, BaseLoader

# todo import matplotlib only when necessary 'cause it's huge
#  and requires numpy. Importing time can actually be noticeable
import matplotlib.pyplot as plt

from helpers.log_helper import get_logger
from helpers.constants import DEFAULT_SENDER_ATTR, MAX_EMAILS_ATTR, \
    USE_TLS_ATTR
from services.setting_service import SettingsService
from services.ssm_service import SSMService
from services.clients.s3 import S3Client

RED_COLOR_HASH = '#D10000'
GREEN_COLOR_HASH = '#00E400'

CUSTODIAN_FOLDER_NAME = 'custodian'

_LOG = get_logger(__name__)


class NotificationService:
    def __init__(self, setting_service: SettingsService,
                 ssm_service: SSMService, s3_service: S3Client):
        self.setting_service = setting_service
        self.ssm_service = ssm_service
        self.s3_service = s3_service

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
            self._password = self.ssm_service.get_secret_value(
                self.setting.get('password'))
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
    def build_donut_chart(succeeded: int, failed: int):
        plt.clf()
        label = f'{succeeded}/{succeeded + failed}'
        size_of_groups = [failed, succeeded]
        plt.pie(size_of_groups, startangle=60,
                colors=[RED_COLOR_HASH, GREEN_COLOR_HASH])

        # circle to create donut chart from pie chart
        circle = plt.Circle((0, 0), 0.8, color='white')
        plt.gca().add_artist(circle)

        # label at the center of chart
        plt.rcParams.update({'font.size': 24})
        plt.text(0, 0, label, ha='center', va='center')

        buf = io.BytesIO()
        plt.savefig(buf, format='png', transparent=True,
                    bbox_inches='tight', pad_inches=0)
        return base64.b64encode(buf.getvalue()).decode('utf-8')

    @staticmethod
    def build_pie_chart(labels: List[str], values: List[Union[float, int]],
                        colors: list = None):
        plt.clf()  # clean plot from previous data
        if len(labels) != len(values):
            _LOG.warning(
                'The length of labels does not math the length of values')
            return None
        for v in values.copy():
            if float(v) == 0.0:
                if colors:
                    colors.remove(colors[values.index(v)])
                labels.remove(labels[values.index(v)])
                values.remove(v)

        if len(values) > 5:
            mapping = dict(zip(labels, values))
            sorted_by_values = dict(sorted(
                mapping.items(), key=lambda item: item[1], reverse=True))
            other = 0
            for v in list(sorted_by_values.values())[5:]:
                other += v
            labels = list(sorted_by_values.keys())[:5] + ['other']
            values = list(sorted_by_values.values())[:5] + [other]

        plt.pie(values, labels=labels, autopct='%.1f%%',
                textprops={'fontsize': 9}, colors=colors)

        buf = io.BytesIO()
        plt.savefig(buf, format='png', transparent=True,
                    bbox_inches='tight', pad_inches=0)
        return base64.b64encode(buf.getvalue()).decode('utf-8')

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

    def _get_event_driven_template(self):
        content = self.s3_service.get_file_content(
            bucket_name=self.setting_service.get_template_bucket(),
            full_file_name=f'{CUSTODIAN_FOLDER_NAME}/'
                           f'event_driven_vulnerabilities.html')
        return content.decode('utf-8')

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

    def send_event_driven_notification(
            self, recipients: list, subject: str, data: dict,
            sender_email: str = None
    ):
        sender_email = sender_email or self.default_sender or self.username
        _LOG.info(f'Going to push event-driven notification from'
                  f' {sender_email} to {recipients}. Subject: \'{subject}\'')
        template_content = self._get_event_driven_template()
        if template_content:
            _body = self._render_template(
                template_content=template_content,
                data=data)
            return self.send_email(
                sender_email=sender_email, recipients=recipients,
                message=self.build_message(
                    sender_email=sender_email, recipients=recipients,
                    subject=subject, html=_body
                )
            )
        _LOG.warning('Cannot get event-driven vulnerabilities template. '
                     'Notification was not sent')
        return None

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
