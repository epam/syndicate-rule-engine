import json
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import boto3

from helpers.log_helper import get_logger
from integrations.abstract_adapter import AbstractAdapter

SIEM_SES_TYPE = 'ses'

KEY_RECIPIENTS = 'recipients'
CHARSET = "UTF-8"
_LOG = get_logger(__name__)

MESSAGE_SUBJECT_TEMPLATE = "Custodian Service Notification: \'{account}\' account"
MESSAGE_TEMPLATE = \
    """<h3>Customer: <strong>{customer}</strong></h3>
    <h4>Account: <strong>{account}</strong></h4>
    <h4>Job id: <strong>{job_id}</strong></h4>
    
    <p>
        Total Checks Performed: <strong>{total_checks_performed}</strong> </br>
        Failed Checks: <strong>{failed_checks}</strong></br>
        Successful Checks: <strong>{successful_checks}</strong></br>
        Total resources with violations: <strong>{total_resources_violated_rules}</strong>
    </p>
    """


class SESAdapter(AbstractAdapter):
    def __init__(self, configuration: dict, region):
        if not configuration.get('type') == SIEM_SES_TYPE:
            return
        self.configuration = configuration.get('configuration')
        self.client = boto3.client('ses', region_name=region)

    def push_notification(self, **kwargs):
        _LOG.debug(f'Going to push email notifications')
        recipients = self.configuration.get(KEY_RECIPIENTS, [])
        _LOG.debug(f'Recipients to notify: {recipients}')
        detailed_report = kwargs.get('detailed_report')
        report = kwargs.get('report')
        account_name = kwargs.get('account_display_name')
        customer_name = kwargs.get('customer_display_name')
        job_id = kwargs.get('job_id')

        email_body = MESSAGE_TEMPLATE.format(
            customer=customer_name,
            account=account_name,
            job_id=job_id,
            **report
        )
        subject = MESSAGE_SUBJECT_TEMPLATE.format(
            account=account_name
        )
        response = self.send_raw_email(
            sender='bohdan_onsha@epam.com',
            recipients=recipients,
            title=subject,
            html=email_body,
            attachment=json.dumps(detailed_report, indent=4).encode('utf-8')
        )
        _LOG.debug(f'ses response: {response}')

    def send_raw_email(self, sender: str, recipients: list, title: str,
                       text: str = None, html: str = None,
                       attachment=None) -> MIMEMultipart:
        multipart_content_subtype = 'alternative' if text and html else 'mixed'
        msg = MIMEMultipart(multipart_content_subtype)
        msg['Subject'] = title
        msg['From'] = sender
        msg['To'] = ', '.join(recipients)

        # Record the MIME types of both parts - text/plain and text/html.
        # According to RFC 2046, the last part of a multipart message,
        # in this case the HTML message, is best and preferred.
        if text:
            part = MIMEText(text, 'plain')
            msg.attach(part)
        if html:
            part = MIMEText(html, 'html')
            msg.attach(part)

        # Add attachments
        if attachment:
            part = MIMEApplication(attachment)
            part.add_header('Content-Disposition', 'attachment',
                            filename='detailed_report.json')
            msg.attach(part)

        return self.client.send_raw_email(
            Source=sender,
            Destinations=recipients,
            RawMessage={'Data': msg.as_string()}
        )
