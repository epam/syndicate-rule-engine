from smtplib import SMTP, SMTPConnectError, SMTPException
from typing import Optional, List

from helpers.log_helper import get_logger

_LOG = get_logger(__name__)


class SMTPClient:
    host: Optional[str]
    port: Optional[int]

    connection: Optional[SMTP]

    def __init__(self):
        self._reset()

    def _reset(self):
        self.host = None
        self.port = None
        self.connection = None

    def __enter__(self):
        if not self.connection:
            self._establish_connection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()
        self._reset()

    def _establish_connection(self):
        if self.host and self.port:
            try:
                self.connection = SMTP(host=self.host, port=self.port)
                self.connection.ehlo()
            except SMTPConnectError as e:
                _LOG.warning('The following exception has occurred, while '
                             'trying to establish a connection with '
                             f'{self.host}:{self.port} - {e}.')

    def close_connection(self):
        if self.connection:
            try:
                self.connection.quit()
                _LOG.debug(f'Connection to {self.host}:{self.port} has been '
                           f'closed.')
            except SMTPException as e:
                _LOG.error('The following exception has occurred, during '
                           f'connection closing as - {e}.')
                self.connection.close()

    def authenticate(self, username: str, password: str):
        if self.connection:
            try:
                _LOG.debug(f'Going to authenticate as {username}.')
                self.connection.ehlo()
                self.connection.login(user=username, password=password)
                return True
            except SMTPException as e:
                _LOG.warning('The following exception has occurred, during '
                             f'authentication as {username} - "{e}".')
        return False

    def send(self, sender: str, recipients: List[str], msg: str) -> bool:
        if self.connection:
            try:
                response = self.connection.sendmail(
                    from_addr=sender, to_addrs=recipients, msg=msg
                )
                _LOG.debug(f'Email to {recipients} from {sender} has been '
                           f'sent, response: {response}')
                return True
            except SMTPException as e:
                _LOG.warning('The following exception has occurred, while '
                             f'trying to send an email to {recipients} '
                             f'from {sender} - {e}.')
        return False

    def tls(self, keyfile=None, certfile=None, context=None):
        if self.connection:
            try:
                self.connection.starttls(
                    keyfile=keyfile, certfile=certfile, context=context
                )
                self.connection.ehlo()
                return True
            except SMTPException as e:
                _LOG.warning('The following exception has occurred, while '
                             f'trying establish TLS - {e}.')
        return False
