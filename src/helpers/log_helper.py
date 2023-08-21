import logging
import os
import re
from sys import stdout
from functools import cached_property
from typing import Dict

LOG_FORMAT = '%(asctime)s - %(levelname)s - %(name)s - %(message)s'


class SensitiveFormatter(logging.Formatter):
    """Formatter that removes sensitive information."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._param_to_regex: Dict[str, re.Pattern] = {}

    @cached_property
    def secured_params(self) -> set:
        return {
            'refresh_token', 'id_token', 'password', 'authorization', 'secret',
            'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN', 'git_access_secret',
            'api_key', 'AZURE_CLIENT_ID', 'AZURE_CLIENT_SECRET',
            'GOOGLE_APPLICATION_CREDENTIALS', 'private_key', 'private_key_id',
            'Authorization', 'Authentication', 'client_email'
        }

    @staticmethod
    def _compile_param_regex(param: str) -> re.Pattern:
        """
        It searches for values in JSON objects where key is $param:
        If param is "password" the string '{"password": "blabla"}' will be
        printed as '{"password": "****"}'
        [\'"] - single or double quote; [ ]* - zero or more spaces
        """
        return re.compile(f'[\'"]{param}[\'"]:[ ]*[\'"](.*?)[\'"]')

    def get_param_regex(self, param: str) -> re.Pattern:
        if param not in self._param_to_regex:
            self._param_to_regex[param] = self._compile_param_regex(param)
        return self._param_to_regex[param]

    def _filter(self, string):
        # Hoping that this regex substitutions do not hit performance...
        for param in self.secured_params:
            string = re.sub(self.get_param_regex(param),
                            f'\'{param}\': \'****\'', string)
        return string

    def format(self, record):
        original = logging.Formatter.format(self, record)
        return self._filter(original)


custodian_logger = logging.getLogger('custodian')
custodian_logger.propagate = False
console_handler = logging.StreamHandler(stream=stdout)
console_handler.setFormatter(SensitiveFormatter(LOG_FORMAT))
custodian_logger.addHandler(console_handler)

log_level = os.getenv('CUSTODIAN_LOG_LEVEL') or 'DEBUG'
try:
    custodian_logger.setLevel(log_level)
except ValueError:  # not valid log level name
    custodian_logger.setLevel(logging.DEBUG)
logging.captureWarnings(True)


def get_logger(log_name, level=log_level):
    """
    :param level:   CRITICAL = 50
                    ERROR = 40
                    WARNING = 30
                    INFO = 20
                    DEBUG = 10
                    NOTSET = 0
    :type log_name: str
    :type level: int
    """
    module_logger = custodian_logger.getChild(log_name)
    if level:
        module_logger.setLevel(level)
    return module_logger
