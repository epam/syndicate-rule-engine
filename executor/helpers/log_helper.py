import logging
import os
import pathlib
import re
from functools import cached_property
from sys import stdout
from typing import Dict

from helpers.constants import ENV_SERVICE_MODE, DOCKER_SERVICE_MODE, \
    SAAS_SERVICE_MODE

JOB_ID = os.environ.get('AWS_BATCH_JOB_ID')
SERVICE_MODE = os.getenv(ENV_SERVICE_MODE) or SAAS_SERVICE_MODE

LOG_FORMAT = '%(asctime)s - %(levelname)s - %(name)s - %(message)s'

_name_to_level = {
    'CRITICAL': logging.CRITICAL,
    'FATAL': logging.FATAL,
    'ERROR': logging.ERROR,
    'WARNING': logging.WARNING,
    'INFO': logging.INFO,
    'DEBUG': logging.DEBUG
}


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
            'Authorization', 'Authentication'
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


try:
    log_level = _name_to_level.get(os.environ['LOG_LEVEL'])
except KeyError as e:
    log_level = logging.DEBUG

custodian_logger = logging.getLogger('custodian')
custodian_logger.propagate = False
custodian_logger.setLevel(log_level)
logging.captureWarnings(True)

console_handler = logging.StreamHandler(stream=stdout)
console_handler.setFormatter(SensitiveFormatter(LOG_FORMAT))
custodian_logger.addHandler(console_handler)

if SERVICE_MODE == DOCKER_SERVICE_MODE:
    cur_dir = os.getcwd()
    file_path = pathlib.Path(__file__).parent.resolve()

    docker = file_path.parent

    LOG_FOLDER = os.path.join(docker, 'logs')
    if not os.path.exists(LOG_FOLDER):
        os.mkdir(LOG_FOLDER)
    error_log_path = os.path.join(LOG_FOLDER, JOB_ID, 'error.log')
    log_path = os.path.join(LOG_FOLDER, JOB_ID, 'all_logs.log')
    if not os.path.exists(os.path.join(LOG_FOLDER, JOB_ID)):
        os.mkdir(os.path.join(LOG_FOLDER, JOB_ID))
    if not os.path.exists(log_path):
        open(log_path, 'w').close()
    if not os.path.exists(error_log_path):
        open(error_log_path, 'w').close()

    log_error = logging.FileHandler(error_log_path, 'a')
    log_error.setLevel(logging.ERROR)
    log_info = logging.FileHandler(log_path, 'a')
    log_info.setLevel(log_level)

    formatter = logging.Formatter(
        '%(asctime)s.%(msecs)03d - %(levelname)s - %(name)s - %(message)s ; ',
        datefmt='%Y-%m-%dT%H:%M:%S')

    log_error.setFormatter(formatter)
    log_info.setFormatter(formatter)

    custodian_logger.addHandler(log_info)
    custodian_logger.addHandler(log_error)


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
