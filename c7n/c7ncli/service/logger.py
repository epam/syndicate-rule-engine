from datetime import date
from getpass import getuser
from logging import (
    DEBUG,
    FileHandler,
    Formatter,
    INFO,
    NullHandler,
    StreamHandler,
    getLogger,
)
import os
from pathlib import Path
import re

from c7ncli.service.constants import C7NCLI_LOG_LEVEL_ENV_NAME
from c7ncli.version import __version__

LOGS_FOLDER = Path('logs/c7ncli')
LOGS_FILE_NAME = date.today().strftime('%Y-%m-%d-c7n.log')


SYSTEM_LOG_FORMAT = f'%(asctime)s [USER: {getuser()}] %(message)s'
USER_LOG_FORMAT = '%(message)s'
VERBOSE_MODE_LOG_FORMAT = f'%(asctime)s [%(levelname)s] ' \
                          f'USER:{getuser()} LOG: %(message)s'


class SensitiveFormatter(Formatter):
    """
    Formatter that removes sensitive information.
    """
    _inner = '|'.join((
        'refresh_token', 'id_token', 'password', 'authorization', 'secret',
        'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN', 'git_access_secret',
        'api_key', 'AZURE_CLIENT_ID', 'AZURE_CLIENT_SECRET',
        'GOOGLE_APPLICATION_CREDENTIALS', 'private_key', 'private_key_id',
        'Authorization', 'Authentication', 'sdk_secret_key', 'key_id',
        'certificate', 'access_token', 'refresh_token'
    ))
    # assuming that only raw python dicts will be written. This regex won't
    # catch exposed secured params inside JSON strings. In looks only for
    # single quotes
    regex = re.compile(rf"'({_inner})':\s*?'(.*?)'")

    def format(self, record):
        return re.sub(
            self.regex,
            r"'\1': '****'",
            super().format(record)
        )


# SYSTEM logger
c7n_logger = getLogger('c7n')
c7n_logger.setLevel(DEBUG)
c7n_logger.propagate = False

log_level = os.getenv(C7NCLI_LOG_LEVEL_ENV_NAME)
if log_level:
    console_handler = StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(SensitiveFormatter(SYSTEM_LOG_FORMAT))
    c7n_logger.addHandler(console_handler)
else:
    c7n_logger.addHandler(NullHandler())

# USER logger
c7n_user_logger = getLogger('user.c7n')
c7n_user_logger.setLevel(DEBUG)
c7n_user_logger.propagate = False
console_handler = StreamHandler()
console_handler.setLevel(INFO)
console_handler.setFormatter(SensitiveFormatter(USER_LOG_FORMAT))
c7n_user_logger.addHandler(console_handler)


def get_logger(log_name, level=DEBUG):
    module_logger = c7n_logger.getChild(log_name)
    module_logger.setLevel(level)
    return module_logger


def get_user_logger(log_name, level=INFO):
    module_logger = c7n_user_logger.getChild(log_name)
    module_logger.setLevel(level)
    return module_logger


def write_verbose_logs():
    os.makedirs(LOGS_FOLDER, exist_ok=True)
    file_handler = FileHandler(LOGS_FOLDER / LOGS_FILE_NAME)
    file_handler.setLevel(DEBUG)
    formatter = SensitiveFormatter(VERBOSE_MODE_LOG_FORMAT)
    file_handler.setFormatter(formatter)
    c7n_logger.addHandler(file_handler)
    c7n_logger.info(f'c7n cli version: {__version__}')
