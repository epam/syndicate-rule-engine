import os
import re
import traceback
from datetime import date
from getpass import getuser
from logging import DEBUG, getLogger, Formatter, StreamHandler, INFO, \
    FileHandler, NullHandler
from pathlib import Path

from c7ncli.service.constants import C7NCLI_LOG_LEVEL_ENV_NAME
from c7ncli.version import __version__

LOGS_FOLDER = 'c7ncli-logs'
LOGS_FILE_NAME = date.today().strftime('%Y-%m-%d-c7n.log')
# LOGS_FOLDER_PATH = Path(__file__).parent.parent.parent / LOGS_FOLDER
LOGS_FOLDER_PATH = Path(os.getcwd(), LOGS_FOLDER)


SYSTEM_LOG_FORMAT = f'%(asctime)s [USER: {getuser()}] %(message)s'
USER_LOG_FORMAT = '%(message)s'
VERBOSE_MODE_LOG_FORMAT = f'%(asctime)s [%(levelname)s] ' \
                          f'USER:{getuser()} LOG: %(message)s'


class SensitiveFormatter(Formatter):
    """Formatter that removes sensitive information."""
    SECURED_PARAMS = {
        'refresh_token', 'id_token', 'password', 'authorization', 'secret',
        'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN', 'git_access_secret',
        'api_key', 'AZURE_CLIENT_ID', 'AZURE_CLIENT_SECRET',
        'GOOGLE_APPLICATION_CREDENTIALS', 'private_key', 'private_key_id',
        'Authorization', 'Authentication', 'sdk_secret_key', 'key_id'
    }

    @staticmethod
    def _filter(string):
        for param in SensitiveFormatter.SECURED_PARAMS:
            # [\'"] - single or double quote; [ ]? - zero or more spaces
            string = re.sub(f'[\'"]{param}[\'"]:[ ]*[\'"](.*?)[\'"]',
                            f'\'{param}\': \'****\'', string)
        return string

    def format(self, record):
        original = Formatter.format(self, record)
        return self._filter(original)


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


def exception_handler_formatter(exception_type, exception, exc_traceback):
    c7n_logger.error('%s: %s', exception_type.__name__, exception)
    traceback.print_tb(tb=exc_traceback, limit=15)


def write_verbose_logs():
    os.makedirs(LOGS_FOLDER_PATH, exist_ok=True)
    file_handler = FileHandler(LOGS_FOLDER_PATH / LOGS_FILE_NAME)
    file_handler.setLevel(DEBUG)
    formatter = SensitiveFormatter(VERBOSE_MODE_LOG_FORMAT)
    file_handler.setFormatter(formatter)
    c7n_logger.addHandler(file_handler)
    c7n_logger.info(f'c7n cli version: {__version__}')
