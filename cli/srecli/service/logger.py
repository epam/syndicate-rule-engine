import logging
import logging.config
import os
import re
from datetime import date
from pathlib import Path

from srecli import __version__
from srecli.service.constants import Env


class TermColor:
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    FAIL = '\033[91m'
    DEBUG = '\033[90m'
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    UNDERLINE = '\033[4m'
    BOLD_RED = '\x1b[31;1m'

    _pattern = '{color}{string}' + ENDC

    @classmethod
    def blue(cls, st: str) -> str:
        return cls._pattern.format(color=cls.OKBLUE, string=st)

    @classmethod
    def cyan(cls, st: str) -> str:
        return cls._pattern.format(color=cls.OKCYAN, string=st)

    @classmethod
    def green(cls, st: str) -> str:
        return cls._pattern.format(color=cls.OKGREEN, string=st)

    @classmethod
    def yellow(cls, st: str) -> str:
        return cls._pattern.format(color=cls.WARNING, string=st)

    @classmethod
    def red(cls, st: str) -> str:
        return cls._pattern.format(color=cls.FAIL, string=st)

    @classmethod
    def gray(cls, st: str) -> str:
        return cls._pattern.format(color=cls.DEBUG, string=st)

    @classmethod
    def bold_red(cls, st: str) -> str:
        return cls._pattern.format(color=cls.BOLD_RED, string=st)


class ColorFormatter(logging.Formatter):
    formats = {
        logging.DEBUG: TermColor.gray,
        logging.INFO: TermColor.green,
        logging.WARNING: TermColor.yellow,
        logging.ERROR: TermColor.red,
        logging.CRITICAL: TermColor.bold_red
    }

    def format(self, record):
        return self.formats[record.levelno](super().format(record))


class SensitiveFormatter(logging.Formatter):
    """
    Formatter that removes sensitive information from jsons
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


LOG_FORMAT = '%(asctime)s %(levelname)s %(name)s.%(funcName)s:%(lineno)d %(message)s'

logging.captureWarnings(capture=True)
logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'console_formatter': {
            'format': '%(asctime)s %(levelname)s %(name)s.%(funcName)s:%(lineno)d %(message)s',
            '()': SensitiveFormatter
        },
        'user_formatter': {
            'format': '%(message)s',
            '()': ColorFormatter
        }
    },
    'handlers': {
        'console_handler': {
            'class': 'logging.StreamHandler',
            'formatter': 'console_formatter'
        },
        'user_handler': {
            'class': 'logging.StreamHandler',
            'formatter': 'user_formatter'
        }
    },
    'loggers': {
        'srecli': {
            'level': Env.LOG_LEVEL.get(),
            'handlers': ['console_handler'],
            'propagate': False
        },
        'srecli.user': {
            'level': Env.LOG_LEVEL.get(),
            'handlers': ['user_handler'],
            'propagate': False
        }
    }
})


def get_logger(name: str, level: str | None = None, /) -> logging.Logger:
    log = logging.getLogger(name)
    if level:
        log.setLevel(level)
    return log


def get_user_logger():
    return logging.getLogger('srecli.user')


LOGS_FOLDER = Path('logs/srecli')
LOGS_FILE_NAME = date.today().strftime('%Y-%m-%d-sre.log')



def write_verbose_logs():
    return # todo make it
    os.makedirs(LOGS_FOLDER, exist_ok=True)
    file_handler = FileHandler(LOGS_FOLDER / LOGS_FILE_NAME)
    file_handler.setLevel(DEBUG)
    formatter = SensitiveFormatter(VERBOSE_MODE_LOG_FORMAT)
    file_handler.setFormatter(formatter)
    c7n_logger.addHandler(file_handler)
    c7n_logger.info(f'sre cli version: {__version__}')
