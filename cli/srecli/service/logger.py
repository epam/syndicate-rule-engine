import logging
import logging.config
import re
from pathlib import Path

from srecli.service.constants import Env

LOG_FORMAT = '%(asctime)s %(levelname)s %(name)s.%(funcName)s:%(lineno)d %(message)s'


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


config = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'base_formatter': {
            'format': LOG_FORMAT,
            '()': SensitiveFormatter
        },
        'user_formatter': {
            'format': '%(message)s',
            '()': ColorFormatter
        },
    },
    'handlers': {
        'user_handler': {
            'class': 'logging.StreamHandler',
            'formatter': 'user_formatter'
        },
        'null_handler': {
            'class': 'logging.NullHandler'
        }
    },
    'loggers': {
        'srecli': {
            'level': Env.LOG_LEVEL.get(),
            'handlers': ['null_handler'],
            'propagate': False,
        },
        'srecli.user': {
            'level': 'DEBUG',
            'handlers': ['user_handler'],
            'propagate': False
        },
    }
}

if folder := Env.LOGS_FOLDER.get():
    folder = Path(folder)
    if folder.is_dir():
        filename = str(folder / 'srecli.log')
    elif folder.is_file() or folder.suffix:
        folder.parent.mkdir(parents=True, exist_ok=True)
        filename = str(folder)
    else:
        folder.mkdir(parents=True, exist_ok=True)
        filename = str(folder / 'srecli.log')
    config['handlers']['file_handler'] = {
        'class': 'logging.handlers.TimedRotatingFileHandler',
        'formatter': 'base_formatter',
        'when': 'D',
        'interval': 1,
        'filename': filename
    }
    config['loggers']['srecli']['handlers'].append('file_handler')


logging.captureWarnings(capture=True)
logging.config.dictConfig(config)


def get_logger(name: str, level: str | None = None, /) -> logging.Logger:
    log = logging.getLogger(name)
    if level:
        log.setLevel(level)
    return log


def get_user_logger():
    """
    Colored logs to output some information to user
    """
    return logging.getLogger('srecli.user')


def enable_verbose_logs():
    """
    Just adds streaming handler to the main logger
    """
    logger = logging.getLogger('srecli')
    handler = logging.StreamHandler()
    handler.setFormatter(SensitiveFormatter(LOG_FORMAT))
    logger.addHandler(handler)
