import json
import logging
import logging.config
from datetime import datetime, timezone
from typing import TypeVar

from helpers.constants import CAASEnv
from modular_sdk.commons.constants import Env as ModularSDKEnv

LOG_FORMAT = (
    '%(asctime)s %(levelname)s %(name)s.%(funcName)s:%(lineno)d %(message)s'
)
# there is no root module so just make up this ephemeral module
ROOT_MODULE = 'rule_engine'


class CustomFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        if datefmt is not None:
            return super().formatTime(record, datefmt)
        return datetime.fromtimestamp(record.created, timezone.utc).isoformat()


LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'console_formatter': {'format': LOG_FORMAT, '()': CustomFormatter}
    },
    'handlers': {
        'console_handler': {
            'class': 'logging.StreamHandler',
            'formatter': 'console_formatter',
        }
    },
    'loggers': {
        ROOT_MODULE: {
            'level': CAASEnv.LOG_LEVEL.get(),
            'handlers': ['console_handler'],
            'propagate': False,
        },
        'custodian': {  # Cloud Custodian logger
            'level': CAASEnv.LOG_LEVEL.get(),
            'handlers': ['console_handler'],
            'propagate': False,
        },
        'modular_sdk': {
            'level': ModularSDKEnv.LOG_LEVEL.get(),
            'handlers': ['console_handler'],
            'propagate': False,
        },
    },
}


def setup_logging():
    # Importing here to prevent modular_sdk from overriding our logging conf
    import modular_sdk.commons.log_helper  # noqa

    logging.config.dictConfig(LOGGING_CONFIG)


def get_logger(name: str, level: str | None = None, /):
    log = logging.getLogger(ROOT_MODULE).getChild(name)
    if level:
        log.setLevel(level)
    return log


SECRET_KEYS = {
    'refresh_token',
    'id_token',
    'password',
    'authorization',
    'secret',
    'AWS_SECRET_ACCESS_KEY',
    'AWS_SESSION_TOKEN',
    'git_access_secret',
    'api_key',
    'AZURE_CLIENT_ID',
    'AZURE_CLIENT_SECRET',
    'GOOGLE_APPLICATION_CREDENTIALS',
    'private_key',
    'private_key_id',
    'Authorization',
    'Authentication',
    'certificate',
}

JT = TypeVar('JT')  # json type


def hide_secret_values(
    obj: JT, secret_keys: set[str] | None = None, replacement: str = '****'
) -> JT:
    if not secret_keys:
        secret_keys = SECRET_KEYS
    match obj:
        case dict():
            res = {}
            for k, v in obj.items():
                if k in secret_keys:
                    res[k] = replacement
                else:
                    res[k] = hide_secret_values(v, secret_keys, replacement)
            return res
        case list():
            return [
                hide_secret_values(v, secret_keys, replacement) for v in obj
            ]
        case str():
            try:
                return hide_secret_values(
                    json.loads(obj), secret_keys, replacement
                )
            except json.JSONDecodeError:
                return obj
        case _:
            return obj


setup_logging()
