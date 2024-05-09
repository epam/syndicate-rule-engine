"""
For saas just import this variable.
For on-prem it's important that all the necessary envs (at least MongoDB's)
are set before importing from here. Otherwise, it could lead to timeout or
an undesirable request to AWS.
"""
import os

from helpers.constants import CAASEnv
from helpers.log_helper import get_logger
from services import SERVICE_PROVIDER
from typing import Final

_LOG = get_logger(__name__)

if CAASEnv.SYSTEM_CUSTOMER_NAME in os.environ:
    SYSTEM_CUSTOMER: Final[str] = os.getenv(CAASEnv.SYSTEM_CUSTOMER_NAME)
else:
    SYSTEM_CUSTOMER: Final[str] = SERVICE_PROVIDER.settings_service.get_system_customer_name()  # noqa
_LOG.info(f'SYSTEM Customer name: \'{SYSTEM_CUSTOMER}\'')
