"""
For saas just import this variable.
For on-prem it's important that all the necessary envs (at least MongoDB's)
are set before importing from here. Otherwise, it could lead to timeout or
an undesirable request to AWS.
"""

from helpers.constants import Env
from helpers.log_helper import get_logger
from services import SP

_LOG = get_logger(__name__)


class SystemCustomer:
    _name = None

    @classmethod
    def get_name(cls) -> str:
        if cls._name is None:
            cls._name = (
                Env.SYSTEM_CUSTOMER_NAME.get(None)
                or SP.settings_service.get_system_customer_name()
            )
            _LOG.info(f'System customer name was initialized: {cls._name}')
        return cls._name
