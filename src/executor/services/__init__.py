"""
This module contains executor specific code that should not be used inside
lambdas
"""

from functools import cached_property
from typing import TYPE_CHECKING

from helpers import SingletonMeta
from helpers.log_helper import get_logger
from services import SP

if TYPE_CHECKING:
    from executor.services.credentials_service import CredentialsService

_LOG = get_logger(__name__)


class BatchServiceProvider(metaclass=SingletonMeta):
    """
    Services that are specific to executor
    """

    @cached_property
    def credentials_service(self) -> 'CredentialsService':
        from executor.services.credentials_service import CredentialsService

        _LOG.debug('Creating CredentialsService')
        return CredentialsService(ssm_client=SP.ssm)


BSP = BatchServiceProvider()
