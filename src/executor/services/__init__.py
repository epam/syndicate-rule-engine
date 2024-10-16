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
    from executor.services.environment_service import BatchEnvironmentService
    from executor.services.policy_service import PoliciesService

_LOG = get_logger(__name__)


class BatchServiceProvider(metaclass=SingletonMeta):
    """
    Services that are specific to executor
    """

    @cached_property
    def credentials_service(self) -> 'CredentialsService':
        from executor.services.credentials_service import CredentialsService
        _LOG.debug('Creating CredentialsService')
        return CredentialsService(
            ssm_client=SP.ssm,
            environment_service=self.environment_service,
        )

    @cached_property
    def environment_service(self) -> 'BatchEnvironmentService':
        from executor.services.environment_service import \
            BatchEnvironmentService
        _LOG.debug('Creating EnvironmentService')
        return BatchEnvironmentService()

    @property
    def env(self) -> 'BatchEnvironmentService':  # alias
        return self.environment_service

    # @cached_property
    # def notification_service(self) -> 'NotificationService':
    #     _LOG.debug('Creating NotificationService')
    #     return NotificationService(
    #         setting_service=SP.settings_service,
    #         ssm_client=SP.ssm,
    #         s3_client=SP.s3
    #     )

    @cached_property
    def policies_service(self) -> 'PoliciesService':
        from executor.services.policy_service import PoliciesService
        _LOG.debug('Creating PoliciesService')
        return PoliciesService(ruleset_service=SP.ruleset_service,
                               environment_service=self.environment_service)


BSP = BatchServiceProvider()  # stands for Batch service provider
