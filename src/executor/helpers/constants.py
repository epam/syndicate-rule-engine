"""
Executor specific constants
"""

from enum import Enum

from typing_extensions import Self

from helpers.constants import Cloud


ENV_AWS_DEFAULT_REGION = 'AWS_DEFAULT_REGION'

ENV_GOOGLE_APPLICATION_CREDENTIALS = 'GOOGLE_APPLICATION_CREDENTIALS'
ENV_CLOUDSDK_CORE_PROJECT = 'CLOUDSDK_CORE_PROJECT'

AWS_DEFAULT_REGION = 'us-east-1'


INVALID_CREDENTIALS_ERROR_CODES = {
    Cloud.AWS: {
        'AuthFailure',
        'InvalidToken',
        'UnrecognizedClientException',
        'ExpiredToken',
        'ExpiredTokenException',
    },  # add 'InvalidClientTokenId'
    Cloud.AZURE: {
        'InvalidAuthenticationTokenTenant',
        'AuthorizationFailed',
        'ClientAuthenticationError',
        'Azure Error: AuthorizationFailed',
    },
    Cloud.GOOGLE: set(),
}
ACCESS_DENIED_ERROR_CODE = {
    Cloud.AWS: {
        'AccessDenied',
        'AccessDeniedException',
        'UnauthorizedOperation',
        'AuthorizationError',
        'AccessDeniedException, AccessDeniedException',
    },
    Cloud.AZURE: set(),
    Cloud.GOOGLE: set(),
}

CACHE_FILE = 'cloud-custodian.cache'


class ExecutorError(str, Enum):
    """
    Explanation for user why the job failed
    """

    reason: str | None

    def __new__(cls, value: str, reason: str | None = None):
        obj = str.__new__(cls, value)
        obj._value_ = value

        obj.reason = reason
        return obj

    METADATA_UPDATE_FAILED = 'Failed to update metadata'
    TENANT_NOT_FOUND = 'Tenant not found'
    NO_SUCCESSFUL_POLICIES = 'All policies have failed'  # exit code 1
    LM_DID_NOT_ALLOW = 'License manager did not allow this job'  # exit code 2
    NO_CREDENTIALS = 'Could not resolve any credentials'
    TIMEOUT = 'Task timeout exceeded'
    INTERNAL = 'Internal executor error'

    @classmethod
    def with_reason(
        cls,
        value: Self | str,
        reason: str,
    ) -> Self:
        """
        Creates a new ExecutorError with the given value and reason.
        """
        return cls(value, reason)

    def get_reason(self) -> str:
        """
        Returns the reason for the error.
        """
        if not self.reason:
            return self.value
        return f'{self.value}: {self.reason}'
