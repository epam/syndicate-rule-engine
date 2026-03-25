"""Cloud-specific policy runners. Execute Cloud Custodian policies per cloud."""

import traceback
from abc import ABC, abstractmethod

from azure.core.exceptions import ClientAuthenticationError
from botocore.exceptions import ClientError
from c7n.policy import Policy
from google.auth.exceptions import GoogleAuthError
from googleapiclient.errors import HttpError
from msrestazure.azure_exceptions import CloudError

from executor.helpers.constants import (
    ACCESS_DENIED_ERROR_CODE,
    INVALID_CREDENTIALS_ERROR_CODES,
)
from executor.job.policies.loader import PoliciesLoader
from helpers.constants import Cloud, PolicyErrorType
from helpers.log_helper import get_logger

_LOG = get_logger(__name__)


class Runner(ABC):
    cloud: Cloud

    def __init__(self, policies: list[Policy], failed: dict | None = None):
        self._policies = policies

        self.failed = failed if isinstance(failed, dict) else {}
        self.n_successful = 0

        self._err = None
        self._err_msg = None
        self._err_exc = None

    @classmethod
    def factory(
        cls, cloud: Cloud, policies: list[Policy], failed: dict | None = None
    ) -> 'Runner':
        _class = next(
            filter(lambda sub: sub.cloud == cloud, cls.__subclasses__())
        )
        return _class(policies, failed)

    def start(self):
        while self._policies:
            self._call_policy(policy=self._policies.pop())

    def _call_policy(self, policy: Policy):
        if self._err is None:
            self._handle_errors(policy)
            return
        _LOG.debug(
            'Some previous policy failed with error that will recur. '
            f'Skipping policy {policy.name}'
        )
        self._add_failed(
            region=PoliciesLoader.get_policy_region(policy),
            policy=policy.name,
            error_type=self._err,
            message=self._err_msg,
            exception=self._err_exc,
        )

    @staticmethod
    def add_failed(
        failed: dict,
        region: str,
        policy: str,
        error_type: PolicyErrorType,
        exception: Exception | None = None,
        message: str | None = None,
    ):
        tb = []
        if exception:
            te = traceback.TracebackException.from_exception(exception)
            tb.extend(te.format())
            if not message:
                message = ''.join(te.format_exception_only())
        failed[(region, policy)] = (error_type, message, tb)

    def _add_failed(
        self,
        region: str,
        policy: str,
        error_type: PolicyErrorType,
        exception: Exception | None = None,
        message: str | None = None,
    ):
        self.add_failed(
            self.failed, region, policy, error_type, exception, message
        )

    @abstractmethod
    def _handle_errors(self, policy: Policy): ...


class AWSRunner(Runner):
    cloud = Cloud.AWS

    def _handle_errors(self, policy: Policy):
        try:
            policy()
            self.n_successful += 1
        except ClientError as error:
            ec = error.response.get('Error', {}).get('Code')
            er = error.response.get('Error', {}).get('Message')

            if ec in ACCESS_DENIED_ERROR_CODE.get(self.cloud):
                _LOG.warning(
                    f"Policy '{policy.name}' is skipped. Reason: '{er}'"
                )
                self._add_failed(
                    region=PoliciesLoader.get_policy_region(policy),
                    policy=policy.name,
                    error_type=PolicyErrorType.ACCESS,
                    message=er,
                )
            elif ec in INVALID_CREDENTIALS_ERROR_CODES.get(self.cloud, ()):
                _LOG.warning(
                    f"Policy '{policy.name}' is skipped due to invalid "
                    f'credentials. All the subsequent rules will be skipped'
                )
                self._add_failed(
                    region=PoliciesLoader.get_policy_region(policy),
                    policy=policy.name,
                    error_type=PolicyErrorType.CREDENTIALS,
                    message=er,
                )
                self._err = PolicyErrorType.CREDENTIALS
                self._err_msg = er
            else:
                _LOG.warning(
                    f"Policy '{policy.name}' has failed. "
                    f'Client error occurred. '
                    f"Code: '{ec}'. "
                    f'Reason: {er}'
                )
                self._add_failed(
                    region=PoliciesLoader.get_policy_region(policy),
                    policy=policy.name,
                    error_type=PolicyErrorType.CLIENT,
                    exception=error,
                )
        except Exception as error:
            _LOG.exception(
                f'Policy {policy.name} has failed with unexpected error'
            )
            self._add_failed(
                region=PoliciesLoader.get_policy_region(policy),
                policy=policy.name,
                error_type=PolicyErrorType.INTERNAL,
                exception=error,
            )


class AZURERunner(Runner):
    cloud = Cloud.AZURE

    def _handle_errors(self, policy: Policy):
        try:
            policy()
            self.n_successful += 1
        except CloudError as error:
            ec = error.error
            er = error.message.split(':')[-1].strip()
            if ec in INVALID_CREDENTIALS_ERROR_CODES.get(self.cloud, ()):
                _LOG.warning(
                    f"Policy '{policy.name}' is skipped due to invalid "
                    f'credentials. All the subsequent rules will be skipped'
                )
                self._add_failed(
                    region=PoliciesLoader.get_policy_region(policy),
                    policy=policy.name,
                    error_type=PolicyErrorType.CREDENTIALS,
                    message=er,
                )
                self._err = PolicyErrorType.CREDENTIALS
                self._err_msg = er
            else:
                _LOG.warning(
                    f"Policy '{policy.name}' has failed. "
                    f'Client error occurred. '
                    f"Code: '{ec}'. "
                    f'Reason: {er}'
                )
                self._add_failed(
                    region=PoliciesLoader.get_policy_region(policy),
                    policy=policy.name,
                    error_type=PolicyErrorType.CLIENT,
                    exception=error,
                )
        except Exception as error:
            _LOG.exception(
                f'Policy {policy.name} has failed with unexpected error'
            )
            self._add_failed(
                region=PoliciesLoader.get_policy_region(policy),
                policy=policy.name,
                error_type=PolicyErrorType.INTERNAL,
                exception=error,
            )
        except SystemExit as error:
            if isinstance(error.__context__, ClientAuthenticationError):
                _LOG.warning(
                    f"Policy '{policy.name}' is skipped due to invalid "
                    f'credentials. All the subsequent rules will be skipped'
                )
                self._add_failed(
                    region=PoliciesLoader.get_policy_region(policy),
                    policy=policy.name,
                    error_type=PolicyErrorType.CREDENTIALS,
                    message=error.__context__.message,
                )
                self._err = PolicyErrorType.CREDENTIALS
                self._err_msg = error.__context__.message
            else:
                raise


class GCPRunner(Runner):
    cloud = Cloud.GOOGLE

    def _handle_errors(self, policy: Policy):
        try:
            policy()
            self.n_successful += 1
        except GoogleAuthError as error:
            error_reason = str(error.args[-1])
            _LOG.warning(
                f"Policy '{policy.name}' is skipped due to invalid "
                f'credentials. All the subsequent rules will be skipped'
            )
            self._add_failed(
                region=PoliciesLoader.get_policy_region(policy),
                policy=policy.name,
                error_type=PolicyErrorType.CREDENTIALS,
                message=error_reason,
            )
            self._err = PolicyErrorType.CREDENTIALS
            self._err_msg = error_reason
        except HttpError as error:
            if error.status_code == 403:
                self._add_failed(
                    region=PoliciesLoader.get_policy_region(policy),
                    policy=policy.name,
                    error_type=PolicyErrorType.ACCESS,
                    message=error.reason,
                )
            else:
                self._add_failed(
                    region=PoliciesLoader.get_policy_region(policy),
                    policy=policy.name,
                    error_type=PolicyErrorType.CLIENT,
                    exception=error,
                )
        except Exception as error:
            _LOG.exception(
                f'Policy {policy.name} has failed with unexpected error'
            )
            self._add_failed(
                region=PoliciesLoader.get_policy_region(policy),
                policy=policy.name,
                error_type=PolicyErrorType.INTERNAL,
                exception=error,
            )


class K8SRunner(Runner):
    cloud = Cloud.KUBERNETES

    def _handle_errors(self, policy: Policy):
        try:
            policy()
            self.n_successful += 1
        except Exception as error:
            _LOG.exception(
                f'Policy {policy.name} has failed with unexpected error'
            )
            self._add_failed(
                region=PoliciesLoader.get_policy_region(policy),
                policy=policy.name,
                error_type=PolicyErrorType.INTERNAL,
                exception=error,
            )
