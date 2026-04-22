"""Cloud-specific policy runners. Execute Cloud Custodian policies per cloud."""

import traceback
from abc import ABC, abstractmethod
from collections.abc import Sequence
from pathlib import Path
from typing_extensions import Self

import msgspec.json
from azure.core.exceptions import ClientAuthenticationError
from botocore.exceptions import ClientError
from c7n.exceptions import PolicyValidationError
from c7n.policy import Policy
from google.auth.exceptions import GoogleAuthError
from googleapiclient.errors import HttpError
from msrestazure.azure_exceptions import CloudError

from executor.helpers.constants import (
    ACCESS_DENIED_ERROR_CODE,
    INVALID_CREDENTIALS_ERROR_CODES,
)
from executor.job.policies.loader import PoliciesLoader
from services.job_policy_filters import apply_scan_entry
from helpers.constants import Cloud, PolicyErrorType
from helpers.log_helper import get_logger
from services.job_policy_filters.types import BundleFilters, PolicyScanEntry

_LOG = get_logger(__name__)


class Runner(ABC):
    cloud: Cloud

    def __init__(
        self,
        policies: list[Policy],
        failed: dict | None = None,
        *,
        policy_bundle: BundleFilters | None = None,
    ) -> None:
        self._policies = policies

        self.failed = failed if isinstance(failed, dict) else {}
        self.n_successful = 0
        self._policy_bundle = policy_bundle

        self._err = None
        self._err_msg = None
        self._err_exc = None

    @classmethod
    def factory(
        cls,
        cloud: Cloud,
        policies: list[Policy],
        failed: dict | None = None,
        *,
        policy_bundle: BundleFilters | None = None,
    ) -> Self:
        _class = next(
            filter(lambda sub: sub.cloud == cloud, cls.__subclasses__())
        )
        return _class(policies, failed, policy_bundle=policy_bundle)

    def start(self) -> None:
        while self._policies:
            self._call_policy(policy=self._policies.pop())

    def _call_policy(self, policy: Policy) -> None:
        if self._err is not None:
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
            return

        bundle_entries = (
            self._policy_bundle.get(policy.name)
            if self._policy_bundle is not None
            else None
        )
        if bundle_entries:
            self._run_with_bundle_entries(policy, bundle_entries)
            return

        success = self._handle_errors(policy)
        if not success:
            return

        self.n_successful += 1

    @staticmethod
    def add_failed(
        failed: dict,
        region: str,
        policy: str,
        error_type: PolicyErrorType,
        exception: Exception | None = None,
        message: str | None = None,
    ) -> None:
        tb = []
        if exception:
            te = traceback.TracebackException.from_exception(exception)
            tb.extend(te.format())
            if not message:
                message = ''.join(te.format_exception_only())
        if isinstance(message, str):
            message = message.rstrip('\r\n')
        failed[(region, policy)] = (error_type, message, tb)

    def _add_failed(
        self,
        region: str,
        policy: str,
        error_type: PolicyErrorType,
        exception: Exception | None = None,
        message: str | None = None,
    ) -> None:
        self.add_failed(
            self.failed, region, policy, error_type, exception, message
        )

    def _run_with_bundle_entries(
        self, policy: Policy, entries: Sequence[PolicyScanEntry]
    ) -> None:
        region = PoliciesLoader.get_policy_region(policy)
        merged_resources: list[dict] = []

        for entry in entries:
            try:
                data = apply_scan_entry(policy.data, entry)
                run_pol = Policy(
                    data=data,
                    options=policy.options,
                    session_factory=policy.session_factory,
                )
                run_pol.expand_variables(run_pol.get_variables())
                run_pol.validate()
            except (PolicyValidationError, ValueError):
                _LOG.warning(
                    'Policy %s bundle scan validation failed',
                    policy.name,
                    exc_info=True,
                )
                self._add_failed(
                    region=region,
                    policy=policy.name,
                    error_type=PolicyErrorType.INTERNAL,
                )
                return
            except Exception as error:
                _LOG.exception(
                    'Policy %s bundle scan could not be prepared',
                    policy.name,
                )
                self._add_failed(
                    region=region,
                    policy=policy.name,
                    error_type=PolicyErrorType.INTERNAL,
                    exception=error,
                )
                return

            success = self._handle_errors(run_pol)
            if not success:
                return

            out_base = Path(run_pol.options.output_dir) / run_pol.name
            res_file = out_base / 'resources.json'
            if res_file.is_file():
                raw = res_file.read_bytes()
                if raw.strip():
                    chunk = msgspec.json.decode(raw)
                    if isinstance(chunk, list):
                        merged_resources.extend(chunk)
                    elif isinstance(chunk, dict):
                        merged_resources.append(chunk)

        # TODO: may be make sense add deduplication here,
        #  but duplicates are not allowed at this time
        final_dir = Path(policy.options.output_dir) / policy.name
        final_dir.mkdir(parents=True, exist_ok=True)
        with open(final_dir / 'resources.json', 'wb') as fp:
            fp.write(msgspec.json.encode(merged_resources))

        self.n_successful += 1

    @abstractmethod
    def _handle_errors(self, policy: Policy) -> bool: ...


class AWSRunner(Runner):
    cloud = Cloud.AWS

    def _handle_errors(self, policy: Policy) -> bool:
        try:
            policy()
            return True
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
        return False


class AZURERunner(Runner):
    cloud = Cloud.AZURE

    def _handle_errors(self, policy: Policy) -> bool:
        try:
            policy()
            return True
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
        return False


class GCPRunner(Runner):
    cloud = Cloud.GOOGLE

    def _handle_errors(self, policy: Policy) -> bool:
        try:
            policy()
            return True
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
        return False


class K8SRunner(Runner):
    cloud = Cloud.KUBERNETES

    def _handle_errors(self, policy: Policy) -> bool:
        try:
            policy()
            return True
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
        return False

