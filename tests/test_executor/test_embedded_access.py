"""Tests for embedded Lambda Environment.Error ACCESS classification."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from jmespath.exceptions import JMESPathTypeError

from executor.helpers.embedded_access import find_embedded_env_access_denied
from executor.job.policies.runners import AWSRunner
from helpers.constants import PolicyErrorType

_ACCESS_MSG = (
    'Lambda was unable to decrypt your environment variables because the '
    'KMS access was denied. Please check your KMS permissions. KMS Exception: '
    'AccessDeniedException KMS Message: User: arn:aws:sts::123:assumed-role/'
    'rule-engine-ami-role/i-abc is not authorized to perform: kms:Decrypt'
)


def _policy_with_cache(resources: list[dict] | None) -> MagicMock:
    cache = MagicMock()
    cache.__enter__ = MagicMock(return_value=cache)
    cache.__exit__ = MagicMock(return_value=False)
    cache.get = MagicMock(return_value=resources)

    source = MagicMock()
    source.get_query_params = MagicMock(return_value=None)

    rm = MagicMock()
    rm.source = source
    rm.get_cache_key = MagicMock(return_value={'resource': 'AWSLambda', 'q': None})
    rm._cache = cache

    policy = MagicMock()
    policy.resource_manager = rm
    return policy


def test_find_embedded_env_access_denied_returns_message() -> None:
    policy = _policy_with_cache(
        [
            {
                'FunctionName': 'ok',
                'Environment': {'Variables': {'A': 'plain'}},
            },
            {
                'FunctionName': 'denied',
                'Environment': {
                    'Error': {
                        'ErrorCode': 'AccessDeniedException',
                        'Message': _ACCESS_MSG,
                    }
                },
            },
        ]
    )
    assert find_embedded_env_access_denied(policy) == _ACCESS_MSG


def test_find_embedded_env_access_denied_normal_variables() -> None:
    policy = _policy_with_cache(
        [{'Environment': {'Variables': {'FOO': 'bar'}}}]
    )
    assert find_embedded_env_access_denied(policy) is None


def test_find_embedded_env_access_denied_empty_cache() -> None:
    assert find_embedded_env_access_denied(_policy_with_cache(None)) is None
    assert find_embedded_env_access_denied(_policy_with_cache([])) is None


def test_find_embedded_env_access_denied_cache_unavailable() -> None:
    policy = MagicMock()
    rm = MagicMock()
    rm.source.get_query_params.side_effect = RuntimeError('boom')
    policy.resource_manager = rm
    assert find_embedded_env_access_denied(policy) is None


def test_find_embedded_env_access_denied_falls_back_to_code() -> None:
    policy = _policy_with_cache(
        [
            {
                'Environment': {
                    'Error': {'ErrorCode': 'AccessDeniedException'},
                }
            }
        ]
    )
    assert find_embedded_env_access_denied(policy) == 'AccessDeniedException'


@patch(
    'executor.job.policies.runners.PoliciesLoader.get_policy_region',
    return_value='eu-west-2',
)
def test_aws_runner_classifies_embedded_access_as_access(
    _mock_region: MagicMock,
) -> None:
    access_msg = _ACCESS_MSG
    policy = _policy_with_cache(
        [
            {
                'Environment': {
                    'Error': {
                        'ErrorCode': 'AccessDeniedException',
                        'Message': access_msg,
                    }
                }
            }
        ]
    )
    policy.name = 'ecc-aws-460-lambda_environment_variables_encrypted_in_transit'
    policy.options = SimpleNamespace(region='eu-west-2')
    policy.side_effect = JMESPathTypeError(
        'values', None, 'null', ['object']
    )

    runner = AWSRunner([policy])
    assert runner._handle_errors(policy) is False

    key = ('eu-west-2', policy.name)
    assert key in runner.failed
    error_type, message, tb = runner.failed[key]
    assert error_type is PolicyErrorType.ACCESS
    assert message == access_msg
    assert tb == []


@patch(
    'executor.job.policies.runners.PoliciesLoader.get_policy_region',
    return_value='eu-west-2',
)
def test_aws_runner_unrelated_exception_stays_internal(
    _mock_region: MagicMock,
) -> None:
    policy = _policy_with_cache(
        [{'Environment': {'Variables': {'A': 'x'}}}]
    )
    policy.name = 'some-policy'
    policy.options = SimpleNamespace(region='eu-west-2')
    policy.side_effect = RuntimeError('unexpected boom')

    runner = AWSRunner([policy])
    assert runner._handle_errors(policy) is False

    key = ('eu-west-2', policy.name)
    error_type, message, tb = runner.failed[key]
    assert error_type is PolicyErrorType.INTERNAL
    assert 'unexpected boom' in (message or '')
    assert tb
