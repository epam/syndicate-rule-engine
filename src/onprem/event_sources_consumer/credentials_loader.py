"""
Loads credentials for event sources (AWS cross-account, assume role, etc.) from SSM/Vault.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from helpers.log_helper import get_logger

if TYPE_CHECKING:
    from modular_sdk.services.ssm_service import SSMClientCachingWrapper
    from services.clients.sts import StsClient

_LOG = get_logger(__name__)


def get_credentials(
    ssm: SSMClientCachingWrapper,
    secret_name: str | None,
    role_arn: str | None = None,
    sts: StsClient | None = None,
) -> dict | None:
    """
    Load credentials for SQS access.

    Priority:
    1. secret_name -> load static credentials from SSM
    2. role_arn + sts -> assume role (uses instance profile as base)
    3. None -> caller uses boto3 default chain (instance profile, env vars)
    """
    if secret_name:
        try:
            value = ssm.get_parameter(secret_name)
            if isinstance(value, dict):
                return value
        except Exception as e:
            _LOG.warning(
                "Failed to load credentials from %s: %s",
                secret_name,
                e,
            )
        return None  # secret load failed or invalid format

    if role_arn and sts:
        try:
            result = sts.assume_role(
                role_arn=role_arn,
                role_session_name="sre-event-sources-consumer",
            )
            creds = result.get("Credentials", {})
            return {
                "aws_access_key_id": creds.get("AccessKeyId"),
                "aws_secret_access_key": creds.get("SecretAccessKey"),
                "aws_session_token": creds.get("SessionToken"),
                "_expires_at": creds.get("Expiration"),  # for refresh logic
            }
        except Exception as e:
            _LOG.warning(
                "Failed to assume role %s: %s",
                role_arn,
                e,
            )
            return None

    return None
