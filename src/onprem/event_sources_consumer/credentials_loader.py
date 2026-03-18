"""
Loads credentials for event sources (AWS cross-account, etc.) from SSM/Vault.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from helpers.log_helper import get_logger

if TYPE_CHECKING:
    from modular_sdk.services.ssm_service import SSMClientCachingWrapper

_LOG = get_logger(__name__)


def get_credentials(
    ssm: SSMClientCachingWrapper,
    secret_name: str | None,
) -> dict | None:
    """
    Load credentials from SSM/Vault by secret name.
    Returns dict with aws_access_key_id, aws_secret_access_key or None
    if no secret or same-account (use IRSA/default).
    """
    if not secret_name:
        return None
    try:
        value = ssm.get_parameter(secret_name)
        if isinstance(value, dict):
            return value
        return None
    except Exception as e:
        _LOG.warning(
            "Failed to load credentials from %s: %s",
            secret_name,
            e,
        )
        return None
