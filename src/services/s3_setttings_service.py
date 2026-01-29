from enum import Enum
from pathlib import PurePosixPath
from typing import Any, cast

from helpers.constants import APP_NAME
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService


class S3SettingsKey(str, Enum):
    """
    Keys for S3 settings.
    """

    AWS_EVENTS = "AWS_EVENTS"
    AZURE_EVENTS = "AZURE_EVENTS"
    GOOGLE_EVENTS = "GOOGLE_EVENTS"


class S3SettingsService:
    """
    Service to manage settings in S3.
    """

    SETTINGS_PREFIX = f"{APP_NAME}-settings"
    FILE_SUFFIX = ".json.gz"

    def __init__(
        self,
        s3_client: S3Client,
        environment_service: EnvironmentService,
    ):
        self._s3 = s3_client
        self._env = environment_service

    @property
    def bucket_name(self) -> str:
        return self._env.get_rulesets_bucket_name()

    def key_from_name(self, name: str) -> str:
        if not name.endswith(self.FILE_SUFFIX):
            name = name.strip(".") + self.FILE_SUFFIX
        return str(PurePosixPath(self.SETTINGS_PREFIX, name))

    def name_from_key(self, key: str) -> str:
        return str(PurePosixPath(key).name).strip(self.FILE_SUFFIX)

    def get(
        self,
        key: S3SettingsKey | str,
        bucket_name: str | None = None,
    ) -> dict[str, Any]:
        if isinstance(key, S3SettingsKey):
            key = key.value
        bucket_name = bucket_name or self.bucket_name
        key_name = self.key_from_name(key)

        return cast(
            dict[str, Any],
            self._s3.gz_get_json(
                bucket=bucket_name,
                key=key_name,
            ),
        )

    def set(
        self,
        key: S3SettingsKey | str,
        value: dict[str, Any],
        bucket_name: str | None = None,
    ) -> None:
        if isinstance(key, S3SettingsKey):
            key = key.value
        bucket_name = bucket_name or self.bucket_name
        key_name = self.key_from_name(key)

        self._s3.gz_put_json(
            bucket=bucket_name,
            key=key_name,
            obj=value,
        )

    # @property
    # def aws_events(self) -> dict[str, Any]:
    #     """get AWS events mapping."""
    #     return self.get(S3SettingsKey.AWS_EVENTS)

    # @aws_events.setter
    # def aws_events(self, value: dict[str, Any]) -> None:
    #     """set AWS events mapping."""
    #     self.set(S3SettingsKey.AWS_EVENTS, value)

    # @property
    # def azure_events(self) -> dict[str, Any]:
    #     """get Azure events mapping."""
    #     return self.get(S3SettingsKey.AZURE_EVENTS)

    # @azure_events.setter
    # def azure_events(self, value: dict[str, Any]) -> None:
    #     """set Azure events mapping."""
    #     self.set(S3SettingsKey.AZURE_EVENTS, value)

    # @property
    # def google_events(self) -> dict[str, Any]:
    #     """get Google events mapping."""
    #     return self.get(S3SettingsKey.GOOGLE_EVENTS)

    # @google_events.setter
    # def google_events(self, value: dict[str, Any]) -> None:
    #     """set Google events mapping."""
    #     self.set(S3SettingsKey.GOOGLE_EVENTS, value)
