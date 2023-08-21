import gzip
import json
from pathlib import PurePosixPath, Path
from typing import Union, Optional

from helpers.constants import KEY_RULES_TO_STANDARDS, \
    KEY_RULES_TO_SEVERITY, KEY_HUMAN_DATA
from helpers.log_helper import get_logger
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService

_LOG = get_logger(__name__)


class S3SettingsService:
    SETTINGS_PREFIX = 'caas-settings'
    FILE_SUFFIX = '.json.gz'

    def __init__(self, s3_client: S3Client,
                 environment_service: EnvironmentService):
        self._s3 = s3_client
        self._environment_service = environment_service

    @property
    def bucket_name(self):
        return self._environment_service.rulesets_bucket_name()

    def key_from_name(self, key: str) -> str:
        if not key.endswith(self.FILE_SUFFIX):
            key = key.strip('.') + self.FILE_SUFFIX
        return str(PurePosixPath(self.SETTINGS_PREFIX, key))

    def name_from_key(self, key: str) -> str:
        return str(Path(key).name).rstrip(self.FILE_SUFFIX)

    def get(self, key: str,
            bucket_name: Optional[str] = None) -> Optional[Union[dict, str]]:
        key = self.key_from_name(key)
        obj = self._s3.get_file_stream(bucket_name or self.bucket_name, key)
        if not obj:
            return
        content = gzip.decompress(obj.read())
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            return content.decode()

    def rules_to_severity(self) -> dict:
        return self.get(KEY_RULES_TO_SEVERITY) or {}

    def rules_to_standards(self) -> dict:
        return self.get(KEY_RULES_TO_STANDARDS) or {}

    def human_data(self) -> dict:
        return self.get(KEY_HUMAN_DATA) or {}


class CachedS3SettingsService(S3SettingsService):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # not adjusted key to value
        self._cache = {}

    def get(self, key: str,
            bucket_name: Optional[str] = None) -> Optional[Union[dict, str]]:
        if key in self._cache:
            _LOG.info(f'Returning cached s3 setting by key {key}')
            return self._cache[key]
        _LOG.info(f'Getting s3 setting by key {key}')
        value = super().get(key, bucket_name)
        if not value:
            return
        self._cache[key] = value
        return value
