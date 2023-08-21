import gzip
import importlib
import json
from pathlib import PurePosixPath, Path
from typing import Union, Optional

from helpers.constants import KEY_AWS_EVENTS, KEY_GOOGLE_EVENTS, \
    KEY_AZURE_EVENTS, KEY_GOOGLE_STANDARDS_COVERAGE, KEY_RULES_TO_STANDARDS, \
    KEY_RULES_TO_SERVICE_SECTION, KEY_RULES_TO_MITRE, KEY_CLOUD_TO_RULES, \
    KEY_RULES_TO_SEVERITY, KEY_AZURE_STANDARDS_COVERAGE, \
    KEY_AWS_STANDARDS_COVERAGE, KEY_HUMAN_DATA
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService

# this setting is not used
S3_KEY_EVENT_BRIDGE_EVENT_SOURCE_TO_RULES_MAPPING = \
    'EVENT_BRIDGE_EVENT_SOURCE_TO_RULES_MAPPING'

S3_KEY_MAESTRO_SUBGROUP_ACTION_TO_AZURE_EVENTS_MAPPING = \
    'MAESTRO_SUBGROUP_ACTION_TO_AZURE_EVENTS_MAPPING'
S3_KEY_MAESTRO_SUBGROUP_ACTION_TO_GOOGLE_EVENTS_MAPPING = \
    'MAESTRO_SUBGROUP_ACTION_TO_GOOGLE_EVENTS_MAPPING'


class S3SettingsService:
    SETTINGS_PREFIX = 'caas-settings'
    FILE_SUFFIX = '.json.gz'

    def __init__(self, s3_client: S3Client,
                 environment_service: EnvironmentService):
        self._s3 = s3_client
        self._environment_service = environment_service

    @property
    def bucket_name(self):
        return self._environment_service.get_rulesets_bucket_name()

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

    def set(self, key: str, data: Union[dict, str],
            bucket_name: Optional[str] = None) -> None:
        data = json.dumps(data, separators=(',', ':')) if isinstance(
            data, dict) else data
        self._s3.put_object(bucket_name or self.bucket_name,
                            self.key_from_name(key), data)

    def ls(self, bucket_name: Optional[str] = None) -> list:
        keys = self._s3.list_dir(bucket_name or self.bucket_name,
                                 self.SETTINGS_PREFIX)
        return [self.name_from_key(key) for key in keys]

    def rules_to_service_section(self) -> Optional[dict]:
        return self.get(KEY_RULES_TO_SERVICE_SECTION)

    def rules_to_severity(self) -> Optional[dict]:
        return self.get(KEY_RULES_TO_SEVERITY)

    def rules_to_standards(self) -> Optional[dict]:
        return self.get(KEY_RULES_TO_STANDARDS)

    def rules_to_mitre(self) -> Optional[dict]:
        return self.get(KEY_RULES_TO_MITRE)

    def cloud_to_rules(self) -> Optional[dict]:
        return self.get(KEY_CLOUD_TO_RULES)

    def human_data(self) -> Optional[dict]:
        return self.get(KEY_HUMAN_DATA)

    def aws_standards_coverage(self) -> Optional[dict]:
        return self.get(KEY_AWS_STANDARDS_COVERAGE)

    def azure_standards_coverage(self) -> Optional[dict]:
        return self.get(KEY_AZURE_STANDARDS_COVERAGE)

    def google_standards_coverage(self) -> Optional[dict]:
        return self.get(KEY_GOOGLE_STANDARDS_COVERAGE)

    def aws_events(self) -> Optional[dict]:
        return self.get(KEY_AWS_EVENTS)

    def azure_events(self) -> Optional[dict]:
        return self.get(KEY_AZURE_EVENTS)

    def google_events(self) -> Optional[dict]:
        return self.get(KEY_GOOGLE_EVENTS)


# class CachedS3SettingsService(S3SettingsService):
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         # not adjusted key to value
#         self._cache = TTLCache(maxsize=30, ttl=900)
#
#     def get(self, key: str,
#             bucket_name: Optional[str] = None) -> Optional[Union[dict, str]]:
#         if key in self._cache:
#             return self._cache[key]
#         value = super().get(key, bucket_name)
#         if not value:
#             return
#         self._cache[key] = value
#         return value
#
#     def set(self, key: str, data: Union[dict, str],
#             bucket_name: Optional[str] = None) -> None:
#         super().set(key, data, bucket_name)
#         self._cache[key] = data


class S3SettingsServiceLocalWrapper:
    data_attr = 'data'

    def __init__(self, s3_setting_service: S3SettingsService):
        self._s3_setting_service = s3_setting_service

    def adjust_key(self, key: str) -> str:
        return self._s3_setting_service.name_from_key(key).lower()

    def _import(self, key: str) -> Optional[Union[dict, str]]:
        try:
            module = importlib.import_module(
                f'helpers.mappings.{self.adjust_key(key)}')
        except ImportError:
            return
        return getattr(module, self.data_attr, None)

    def get(self, key: str) -> Optional[Union[dict, str]]:
        imported = self._import(key)
        if imported:
            return imported
        return self._s3_setting_service.get(key)
