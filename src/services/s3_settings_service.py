import importlib
import json
from pathlib import PurePosixPath

from helpers.constants import S3SettingKey
from services.clients.s3 import S3Client
from helpers.log_helper import get_logger
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
        return self._environment_service.get_rulesets_bucket_name()

    def key_from_name(self, key: str) -> str:
        if not key.endswith(self.FILE_SUFFIX):
            key = key.strip('.') + self.FILE_SUFFIX
        return str(PurePosixPath(self.SETTINGS_PREFIX, key))

    def name_from_key(self, key: str) -> str:
        return str(PurePosixPath(key).name).rstrip(self.FILE_SUFFIX)

    def get(self, key: S3SettingKey | str,
            bucket_name: str = None) -> dict:
        if isinstance(key, S3SettingKey):
            key = key.value
        return self._s3.gz_get_json(
            bucket=bucket_name or self.bucket_name,
            key=self.key_from_name(key)
        )

    def set(self, key: S3SettingKey | str, data: dict,
            bucket_name: str = None):
        if isinstance(key, S3SettingKey):
            key = key.value
        if key in (S3SettingKey.AWS_STANDARDS_COVERAGE,
                   S3SettingKey.AZURE_STANDARDS_COVERAGE,
                   S3SettingKey.GOOGLE_STANDARDS_COVERAGE):
            # kludge for a while because somehow these settings can have None
            # as json keys. Such json is not valid, so we convert it to "null".
            # json.dumps does it
            _LOG.warning('Dumping standards dict converting None keys to '
                         '"null"')
            self._s3.gz_put_object(
                bucket=bucket_name or self.bucket_name,
                key=self.key_from_name(key),
                body=json.dumps(data, separators=(',', ':')).encode()
            )
        else:
            self._s3.gz_put_json(
                bucket=bucket_name or self.bucket_name,
                key=self.key_from_name(key),
                obj=data
            )

    def ls(self, bucket_name: str = None) -> list:
        keys = self._s3.list_dir(bucket_name or self.bucket_name,
                                 self.SETTINGS_PREFIX)
        return [self.name_from_key(key) for key in keys]

    def rules_to_service_section(self) -> dict:
        return self.get(S3SettingKey.RULES_TO_SERVICE_SECTION)

    def rules_to_severity(self) -> dict:
        return self.get(S3SettingKey.RULES_TO_SEVERITY)

    def rules_to_standards(self) -> dict:
        return self.get(S3SettingKey.RULES_TO_STANDARDS)

    def rules_to_mitre(self) -> dict:
        return self.get(S3SettingKey.RULES_TO_MITRE)

    def cloud_to_rules(self) -> dict:
        return self.get(S3SettingKey.CLOUD_TO_RULES)

    def human_data(self) -> dict:
        return self.get(S3SettingKey.HUMAN_DATA)

    def rules_to_service(self) -> dict:
        return self.get(S3SettingKey.RULES_TO_SERVICE)

    def rules_to_category(self) -> dict:
        return self.get(S3SettingKey.RULES_TO_CATEGORY)

    def aws_standards_coverage(self) -> dict:
        return self.get(S3SettingKey.AWS_STANDARDS_COVERAGE)

    def azure_standards_coverage(self) -> dict:
        return self.get(S3SettingKey.AZURE_STANDARDS_COVERAGE)

    def google_standards_coverage(self) -> dict:
        return self.get(S3SettingKey.GOOGLE_STANDARDS_COVERAGE)

    def aws_events(self) -> dict:
        return self.get(S3SettingKey.AWS_EVENTS)

    def azure_events(self) -> dict:
        return self.get(S3SettingKey.AZURE_EVENTS)

    def google_events(self) -> dict:
        return self.get(S3SettingKey.GOOGLE_EVENTS)


class S3SettingsServiceLocalWrapper:
    data_attr = 'data'

    def __init__(self, s3_setting_service: S3SettingsService):
        self._s3_setting_service = s3_setting_service

    def adjust_key(self, key: str) -> str:
        return self._s3_setting_service.name_from_key(key).lower()

    def _import(self, key: str) -> dict | None:
        try:
            module = importlib.import_module(
                f'helpers.mappings.{self.adjust_key(key)}')
        except ImportError:
            return
        return getattr(module, self.data_attr, None)

    def get(self, key: S3SettingKey) -> dict:
        imported = self._import(key.value)
        if imported:
            return imported
        return self._s3_setting_service.get(key)
