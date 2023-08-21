from typing import Union, Optional

from cachetools import TTLCache
from modular_sdk.services.impl.maestro_credentials_service import AccessMeta
from pynamodb.exceptions import PynamoDBException

from helpers.constants import DEFAULT_SYSTEM_CUSTOMER, \
    DEFAULT_METRICS_BUCKET_NAME, \
    DEFAULT_TEMPLATES_BUCKET_NAME, DEFAULT_STATISTICS_BUCKET_NAME, \
    DEFAULT_RULESETS_BUCKET_NAME, DEFAULT_REPORTS_BUCKET_NAME, \
    DEFAULT_SSM_BACKUP_BUCKET_NAME, KEY_AWS_EVENTS, KEY_GOOGLE_EVENTS, \
    KEY_AZURE_EVENTS, KEY_GOOGLE_STANDARDS_COVERAGE, KEY_RULES_TO_STANDARDS, \
    KEY_RULES_TO_SERVICE_SECTION, KEY_RULES_TO_MITRE, KEY_CLOUD_TO_RULES, \
    KEY_RULES_TO_SEVERITY, KEY_AZURE_STANDARDS_COVERAGE, \
    KEY_AWS_STANDARDS_COVERAGE
from helpers.log_helper import get_logger
from models.setting import Setting
from services.environment_service import EnvironmentService

KEY_SYSTEM_CUSTOMER = 'SYSTEM_CUSTOMER_NAME'
KEY_BACKUP_REPO_CONFIGURATION = 'BACKUP_REPO_ACCESS_INFO'
KEY_STATS_S3_BUCKET_NAME = 'STATS_S3_BUCKET_NAME'
KEY_TEMPLATE_BUCKET = 'TEMPLATES_S3_BUCKET_NAME'
KEY_CURRENT_CUSTODIAN_CUSTOM_CORE_VERSION = 'CURRENT_CCC_VERSION'
KEY_ACCESS_DATA_LM = 'ACCESS_DATA_LM'
KEY_MAIL_CONFIGURATION = 'MAIL_CONFIGURATION'
KEY_CONTACTS = 'CONTACTS'
KEY_LM_CLIENT_KEY = 'LM_CLIENT_KEY'
KEY_EVENT_ASSEMBLER = 'EVENT_ASSEMBLER'
KEY_REPORT_DATE_MARKER = 'REPORT_DATE_MARKER'
KEY_RULES_METADATA_REPO_ACCESS_SSM_NAME = 'RULES_METADATA_REPO_ACCESS_SSM_NAME'
DEFAULT_RULES_METADATA_REPO_ACCESS_SSM_NAME = \
    'custodian.rules-metadata-repo-access'

KEY_BUCKET_NAMES = 'BUCKET_NAMES'
RULESETS_BUCKET = 'rulesets'
REPORTS_BUCKET = 'reports'
SSM_BACKUP_BUCKET = 'ssm-backup'
STATISTICS_BUCKET = 'statistics'
TEMPLATES_BUCKET = 'templates'
METRICS_BUCKET = 'metrics'

EVENT_CURSOR_TIMESTAMP_ATTR = 'ect'

_LOG = get_logger(__name__)


class SettingsService:
    def __init__(self, environment_service: EnvironmentService):
        self._environment = environment_service

    @staticmethod
    def get_all_settings():
        return Setting.scan()

    def get_backup_repo_settings(self):
        return self.get(name=KEY_BACKUP_REPO_CONFIGURATION)

    @staticmethod
    def create(name, value) -> Setting:
        return Setting(name=name, value=value)

    def get(self, name, value: bool = True) -> Optional[Union[Setting, dict]]:
        _LOG.debug(f'Querying {name} setting')
        setting = Setting.get_nullable(hash_key=name)
        if setting and value:
            return setting.value
        elif setting:
            return setting

    def delete(self, setting: Union[Setting, str]) -> bool:
        setting = setting if isinstance(setting, Setting) else \
            Setting.get_nullable(hash_key=setting)
        if setting:
            setting.delete()
            return True
        return False

    def save(self, setting: Setting):
        return setting.save()

    def get_current_ccc_version(self):
        return self.get(
            name=KEY_CURRENT_CUSTODIAN_CUSTOM_CORE_VERSION)

    def get_license_manager_access_data(self, value: bool = True):
        return self.get(name=KEY_ACCESS_DATA_LM, value=value)

    def create_license_manager_access_data_configuration(
            self, host: str,
            port: Optional[int] = None,
            protocol: Optional[str] = None,
            stage: Optional[str] = None) -> Setting:
        model = AccessMeta.from_dict({})
        model.update_host(host=host, port=port, protocol=protocol, stage=stage)
        return self.create(
            name=KEY_ACCESS_DATA_LM, value=model.dict()
        )

    def get_license_manager_client_key_data(self, value: bool = True):
        return self.get(name=KEY_LM_CLIENT_KEY, value=value)

    def create_license_manager_client_key_data(self, kid: str, alg: str
                                               ) -> Setting:
        """
        :param kid: str, id of a key, delegated by the License Manager
        :param alg: str, algorithm to use with a key,
        delegated by the License Manager

        Note: kid != id of a key within a persistence, such as parameter store.
        Ergo, kid is used to derive reference to the persisted data.
        """
        return self.create(
            name=KEY_LM_CLIENT_KEY, value=dict(
                kid=kid,
                alg=alg
            )
        )

    def create_mail_configuration(
            self, username: str, password_alias: str, default_sender: str,
            host: str, port: int, use_tls: bool,
            max_emails: Optional[int] = None
    ):
        return self.create(
            name=KEY_MAIL_CONFIGURATION, value=dict(
                username=username,
                password=password_alias,
                default_sender=default_sender,
                host=host, port=port,
                max_emails=max_emails,
                use_tls=use_tls
            )
        )

    def get_mail_configuration(self, value: bool = True
                               ) -> Optional[Union[Setting, dict]]:
        return self.get(
            name=KEY_MAIL_CONFIGURATION, value=value
        )

    def get_system_customer_name(self) -> str:
        """
        Returns the name of SYSTEM customer. If the setting is not found,
        default system customer name is returned.
        """
        if self._environment.is_testing():
            _LOG.info('Testing mode. Returning default system customer name')
            return DEFAULT_SYSTEM_CUSTOMER
        _LOG.info('Querying CaaSSettings in order to get SYSTEM Customer name')
        name: Optional[str] = None
        try:
            name = self.get(KEY_SYSTEM_CUSTOMER)
        except PynamoDBException as e:
            _LOG.warning(f'Could not query {KEY_SYSTEM_CUSTOMER} setting: {e}.'
                         f' Using the default SYSTEM customer name')
        except Exception as e:
            _LOG.warning(f'Unexpected error occurred trying querying '
                         f'{KEY_SYSTEM_CUSTOMER} setting: {e}. Using the '
                         f'default SYSTEM customer name')
        return name or DEFAULT_SYSTEM_CUSTOMER

    def create_event_assembler_configuration(self, cursor: float) -> Setting:
        return self.create(
            name=KEY_EVENT_ASSEMBLER, value={
                EVENT_CURSOR_TIMESTAMP_ATTR: cursor
            }
        )

    def get_event_assembler_configuration(self, value: bool = True
                                          ) -> Optional[Union[Setting, dict]]:
        return self.get(name=KEY_EVENT_ASSEMBLER, value=value)

    def get_template_bucket(self):
        bucket = self.get(name=KEY_TEMPLATE_BUCKET)
        return bucket if bucket else ''

    def get_custodian_contacts(self):
        contacts = self.get(name=KEY_CONTACTS)
        return contacts if contacts else ''

    def get_report_date_marker(self) -> dict:
        marker = self.get(name=KEY_REPORT_DATE_MARKER)
        return marker or {}

    def set_report_date_marker(self, current_week_date: str = None,
                               last_week_date: str = None):
        marker = self.get(name=KEY_REPORT_DATE_MARKER)
        if current_week_date:
            marker.update({'current_week_date': current_week_date})
        if last_week_date:
            marker.update({'last_week_date': last_week_date})
        new_marker = self.create(name=KEY_REPORT_DATE_MARKER, value=marker)
        new_marker.save()

    def get_bucket_names(self) -> dict:
        """
        Such a dict is returned. Default values is used in case the
        values are not set
        {
            "rulesets": "",
            "reports": "",
            "ssm-backup": "",
            "statistics": "",
            "templates": "",
            "metrics": ""
        }
        :return:
        """
        value = self.get(KEY_BUCKET_NAMES) or {}
        value.setdefault(RULESETS_BUCKET, DEFAULT_RULESETS_BUCKET_NAME)
        value.setdefault(REPORTS_BUCKET, DEFAULT_REPORTS_BUCKET_NAME)
        value.setdefault(STATISTICS_BUCKET, DEFAULT_STATISTICS_BUCKET_NAME)
        value.setdefault(TEMPLATES_BUCKET, DEFAULT_TEMPLATES_BUCKET_NAME)
        value.setdefault(METRICS_BUCKET, DEFAULT_METRICS_BUCKET_NAME)
        value.setdefault(SSM_BACKUP_BUCKET, DEFAULT_SSM_BACKUP_BUCKET_NAME)
        return value

    def rulesets_bucket(self) -> str:
        return self.get_bucket_names().get(RULESETS_BUCKET)

    def reports_bucket(self) -> str:
        return self.get_bucket_names().get(REPORTS_BUCKET)

    def statistics_bucket(self) -> str:
        return self.get_bucket_names().get(REPORTS_BUCKET)

    def templates_bucket(self) -> str:
        return self.get_bucket_names().get(TEMPLATES_BUCKET)

    def metrics_bucket(self) -> str:
        return self.get_bucket_names().get(METRICS_BUCKET)

    def ssm_backup_bucket(self) -> str:
        return self.get_bucket_names().get(SSM_BACKUP_BUCKET)

    # metadata
    def rules_metadata_repo_access_data(self) -> str:
        """
        Returns the name of ssm parameter which contains access data to
        repository with rules metadata
        :return:
        """
        return self.get(KEY_RULES_METADATA_REPO_ACCESS_SSM_NAME) or \
            DEFAULT_RULES_METADATA_REPO_ACCESS_SSM_NAME

    def rules_to_service_section(self) -> Optional[str]:
        return self.get(KEY_RULES_TO_SERVICE_SECTION)

    def rules_to_severity(self) -> Optional[str]:
        return self.get(KEY_RULES_TO_SEVERITY)

    def rules_to_standards(self) -> Optional[str]:
        return self.get(KEY_RULES_TO_STANDARDS)

    def rules_to_mitre(self) -> Optional[str]:
        return self.get(KEY_RULES_TO_MITRE)

    def cloud_to_rules(self) -> Optional[str]:
        return self.get(KEY_CLOUD_TO_RULES)

    def aws_standards_coverage(self) -> Optional[Setting]:
        return self.get(KEY_AWS_STANDARDS_COVERAGE, value=False)

    def azure_standards_coverage(self) -> Optional[Setting]:
        return self.get(KEY_AZURE_STANDARDS_COVERAGE, value=False)

    def google_standards_coverage(self) -> Optional[Setting]:
        return self.get(KEY_GOOGLE_STANDARDS_COVERAGE, value=False)

    def aws_events(self) -> Optional[str]:
        return self.get(KEY_AWS_EVENTS)

    def azure_events(self) -> Optional[str]:
        return self.get(KEY_AZURE_EVENTS)

    def google_events(self) -> Optional[str]:
        return self.get(KEY_GOOGLE_EVENTS)


class CachedSettingsService(SettingsService):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # name to Setting instance
        self._cache = TTLCache(maxsize=30, ttl=900)

    def get(self, name, value: bool = True) -> Optional[Union[Setting, dict]]:
        if name in self._cache:
            setting = self._cache[name]
            return setting.value if value else setting
        # not in cache
        setting = super().get(name, value=False)
        if not setting:
            return
        self._cache[name] = setting
        return setting.value if value else setting

    def delete(self, setting: Union[Setting, str]) -> bool:
        name = setting if isinstance(setting, str) else setting.name
        self._cache.pop(name, None)
        return super().delete(setting)

    def save(self, setting: Setting):
        self._cache[setting.name] = setting
        return super().save(setting)
