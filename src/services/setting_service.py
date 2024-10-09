from typing import Union, Optional

from pynamodb.exceptions import PynamoDBException

import services.cache as cache
from helpers.constants import (DEFAULT_SYSTEM_CUSTOMER, SettingKey,
                               DEFAULT_RULES_METADATA_REPO_ACCESS_SSM_NAME)
from helpers.log_helper import get_logger
from models.setting import Setting
from services.environment_service import EnvironmentService


EVENT_CURSOR_TIMESTAMP_ATTR = 'ect'

_LOG = get_logger(__name__)


# TODO do not retrieve items from db before updating
class SettingsService:
    def __init__(self, environment_service: EnvironmentService):
        self._environment = environment_service

    @staticmethod
    def get_all_settings():
        return Setting.scan()

    @staticmethod
    def create(name: SettingKey | str,
               value: float | str | list | dict) -> Setting:
        if isinstance(name, SettingKey):
            name = name.value
        return Setting(name=name, value=value)

    def get(self, name: SettingKey | str, value: bool = True,
            consistent_read: bool = False) -> Setting | dict | None:
        if isinstance(name, SettingKey):
            name = name.value
        _LOG.debug(f'Querying {name} setting')
        setting = Setting.get_nullable(hash_key=name,
                                       consistent_read=consistent_read)
        if setting and value:
            return setting.value
        elif setting:
            return setting

    def delete(self, setting: Union[Setting, str, SettingKey]) -> bool:
        if isinstance(setting, Setting):
            name = setting.name
        elif isinstance(setting, SettingKey):
            name = setting.value
        else:
            name = setting
        Setting(name=name).delete()
        return True

    def save(self, setting: Setting):
        return setting.save()

    def get_license_manager_access_data(self, value: bool = True):
        return self.get(name=SettingKey.ACCESS_DATA_LM, value=value)

    def create_license_manager_access_data_configuration(
            self, host: str,
            port: Optional[int] = None,
            protocol: Optional[str] = None,
            stage: Optional[str] = None) -> Setting:
        from services.clients.lm_client import LMAccessData
        model = LMAccessData.from_dict({})
        model.update_host(host=host, port=port, protocol=protocol, stage=stage)
        return self.create(
            name=SettingKey.ACCESS_DATA_LM.value, value=model.dict()
        )

    def get_license_manager_client_key_data(self, value: bool = True):
        return self.get(name=SettingKey.LM_CLIENT_KEY, value=value)

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
            name=SettingKey.LM_CLIENT_KEY, value=dict(
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
            name=SettingKey.MAIL_CONFIGURATION, value=dict(
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
            name=SettingKey.MAIL_CONFIGURATION, value=value
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
            name = self.get(SettingKey.SYSTEM_CUSTOMER)
        except PynamoDBException as e:
            _LOG.warning(f'Could not query {SettingKey.SYSTEM_CUSTOMER} '
                         f'setting: {e}.'
                         f' Using the default SYSTEM customer name')
        except Exception as e:
            _LOG.warning(f'Unexpected error occurred trying querying '
                         f'{SettingKey.SYSTEM_CUSTOMER} setting: {e}. Using the '
                         f'default SYSTEM customer name')
        return name or DEFAULT_SYSTEM_CUSTOMER

    def create_event_assembler_configuration(self, cursor: float) -> Setting:
        return self.create(
            name=SettingKey.EVENT_ASSEMBLER, value={
                EVENT_CURSOR_TIMESTAMP_ATTR: cursor
            }
        )

    def get_event_assembler_configuration(self, value: bool = True
                                          ) -> Optional[Union[Setting, dict]]:
        return self.get(name=SettingKey.EVENT_ASSEMBLER, value=value)

    def get_report_date_marker(self) -> dict:
        marker = self.get(name=SettingKey.REPORT_DATE_MARKER)
        return marker or {}

    def set_report_date_marker(self, current_week_date: str = None,
                               last_week_date: str = None):
        marker = self.get(name=SettingKey.REPORT_DATE_MARKER)
        if current_week_date:
            marker.update({'current_week_date': current_week_date})
        if last_week_date:
            marker.update({'last_week_date': last_week_date})
        new_marker = self.create(name=SettingKey.REPORT_DATE_MARKER,
                                 value=marker)
        new_marker.save()

    # metadata
    def rules_metadata_repo_access_data(self) -> str:
        """
        Returns the name of ssm parameter which contains access data to
        repository with rules metadata
        :return:
        """
        return self.get(SettingKey.RULES_METADATA_REPO_ACCESS_SSM_NAME) or \
            DEFAULT_RULES_METADATA_REPO_ACCESS_SSM_NAME

    def aws_standards_coverage(self) -> Optional[Setting]:
        return self.get(SettingKey.AWS_STANDARDS_COVERAGE, value=False)

    def azure_standards_coverage(self) -> Optional[Setting]:
        return self.get(SettingKey.AZURE_STANDARDS_COVERAGE, value=False)

    def google_standards_coverage(self) -> Optional[Setting]:
        return self.get(SettingKey.GOOGLE_STANDARDS_COVERAGE, value=False)

    def max_cron_number(self) -> Optional[int]:
        value = self.get(SettingKey.MAX_CRON_NUMBER)
        if isinstance(value, str) and value.isdigit():
            value = int(value)
            return value if 2 <= value <= 20 else 10
        else:
            return 10

    def get_retry_interval(self) -> Optional[int]:
        value = self.get('retry_interval')
        if isinstance(value, str) and value.isdigit():
            value = int(value) if int(value) <= 60 else 60
            return value if value >= 0 else 30
        return 30

    def disable_send_reports(self):
        self.create(name=SettingKey.SEND_REPORTS, value=False).save()

    def enable_send_reports(self):
        self.create(name=SettingKey.SEND_REPORTS, value=True).save()

    def get_send_reports(self) -> bool:
        value = self.get(name=SettingKey.SEND_REPORTS,
                         consistent_read=True)
        return value if value else False

    def get_max_attempt_number(self) -> int:
        value = self.get(name=SettingKey.MAX_ATTEMPT)
        return value if value else 4

    def get_max_rabbitmq_size(self) -> int:
        value = self.get(name=SettingKey.MAX_RABBITMQ_REQUEST_SIZE)
        return int(value) if value else 5000000  # 5 MB


class CachedSettingsService(SettingsService):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # name to Setting instance
        self._cache = cache.factory()

    def get(self, name: str, value: bool = True, consistent_read: bool = False
            ) -> Optional[Union[Setting, dict]]:
        if name in self._cache and not consistent_read:
            setting = self._cache[name]
            _LOG.debug(f'Getting setting {name} from cache')
            return setting.value if value else setting
        # not in cache
        _LOG.debug(f'{name} setting value is missing from cache')
        setting = super().get(name, value=False,
                              consistent_read=consistent_read)
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
        _LOG.debug(f'{setting.name} setting was saved to cache')
        return super().save(setting)
