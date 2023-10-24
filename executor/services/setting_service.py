from typing import Optional, Union

from helpers.log_helper import get_logger
from models.setting import Setting

KEY_CONTACTS = 'CONTACTS'
KEY_MAIL_CONFIGURATION = 'MAIL_CONFIGURATION'
KEY_SYSTEM_CUSTOMER = 'SYSTEM_CUSTOMER_NAME'
KEY_CUSTODIAN_SERVICE_PRIVATE_KEY = 'LM_CLIENT_KEY'
KEY_STATS_S3_BUCKET_NAME = 'STATS_S3_BUCKET_NAME'
KEY_ACCESS_DATA_LM = 'ACCESS_DATA_LM'
KEY_TEMPLATE_BUCKET = 'TEMPLATES_S3_BUCKET_NAME'

_LOG = get_logger(__name__)


class SettingService:

    @staticmethod
    def get(name, value: bool = True) -> Optional[Union[Setting, dict]]:
        setting = Setting.get_nullable(hash_key=name)
        if setting and value:
            return setting.value
        elif setting:
            return setting

    @staticmethod
    def get_contacts():
        """
        Returns Custodian Team contacts (DL)
        """
        contacts = SettingService.get(name=KEY_CONTACTS)
        return contacts if contacts else ''

    @staticmethod
    def get_mail_configuration(value: bool = True) -> Optional[
        Union[Setting, dict]
    ]:
        return SettingService.get(
            name=KEY_MAIL_CONFIGURATION, value=value
        )

    @staticmethod
    def get_custodian_service_private_key():
        """
        Returns `custodian service key` data, including key-id and algorithm.
        :return: dict
        """
        key = SettingService.get(name=KEY_CUSTODIAN_SERVICE_PRIVATE_KEY)
        return key if key else {}

    @staticmethod
    def get_license_manager_access_data(value: bool = True):
        return SettingService.get(name=KEY_ACCESS_DATA_LM, value=value)

    @staticmethod
    def get_template_bucket() -> Optional[str]:
        bucket = SettingService.get(name=KEY_TEMPLATE_BUCKET)
        return bucket or None
