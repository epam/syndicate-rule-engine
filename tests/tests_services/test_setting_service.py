from unittest.mock import MagicMock, patch

import pytest

from helpers.constants import SettingKey
from models.setting import Setting
from services.setting_service import CachedSettingsService, SettingsService


@pytest.fixture
def settings_service():
    return SettingsService(environment_service=MagicMock())


@pytest.fixture
def cached_settings_service():
    return CachedSettingsService(environment_service=MagicMock())


def test_delete_uses_loaded_setting_instance(settings_service):
    setting = Setting(name=SettingKey.LM_CLIENT_KEY.value, value={'kid': 'kid'})

    with patch.object(setting, 'delete', autospec=True) as delete_mock:
        settings_service.delete(setting=setting)

    delete_mock.assert_called_once_with()


def test_cached_settings_service_delete_invalidates_cache(
    cached_settings_service,
):
    setting = Setting(name=SettingKey.LM_CLIENT_KEY.value, value={'kid': 'kid'})
    cached_settings_service._cache[SettingKey.LM_CLIENT_KEY] = setting

    with patch.object(setting, 'delete', autospec=True):
        cached_settings_service.delete(setting=setting)

    assert SettingKey.LM_CLIENT_KEY not in cached_settings_service._cache
    assert SettingKey.LM_CLIENT_KEY.value not in cached_settings_service._cache


def test_cached_settings_service_get_uses_consistent_read_after_delete(
    cached_settings_service,
):
    setting = Setting(name=SettingKey.LM_CLIENT_KEY.value, value={'kid': 'kid'})
    cached_settings_service._cache[SettingKey.LM_CLIENT_KEY.value] = setting

    with patch.object(
        SettingsService,
        'get',
        return_value=None,
    ) as get_mock:
        result = cached_settings_service.get(
            name=SettingKey.LM_CLIENT_KEY,
            consistent_read=True,
        )

    assert result is None
    get_mock.assert_called_once_with(
        SettingKey.LM_CLIENT_KEY,
        value=False,
        consistent_read=True,
    )
