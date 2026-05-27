from typing import TYPE_CHECKING

import pytest
from unittest.mock import MagicMock, patch
from modular_sdk.commons.constants import ApplicationType

from services import SP

if TYPE_CHECKING:
    from modular_sdk.models.application import Application


@pytest.fixture(autouse=True)
def send_reports_true(mocked_mongo_client) -> None:
    from models.setting import Setting
    from services.setting_service import SettingKey
    Setting(
        name=SettingKey.SEND_REPORTS,
        value=True
    ).save()


@pytest.fixture(autouse=True)
def rabbitmq_application(main_customer, mocked_mongo_client, mocked_hvac_client
                         ) -> 'Application':
    name = f'{main_customer.name}-rabbitmq'
    SP.ssm.create_secret(
        secret_name=name,
        secret_value=dict(
            connection_url='testing',
            sdk_secret_key='testing'
        )
    )
    app = SP.modular_client.application_service().build(
        customer_id=main_customer.name,
        type=ApplicationType.CUSTODIAN_RABBITMQ,
        created_by='testing',
        is_deleted=False,
        description='Mocked rabbitmq',
        meta=dict(
            maestro_user='testing',
            rabbit_exchange='testing',
            request_queue='testing',
            response_queue='testing',
            sdk_access_key='testing'
        ),
        secret=name
    )
    app.save()
    return app


@pytest.fixture(autouse=True)
def mocked_rabbitmq():
    mock = MagicMock()
    mock.send_sync.return_value = 200, 'SUCCESS', 'Successfully sent'
    p = patch.object(SP.modular_client, 'rabbit_transport_service',
                     return_value=mock)
    p.start()
    yield mock
    p.stop()


# @pytest.fixture(autouse=True)
# def activate_all_tenants(main_customer):
#     lic = SP.license_service.create(
#         license_key='license_key',
#         customer=main_customer.name,
#         created_by='testing',
#         expiration=utc_iso(utc_datetime() + timedelta(days=1)),
#         description='Testing license',
#         ruleset_ids=['TESTING'],
#     )
#     SP.license_service.save(lic)
#     SP.modular_client.parent_service().create_all_scope(
#         application_id=lic.license_key,
#         customer_id=main_customer.name,
#         type_=ParentType.CUSTODIAN_LICENSES,
#         created_by='testing'
#     ).save()

