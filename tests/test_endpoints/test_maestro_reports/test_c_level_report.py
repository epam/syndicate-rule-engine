import pytest
import json
from helpers.constants import ReportType
from ...commons import dicts_equal

from services import SP


@pytest.fixture()
def c_level_overview(main_customer, load_expected, utcnow):
    SP.report_metrics_service.create(
        key=SP.report_metrics_service.key_for_customer(ReportType.C_LEVEL_OVERVIEW, main_customer.name),
        data=load_expected('metrics/c_level_overview'),
        end=utcnow
    ).save()


def test_c_level_overview(
    system_user_token, sre_client, c_level_overview, mocked_rabbitmq,
    load_expected
):
    resp = sre_client.request(
        "/reports/clevel",
        "POST",
        auth=system_user_token,
        data={
            "customer_id": "TEST_CUSTOMER",
            "types": ["OVERVIEW"],
            "receivers": ["admin@gmail.com"]
        }
    )
    assert resp.status_int == 202

    assert len(mocked_rabbitmq.send_sync.mock_calls) == 1
    kw = mocked_rabbitmq.send_sync.mock_calls[0].kwargs
    # checking models

    params = kw['parameters']
    assert len(params) == 1, 'Only one operational report is sent'
    typ = params[0]['model']['notificationType']
    model = json.loads(params[0]['model']['notificationAsJson'])

    assert typ == 'CUSTODIAN_CUSTOMER_OVERVIEW_REPORT'
    assert dicts_equal(
        model,
        load_expected('c_level/overview_report')
    )
