import json

import pytest

from services import SP
from helpers.constants import ReportType
from ...commons import valid_uuid, dicts_equal


@pytest.fixture()
def project_overview_metrics(aws_tenant, load_expected, utcnow):
    item = SP.report_metrics_service.create(
        key=SP.report_metrics_service.key_for_project(ReportType.PROJECT_OVERVIEW, aws_tenant.customer_name, aws_tenant.display_name),
        end=utcnow
    )
    SP.report_metrics_service.save(item, load_expected('metrics/aws_project_overview'))


def validate_maestro_model(m: dict):
    assert isinstance(m, dict)
    assert m['viewType'] == 'm3'
    assert valid_uuid(m['model']['uuid'])
    assert m['model']['notificationProcessorTypes'] == ['MAIL']
    assert m['model']['notificationType']
    assert isinstance(m['model']['notificationAsJson'], str)


def test_project_overview_report(
        system_user_token, sre_client, project_overview_metrics, mocked_rabbitmq,
        load_expected):
    resp = sre_client.request(
        "/reports/project",
        "POST",
        auth=system_user_token,
        data={
            "customer_id": "TEST_CUSTOMER",
            "tenant_display_names": ['testing'],
            "types": ["OVERVIEW"],
            "receivers": ["admin@gmail.com"]
        }
    )
    assert resp.status_int == 202

    assert len(mocked_rabbitmq.send_sync.mock_calls) == 1

    kw = mocked_rabbitmq.send_sync.mock_calls[0].kwargs
    assert kw['command_name'] == 'SEND_MAIL'
    assert kw['is_flat_request'] is False
    assert kw['async_request'] is False
    assert kw['secure_parameters'] is None
    assert kw['compressed'] is True

    # checking models
    params = kw['parameters']
    assert len(params) == 1, 'Only one operational report is sent'
    typ = params[0]['model']['notificationType']
    model = json.loads(params[0]['model']['notificationAsJson'])

    assert typ == 'CUSTODIAN_PROJECT_OVERVIEW_REPORT'
    assert dicts_equal(
        model,
        load_expected('project/overview_report')
    )
