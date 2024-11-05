import json

import pytest

from services import SP
from ...commons import valid_uuid, dicts_equal


@pytest.fixture()
def aws_metrics(aws_tenant, load_expected, report_bounds):
    _, end = report_bounds
    SP.s3.gz_put_json(
        bucket='metrics',
        key=f'TEST_CUSTOMER/accounts/{end.date().isoformat()}/{aws_tenant.project}.json',
        obj=load_expected('metrics/aws_account')
    )


@pytest.fixture()
def azure_metrics(azure_tenant, load_expected, report_bounds):
    _, end = report_bounds
    SP.s3.gz_put_json(
        bucket='metrics',
        key=f'TEST_CUSTOMER/accounts/{end.date().isoformat()}/{azure_tenant.project}.json',
        obj=load_expected('metrics/azure_account')
    )


@pytest.fixture()
def google_metrics(google_tenant, load_expected, report_bounds):
    _, end = report_bounds
    SP.s3.gz_put_json(
        bucket='metrics',
        key=f'TEST_CUSTOMER/accounts/{end.date().isoformat()}/{google_tenant.project}.json',
        obj=load_expected('metrics/google_account')
    )


def validate_maestro_model(m: dict):
    assert isinstance(m, dict)
    assert m['viewType'] == 'm3'
    assert valid_uuid(m['model']['uuid'])
    assert m['model']['notificationProcessorTypes'] == ['MAIL']
    assert m['model']['notificationType']
    assert isinstance(m['model']['notificationAsJson'], str)


def test_operational_report_aws_tenant(
        system_user_token, sre_client, aws_metrics, mocked_rabbitmq,
        load_expected):
    resp = sre_client.request(
        "/reports/operational",
        "POST",
        auth=system_user_token,
        data={
            "customer_id": "TEST_CUSTOMER",
            "tenant_names": ['AWS-TESTING'],
            "receivers": ["admin@gmail.com"]
        }
    )
    assert resp.status_int == 200

    # testing that modular-sdk's rabbit client was called with expected data
    assert len(mocked_rabbitmq.send_sync.mock_calls) == 1

    kw = mocked_rabbitmq.send_sync.mock_calls[0].kwargs
    assert kw['command_name'] == 'SEND_MAIL'
    assert kw['is_flat_request'] is False
    assert kw['async_request'] is False
    assert kw['secure_parameters'] is None
    assert kw['compressed'] is True

    # checking models
    params = kw['parameters']
    assert len(params) == 6, 'All 6 report types must be sent'
    type_model = {}
    for param in params:
        validate_maestro_model(param)
        type_model[param['model']['notificationType']] = json.loads(
            param['model']['notificationAsJson'])

    assert dicts_equal(
        type_model['CUSTODIAN_ATTACKS_REPORT'],
        load_expected('operational/attacks_report')
    )
    assert dicts_equal(
        type_model['CUSTODIAN_COMPLIANCE_REPORT'],
        load_expected('operational/compliance_report')
    )
    assert dicts_equal(
        type_model['CUSTODIAN_OVERVIEW_REPORT'],
        load_expected('operational/overview_report')
    )
    assert dicts_equal(
        type_model['CUSTODIAN_RESOURCES_REPORT'],
        load_expected('operational/resources_report')
    )
    assert dicts_equal(
        type_model['CUSTODIAN_RULES_REPORT'],
        load_expected('operational/rules_report')
    )
    assert dicts_equal(
        type_model['CUSTODIAN_FINOPS_REPORT'],
        load_expected('operational/finops_report')
    )
