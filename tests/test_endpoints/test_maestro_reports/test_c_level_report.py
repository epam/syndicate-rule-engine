import json

import pytest

from helpers.constants import ReportType
from services import SP
from models.metrics import ReportMetrics

from ...commons import dicts_equal


@pytest.fixture()
def c_level_overview(main_customer, load_expected, utcnow):
    item = SP.report_metrics_service.create(
        key=ReportMetrics.build_key_for_customer(
            ReportType.C_LEVEL_OVERVIEW, main_customer.name
        ),
        start=ReportType.C_LEVEL_OVERVIEW.start(utcnow),
        end=ReportType.C_LEVEL_OVERVIEW.end(utcnow),
    )
    SP.report_metrics_service.save(
        item, load_expected('metrics/c_level_overview')
    )


@pytest.fixture()
def c_level_compliance(main_customer, load_expected, utcnow):
    item = SP.report_metrics_service.create(
        key=ReportMetrics.build_key_for_customer(
            ReportType.C_LEVEL_COMPLIANCE, main_customer.name
        ),
        start=ReportType.C_LEVEL_COMPLIANCE.start(utcnow),
        end=ReportType.C_LEVEL_COMPLIANCE.end(utcnow),
    )
    SP.report_metrics_service.save(
        item, load_expected('metrics/c_level_compliance')
    )


@pytest.fixture()
def c_level_attacks(main_customer, load_expected, utcnow):
    item = SP.report_metrics_service.create(
        key=ReportMetrics.build_key_for_customer(
            ReportType.C_LEVEL_ATTACKS, main_customer.name
        ),
        start=ReportType.C_LEVEL_ATTACKS.start(utcnow),
        end=ReportType.C_LEVEL_ATTACKS.end(utcnow),
    )
    SP.report_metrics_service.save(
        item, load_expected('metrics/c_level_attacks')
    )


def test_c_level_overview(
    system_user_token,
    sre_client,
    c_level_overview,
    mocked_rabbitmq,
    load_expected,
):
    resp = sre_client.request(
        '/reports/clevel',
        'POST',
        auth=system_user_token,
        data={
            'customer_id': 'TEST_CUSTOMER',
            'types': ['OVERVIEW'],
            'receivers': ['admin@gmail.com'],
        },
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
    assert dicts_equal(model, load_expected('c_level/overview_report'))


def test_c_level_compliance(
    system_user_token,
    sre_client,
    c_level_compliance,
    mocked_rabbitmq,
    load_expected,
):
    resp = sre_client.request(
        '/reports/clevel',
        'POST',
        auth=system_user_token,
        data={
            'customer_id': 'TEST_CUSTOMER',
            'types': ['COMPLIANCE'],
            'receivers': ['admin@gmail.com'],
        },
    )
    assert resp.status_int == 202

    assert len(mocked_rabbitmq.send_sync.mock_calls) == 1
    kw = mocked_rabbitmq.send_sync.mock_calls[0].kwargs
    # checking models

    params = kw['parameters']
    assert len(params) == 1, 'Only one operational report is sent'
    typ = params[0]['model']['notificationType']
    model = json.loads(params[0]['model']['notificationAsJson'])

    assert typ == 'CUSTODIAN_CUSTOMER_COMPLIANCE_REPORT'
    assert dicts_equal(model, load_expected('c_level/compliance_report'))


def test_c_level_attacks(
    system_user_token,
    sre_client,
    c_level_attacks,
    mocked_rabbitmq,
    load_expected,
):
    resp = sre_client.request(
        '/reports/clevel',
        'POST',
        auth=system_user_token,
        data={
            'customer_id': 'TEST_CUSTOMER',
            'types': ['ATTACK_VECTOR'],
            'receivers': ['admin@gmail.com'],
        },
    )
    assert resp.status_int == 202

    assert len(mocked_rabbitmq.send_sync.mock_calls) == 1
    kw = mocked_rabbitmq.send_sync.mock_calls[0].kwargs
    # checking models

    params = kw['parameters']
    assert len(params) == 1, 'Only one operational report is sent'
    typ = params[0]['model']['notificationType']
    model = json.loads(params[0]['model']['notificationAsJson'])

    assert typ == 'CUSTODIAN_CUSTOMER_ATTACKS_REPORT'
    assert dicts_equal(model, load_expected('c_level/attacks_report'))
