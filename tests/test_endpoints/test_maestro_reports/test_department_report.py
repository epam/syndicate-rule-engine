import json

import pytest

from helpers.constants import ReportType
from services import SP
from models.metrics import ReportMetrics

from ...commons import dicts_equal


@pytest.fixture()
def top_attacks_by_cloud(main_customer, load_expected, utcnow):
    item = SP.report_metrics_service.create(
        key=ReportMetrics.build_key_for_customer(
            ReportType.DEPARTMENT_TOP_ATTACK_BY_CLOUD, main_customer.name
        ),
        start=ReportType.DEPARTMENT_TOP_ATTACK_BY_CLOUD.start(utcnow),
        end=ReportType.DEPARTMENT_TOP_ATTACK_BY_CLOUD.end(utcnow),
    )
    SP.report_metrics_service.save(
        item, load_expected('metrics/department_top_attacks_by_cloud')
    )


@pytest.fixture()
def top_compliance_by_cloud(main_customer, load_expected, utcnow):
    item = SP.report_metrics_service.create(
        key=ReportMetrics.build_key_for_customer(
            ReportType.DEPARTMENT_TOP_COMPLIANCE_BY_CLOUD, main_customer.name
        ),
        start=ReportType.DEPARTMENT_TOP_COMPLIANCE_BY_CLOUD.start(utcnow),
        end=ReportType.DEPARTMENT_TOP_COMPLIANCE_BY_CLOUD.end(utcnow),
    )
    SP.report_metrics_service.save(
        item, load_expected('metrics/department_top_compliance_by_cloud')
    )


@pytest.fixture()
def top_resources_by_cloud(main_customer, load_expected, utcnow):
    item = SP.report_metrics_service.create(
        key=ReportMetrics.build_key_for_customer(
            ReportType.DEPARTMENT_TOP_RESOURCES_BY_CLOUD, main_customer.name
        ),
        start=ReportType.DEPARTMENT_TOP_RESOURCES_BY_CLOUD.start(utcnow),
        end=ReportType.DEPARTMENT_TOP_RESOURCES_BY_CLOUD.end(utcnow),
    )
    SP.report_metrics_service.save(
        item, load_expected('metrics/department_top_resources_by_cloud')
    )


@pytest.fixture()
def top_tenants_attacks(main_customer, load_expected, utcnow):
    item = SP.report_metrics_service.create(
        key=ReportMetrics.build_key_for_customer(
            ReportType.DEPARTMENT_TOP_TENANTS_ATTACKS, main_customer.name
        ),
        start=ReportType.DEPARTMENT_TOP_TENANTS_ATTACKS.start(utcnow),
        end=ReportType.DEPARTMENT_TOP_TENANTS_ATTACKS.end(utcnow),
    )
    SP.report_metrics_service.save(
        item, load_expected('metrics/department_top_tenants_attacks')
    )


@pytest.fixture()
def top_tenants_compliance(main_customer, load_expected, utcnow):
    item = SP.report_metrics_service.create(
        key=ReportMetrics.build_key_for_customer(
            ReportType.DEPARTMENT_TOP_TENANTS_COMPLIANCE, main_customer.name
        ),
        start=ReportType.DEPARTMENT_TOP_TENANTS_COMPLIANCE.start(utcnow),
        end=ReportType.DEPARTMENT_TOP_TENANTS_COMPLIANCE.end(utcnow),
    )
    SP.report_metrics_service.save(
        item, load_expected('metrics/department_top_tenants_compliance')
    )


@pytest.fixture()
def top_tenants_resources(main_customer, load_expected, utcnow):
    item = SP.report_metrics_service.create(
        key=ReportMetrics.build_key_for_customer(
            ReportType.DEPARTMENT_TOP_TENANTS_RESOURCES, main_customer.name
        ),
        start=ReportType.DEPARTMENT_TOP_TENANTS_RESOURCES.start(utcnow),
        end=ReportType.DEPARTMENT_TOP_TENANTS_RESOURCES.end(utcnow),
    )
    SP.report_metrics_service.save(
        item, load_expected('metrics/department_top_tenants_resources')
    )


def test_top_attacks_by_cloud(
    system_user_token,
    sre_client,
    top_attacks_by_cloud,
    mocked_rabbitmq,
    load_expected,
):
    resp = sre_client.request(
        '/reports/department',
        'POST',
        auth=system_user_token,
        data={
            'customer_id': 'TEST_CUSTOMER',
            'types': ['TOP_ATTACK_BY_CLOUD'],
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

    assert typ == 'CUSTODIAN_TOP_TENANTS_BY_CLOUD_ATTACKS_REPORT'
    assert dicts_equal(
        model, load_expected('department/top_attacks_by_cloud_report')
    )


def test_top_compliance_by_cloud(
    system_user_token,
    sre_client,
    top_compliance_by_cloud,
    mocked_rabbitmq,
    load_expected,
):
    resp = sre_client.request(
        '/reports/department',
        'POST',
        auth=system_user_token,
        data={
            'customer_id': 'TEST_CUSTOMER',
            'types': ['TOP_COMPLIANCE_BY_CLOUD'],
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

    assert typ == 'CUSTODIAN_TOP_COMPLIANCE_BY_CLOUD_REPORT'
    assert dicts_equal(
        model, load_expected('department/top_compliance_by_cloud_report')
    )


def test_top_resources_by_cloud(
    system_user_token,
    sre_client,
    top_resources_by_cloud,
    mocked_rabbitmq,
    load_expected,
):
    resp = sre_client.request(
        '/reports/department',
        'POST',
        auth=system_user_token,
        data={
            'customer_id': 'TEST_CUSTOMER',
            'types': ['TOP_RESOURCES_BY_CLOUD'],
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

    assert typ == 'CUSTODIAN_TOP_RESOURCES_BY_CLOUD_REPORT'
    assert dicts_equal(
        model, load_expected('department/top_resources_by_cloud_report')
    )


def test_top_tenants_attacks(
    system_user_token,
    sre_client,
    top_tenants_attacks,
    mocked_rabbitmq,
    load_expected,
):
    resp = sre_client.request(
        '/reports/department',
        'POST',
        auth=system_user_token,
        data={
            'customer_id': 'TEST_CUSTOMER',
            'types': ['TOP_TENANTS_ATTACKS'],
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

    assert typ == 'CUSTODIAN_TOP_TENANTS_ATTACKS_REPORT'
    assert dicts_equal(
        model, load_expected('department/top_tenants_attacks_report')
    )


def test_top_tenants_compliance(
    system_user_token,
    sre_client,
    top_tenants_compliance,
    mocked_rabbitmq,
    load_expected,
):
    resp = sre_client.request(
        '/reports/department',
        'POST',
        auth=system_user_token,
        data={
            'customer_id': 'TEST_CUSTOMER',
            'types': ['TOP_TENANTS_COMPLIANCE'],
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

    assert typ == 'CUSTODIAN_TOP_TENANTS_COMPLIANCE_REPORT'
    assert dicts_equal(
        model, load_expected('department/top_tenants_compliance_report')
    )


def test_top_tenants_resources(
    system_user_token,
    sre_client,
    top_tenants_resources,
    mocked_rabbitmq,
    load_expected,
):
    resp = sre_client.request(
        '/reports/department',
        'POST',
        auth=system_user_token,
        data={
            'customer_id': 'TEST_CUSTOMER',
            'types': ['TOP_TENANTS_RESOURCES'],
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

    assert typ == 'CUSTODIAN_TOP_TENANTS_VIOLATED_RESOURCES_REPORT'
    assert dicts_equal(
        model, load_expected('department/top_tenants_resources_report')
    )
