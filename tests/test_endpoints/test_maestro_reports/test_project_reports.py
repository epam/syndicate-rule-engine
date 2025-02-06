import json

import pytest

from helpers.constants import ReportType
from services import SP
from models.metrics import ReportMetrics

from ...commons import dicts_equal, valid_uuid


@pytest.fixture()
def project_overview_metrics(aws_tenant, load_expected, utcnow):
    item = SP.report_metrics_service.create(
        key=ReportMetrics.build_key_for_project(
            ReportType.PROJECT_OVERVIEW,
            aws_tenant.customer_name,
            aws_tenant.display_name,
        ),
        end=utcnow,
    )
    SP.report_metrics_service.save(
        item, load_expected('metrics/aws_project_overview')
    )


@pytest.fixture()
def project_compliance_metrics(aws_tenant, load_expected, utcnow):
    item = SP.report_metrics_service.create(
        key=ReportMetrics.build_key_for_project(
            ReportType.PROJECT_COMPLIANCE,
            aws_tenant.customer_name,
            aws_tenant.display_name,
        ),
        end=utcnow,
    )
    SP.report_metrics_service.save(
        item, load_expected('metrics/aws_project_compliance')
    )


@pytest.fixture()
def project_resources_metrics(aws_tenant, load_expected, utcnow):
    item = SP.report_metrics_service.create(
        key=ReportMetrics.build_key_for_project(
            ReportType.PROJECT_RESOURCES,
            aws_tenant.customer_name,
            aws_tenant.display_name,
        ),
        end=utcnow,
    )
    SP.report_metrics_service.save(
        item, load_expected('metrics/aws_project_resources')
    )


@pytest.fixture()
def project_finops_metrics(aws_tenant, load_expected, utcnow):
    item = SP.report_metrics_service.create(
        key=ReportMetrics.build_key_for_project(
            ReportType.PROJECT_FINOPS,
            aws_tenant.customer_name,
            aws_tenant.display_name,
        ),
        end=utcnow,
    )
    SP.report_metrics_service.save(
        item, load_expected('metrics/aws_project_finops')
    )


@pytest.fixture()
def project_attacks_metrics(aws_tenant, load_expected, utcnow):
    item = SP.report_metrics_service.create(
        key=ReportMetrics.build_key_for_project(
            ReportType.PROJECT_ATTACKS,
            aws_tenant.customer_name,
            aws_tenant.display_name,
        ),
        end=utcnow,
    )
    SP.report_metrics_service.save(
        item, load_expected('metrics/aws_project_attacks')
    )


def validate_maestro_model(m: dict):
    assert isinstance(m, dict)
    assert m['viewType'] == 'm3'
    assert valid_uuid(m['model']['uuid'])
    assert m['model']['notificationProcessorTypes'] == ['MAIL']
    assert m['model']['notificationType']
    assert isinstance(m['model']['notificationAsJson'], str)


def test_project_overview_report(
    system_user_token,
    sre_client,
    project_overview_metrics,
    mocked_rabbitmq,
    load_expected,
):
    resp = sre_client.request(
        '/reports/project',
        'POST',
        auth=system_user_token,
        data={
            'customer_id': 'TEST_CUSTOMER',
            'tenant_display_names': ['testing'],
            'types': ['OVERVIEW'],
            'receivers': ['admin@gmail.com'],
        },
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
    assert dicts_equal(model, load_expected('project/overview_report'))


def test_project_compliance_report(
    system_user_token,
    sre_client,
    project_compliance_metrics,
    mocked_rabbitmq,
    load_expected,
):
    resp = sre_client.request(
        '/reports/project',
        'POST',
        auth=system_user_token,
        data={
            'customer_id': 'TEST_CUSTOMER',
            'tenant_display_names': ['testing'],
            'types': ['COMPLIANCE'],
            'receivers': ['admin@gmail.com'],
        },
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

    assert typ == 'CUSTODIAN_PROJECT_COMPLIANCE_REPORT'
    assert dicts_equal(model, load_expected('project/compliance_report'))


def test_project_resources_report(
    system_user_token,
    sre_client,
    project_resources_metrics,
    mocked_rabbitmq,
    load_expected,
):
    resp = sre_client.request(
        '/reports/project',
        'POST',
        auth=system_user_token,
        data={
            'customer_id': 'TEST_CUSTOMER',
            'tenant_display_names': ['testing'],
            'types': ['RESOURCES'],
            'receivers': ['admin@gmail.com'],
        },
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

    assert typ == 'CUSTODIAN_PROJECT_RESOURCES_REPORT'
    assert dicts_equal(model, load_expected('project/resources_report'))


def test_project_attacks_report(
    system_user_token,
    sre_client,
    project_attacks_metrics,
    mocked_rabbitmq,
    load_expected,
):
    resp = sre_client.request(
        '/reports/project',
        'POST',
        auth=system_user_token,
        data={
            'customer_id': 'TEST_CUSTOMER',
            'tenant_display_names': ['testing'],
            'types': ['ATTACK_VECTOR'],
            'receivers': ['admin@gmail.com'],
        },
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

    assert typ == 'CUSTODIAN_PROJECT_ATTACKS_REPORT'
    assert dicts_equal(model, load_expected('project/attacks_report'))


def test_project_finops_report(
    system_user_token,
    sre_client,
    project_finops_metrics,
    mocked_rabbitmq,
    load_expected,
):
    resp = sre_client.request(
        '/reports/project',
        'POST',
        auth=system_user_token,
        data={
            'customer_id': 'TEST_CUSTOMER',
            'tenant_display_names': ['testing'],
            'types': ['FINOPS'],
            'receivers': ['admin@gmail.com'],
        },
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

    assert typ == 'CUSTODIAN_PROJECT_FINOPS_REPORT'
    assert dicts_equal(model, load_expected('project/finops_report'))
