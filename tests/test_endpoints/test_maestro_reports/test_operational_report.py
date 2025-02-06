import json

import pytest

from helpers.constants import ReportType
from models.metrics import ReportMetrics
from services import SP

from ...commons import dicts_equal, valid_uuid


@pytest.fixture()
def aws_operational_overview_metrics(aws_tenant, load_expected, utcnow):
    item = SP.report_metrics_service.create(
        key=ReportMetrics.build_key_for_tenant(
            ReportType.OPERATIONAL_OVERVIEW, aws_tenant
        ),
        end=utcnow,
    )
    SP.report_metrics_service.save(
        item, load_expected('metrics/aws_operational_overview')
    )


@pytest.fixture()
def aws_operational_resources_metrics(aws_tenant, load_expected, utcnow):
    item = SP.report_metrics_service.create(
        key=ReportMetrics.build_key_for_tenant(
            ReportType.OPERATIONAL_RESOURCES, aws_tenant
        ),
        end=utcnow,
    )
    SP.report_metrics_service.save(
        item, load_expected('metrics/aws_operational_resources')
    )


@pytest.fixture()
def aws_operational_rules_metrics(aws_tenant, load_expected, utcnow):
    item = SP.report_metrics_service.create(
        key=ReportMetrics.build_key_for_tenant(
            ReportType.OPERATIONAL_RULES, aws_tenant
        ),
        end=utcnow,
    )
    SP.report_metrics_service.save(
        item, load_expected('metrics/aws_operational_rules')
    )


@pytest.fixture()
def aws_operational_finops_metrics(aws_tenant, load_expected, utcnow):
    item = SP.report_metrics_service.create(
        key=ReportMetrics.build_key_for_tenant(
            ReportType.OPERATIONAL_FINOPS, aws_tenant
        ),
        end=utcnow,
    )
    SP.report_metrics_service.save(
        item, load_expected('metrics/aws_operational_finops')
    )


@pytest.fixture()
def aws_operational_compliance_metrics(aws_tenant, load_expected, utcnow):
    item = SP.report_metrics_service.create(
        key=ReportMetrics.build_key_for_tenant(
            ReportType.OPERATIONAL_COMPLIANCE, aws_tenant
        ),
        end=utcnow,
    )
    SP.report_metrics_service.save(
        item, load_expected('metrics/aws_operational_compliance')
    )


@pytest.fixture()
def k8s_operational_metrics(k8s_platform, load_expected, utcnow):
    item = SP.report_metrics_service.create(
        key=ReportMetrics.build_key_for_platform(
            ReportType.OPERATIONAL_KUBERNETES, k8s_platform
        ),
        end=utcnow,
    )
    SP.report_metrics_service.save(
        item, load_expected('metrics/k8s_operational')
    )


@pytest.fixture()
def azure_operational_deprecations_metrics(
    azure_tenant, load_expected, utcnow
):
    item = SP.report_metrics_service.create(
        key=ReportMetrics.build_key_for_tenant(
            ReportType.OPERATIONAL_DEPRECATION, azure_tenant
        ),
        end=utcnow,
    )
    SP.report_metrics_service.save(
        item, load_expected('metrics/azure_operational_deprecations')
    )


def validate_maestro_model(m: dict):
    assert isinstance(m, dict)
    assert m['viewType'] == 'm3'
    assert valid_uuid(m['model']['uuid'])
    assert m['model']['notificationProcessorTypes'] == ['MAIL']
    assert m['model']['notificationType']
    assert isinstance(m['model']['notificationAsJson'], str)


def test_operational_overview_report_aws_tenant(
    system_user_token,
    sre_client,
    aws_operational_overview_metrics,
    mocked_rabbitmq,
    load_expected,
):
    resp = sre_client.request(
        '/reports/operational',
        'POST',
        auth=system_user_token,
        data={
            'customer_id': 'TEST_CUSTOMER',
            'tenant_names': ['AWS-TESTING'],
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

    assert typ == 'CUSTODIAN_OVERVIEW_REPORT'
    assert dicts_equal(model, load_expected('operational/overview_report'))


def test_operational_resources_report_aws_tenant(
    system_user_token,
    sre_client,
    aws_operational_resources_metrics,
    mocked_rabbitmq,
    load_expected,
):
    resp = sre_client.request(
        '/reports/operational',
        'POST',
        auth=system_user_token,
        data={
            'customer_id': 'TEST_CUSTOMER',
            'tenant_names': ['AWS-TESTING'],
            'types': ['RESOURCES'],
            'receivers': ['admin@gmail.com'],
        },
    )
    assert resp.status_int == 202
    assert len(mocked_rabbitmq.send_sync.mock_calls) == 1
    params = mocked_rabbitmq.send_sync.mock_calls[0].kwargs['parameters']

    assert len(params) == 1, 'Only one operational report is sent'
    assert (
        params[0]['model']['notificationType'] == 'CUSTODIAN_RESOURCES_REPORT'
    )

    assert dicts_equal(
        json.loads(params[0]['model']['notificationAsJson']),
        load_expected('operational/resources_report'),
    )


def test_operational_rules_report_aws_tenant(
    system_user_token,
    sre_client,
    aws_operational_rules_metrics,
    mocked_rabbitmq,
    load_expected,
):
    resp = sre_client.request(
        '/reports/operational',
        'POST',
        auth=system_user_token,
        data={
            'customer_id': 'TEST_CUSTOMER',
            'tenant_names': ['AWS-TESTING'],
            'types': ['RULE'],
            'receivers': ['admin@gmail.com'],
        },
    )
    assert resp.status_int == 202
    assert len(mocked_rabbitmq.send_sync.mock_calls) == 1
    params = mocked_rabbitmq.send_sync.mock_calls[0].kwargs['parameters']

    assert len(params) == 1, 'Only one operational report is sent'
    assert params[0]['model']['notificationType'] == 'CUSTODIAN_RULES_REPORT'
    assert dicts_equal(
        json.loads(params[0]['model']['notificationAsJson']),
        load_expected('operational/rules_report'),
    )


def test_operational_finops_report_aws_tenant(
    system_user_token,
    sre_client,
    aws_operational_finops_metrics,
    mocked_rabbitmq,
    load_expected,
):
    resp = sre_client.request(
        '/reports/operational',
        'POST',
        auth=system_user_token,
        data={
            'customer_id': 'TEST_CUSTOMER',
            'tenant_names': ['AWS-TESTING'],
            'types': ['FINOPS'],
            'receivers': ['admin@gmail.com'],
        },
    )
    assert resp.status_int == 202
    assert len(mocked_rabbitmq.send_sync.mock_calls) == 1
    params = mocked_rabbitmq.send_sync.mock_calls[0].kwargs['parameters']

    assert len(params) == 1, 'Only one operational report is sent'
    assert params[0]['model']['notificationType'] == 'CUSTODIAN_FINOPS_REPORT'
    assert dicts_equal(
        json.loads(params[0]['model']['notificationAsJson']),
        load_expected('operational/finops_report'),
    )


def test_operational_compliance_report_aws_tenant(
    system_user_token,
    sre_client,
    aws_operational_compliance_metrics,
    mocked_rabbitmq,
    load_expected,
):
    resp = sre_client.request(
        '/reports/operational',
        'POST',
        auth=system_user_token,
        data={
            'customer_id': 'TEST_CUSTOMER',
            'tenant_names': ['AWS-TESTING'],
            'types': ['COMPLIANCE'],
            'receivers': ['admin@gmail.com'],
        },
    )
    assert resp.status_int == 202
    assert len(mocked_rabbitmq.send_sync.mock_calls) == 1
    params = mocked_rabbitmq.send_sync.mock_calls[0].kwargs['parameters']

    assert len(params) == 1, 'Only one operational report is sent'
    assert (
        params[0]['model']['notificationType'] == 'CUSTODIAN_COMPLIANCE_REPORT'
    )
    assert dicts_equal(
        json.loads(params[0]['model']['notificationAsJson']),
        load_expected('operational/compliance_report'),
    )


def test_operational_k8s(
    system_user_token,
    sre_client,
    k8s_operational_metrics,
    mocked_rabbitmq,
    load_expected,
):
    resp = sre_client.request(
        '/reports/operational',
        'POST',
        auth=system_user_token,
        data={
            'customer_id': 'TEST_CUSTOMER',
            'tenant_names': ['AWS-TESTING'],
            'types': ['KUBERNETES'],
            'receivers': ['admin@gmail.com'],
        },
    )
    assert resp.status_int == 202
    assert len(mocked_rabbitmq.send_sync.mock_calls) == 1
    params = mocked_rabbitmq.send_sync.mock_calls[0].kwargs['parameters']

    assert len(params) == 1, 'Only one operational report is sent'
    assert (
        params[0]['model']['notificationType']
        == 'CUSTODIAN_K8S_CLUSTER_REPORT'
    )
    assert dicts_equal(
        json.loads(params[0]['model']['notificationAsJson']),
        load_expected('operational/k8s_report'),
    )


def test_operational_deprecations_report_azure_tenant(
    system_user_token,
    sre_client,
    azure_operational_deprecations_metrics,
    mocked_rabbitmq,
    load_expected,
):
    resp = sre_client.request(
        '/reports/operational',
        'POST',
        auth=system_user_token,
        data={
            'customer_id': 'TEST_CUSTOMER',
            'tenant_names': ['AZURE-TESTING'],
            'types': ['DEPRECATIONS'],
            'receivers': ['admin@gmail.com'],
        },
    )
    assert resp.status_int == 202
    assert len(mocked_rabbitmq.send_sync.mock_calls) == 1
    params = mocked_rabbitmq.send_sync.mock_calls[0].kwargs['parameters']

    assert len(params) == 1, 'Only one operational report is sent'
    assert (
        params[0]['model']['notificationType']
        == 'CUSTODIAN_DEPRECATIONS_REPORT'
    )

    assert dicts_equal(
        json.loads(params[0]['model']['notificationAsJson']),
        load_expected('operational/deprecations_report'),
    )
