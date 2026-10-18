from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, call

import pytest

from helpers.constants import (
    Cloud,
    JobState,
    JobType,
    PolicyErrorType,
    RemediationComplexity,
    SettingKey,
    Severity,
)
from models.job import Job
from models.resource import Resource
from services import dashboard_report_service as dashboard
from services.metadata import Metadata, RuleMetadata
from services.report_service import StatisticsItem
from services.reports_bucket import (
    PlatformReportsBucketKeysBuilder,
    TenantReportsBucketKeysBuilder,
)
from services.sharding import ShardsCollectionFactory

RULE = 'ecc-aws-123-check'
REGION = 'eu-west-1'
TIMESTAMP = 1_735_689_600


def _resource(
    resource_id='resource-1',
    resource_type='aws.ec2',
    region=REGION,
    data=None,
    namespace=None,
):
    return Resource(
        id=resource_id,
        account_id='account',
        location=region,
        resource_type=resource_type,
        sync_date=TIMESTAMP,
        tenant_name='tenant',
        customer_name='customer',
        namespace=namespace,
        _data=data or {},
    )


def _statistic(rule=RULE, region=REGION, error_type=None):
    return StatisticsItem(
        policy=rule,
        region=region,
        tenant_name='tenant',
        customer_name='customer',
        start_time=TIMESTAMP,
        end_time=TIMESTAMP + 1,
        error_type=error_type,
    )


def _scan_metadata(rule=RULE, resource_type='ec2', comment=''):
    return {
        rule: {
            'description': 'Check resource configuration',
            'resource': resource_type,
            'comment': comment,
        }
    }


def _page(resources=(), last_evaluated_key=None):
    page = MagicMock()
    page.__iter__.side_effect = lambda: iter(resources)
    page.last_evaluated_key = last_evaluated_key
    return page


@pytest.fixture
def rule_metadata():
    return RuleMetadata(
        cloud='AWS',
        source='CIS',
        category=' Security > Identity > Access ',
        service_section='Compute',
        service='EC2',
        severity=Severity.HIGH,
        remediation_complexity=RemediationComplexity.LOW_MEDIUM,
        article='',
        impact='',
        remediation='',
    )


@pytest.fixture
def metadata(rule_metadata):
    return Metadata(rules={RULE: rule_metadata})


@pytest.fixture
def job(aws_tenant):
    return Job(
        id='dashboard-job',
        job_type=JobType.STANDARD,
        tenant_name=aws_tenant.name,
        customer_name=aws_tenant.customer_name,
        status=JobState.SUCCEEDED.value,
        submitted_at='2025-01-01T00:00:00Z',
        started_at='2025-01-01T00:01:00Z',
        stopped_at='2025-01-01T00:02:00Z',
    )


@pytest.fixture
def dependencies(metadata):
    return SimpleNamespace(
        s3_client=Mock(),
        environment_service=Mock(
            default_reports_bucket_name=Mock(return_value='reports')
        ),
        license_service=Mock(
            get_customer_metadata=Mock(return_value=metadata)
        ),
        resources_service=Mock(get_resources=Mock(return_value=_page())),
        platform_service=Mock(),
        settings_service=Mock(get=Mock(return_value=None)),
    )


@pytest.fixture
def service(dependencies):
    return dashboard.DashboardReportService(**vars(dependencies))


@pytest.fixture
def renderer(monkeypatch):
    renderer = Mock()
    monkeypatch.setattr(dashboard, 'fill_dashboard_template', renderer)
    return renderer


@pytest.mark.parametrize(
    'has_finding,error_type,has_inventory,expected',
    [
        (True, None, True, ['Failed']),
        (True, None, False, ['Failed']),
        (False, None, True, ['Passed']),
        (False, PolicyErrorType.ACCESS, True, ['Not evaluated']),
        (True, PolicyErrorType.ACCESS, True, ['Failed']),
        (True, PolicyErrorType.ACCESS, False, ['Failed']),
        (False, PolicyErrorType.ACCESS, False, ['Not evaluated']),
        (False, None, False, []),
    ],
)
def test_make_dashboard_rows_reconciles_statuses(
    metadata, has_finding, error_type, has_inventory, expected
):
    resource = _resource()
    rows = dashboard.make_dashboard_rows(
        findings=(
            [(RULE, REGION, iter([resource.to_cloud_resource()]))]
            if has_finding
            else []
        ),
        statistics=[_statistic(error_type=error_type)],
        inventory=[resource] if has_inventory else [],
        metadata=metadata,
        cloud=Cloud.AWS,
        scan_metadata=_scan_metadata(),
    )

    assert [row['check_status'] for row in rows] == expected
    if rows:
        assert rows[0]['resource_id'] == (
            'resource-1' if has_inventory or has_finding else ''
        )
        assert rows[0]['resource_type'] == 'aws.ec2'


def test_make_dashboard_rows_has_exact_field_contract(metadata):
    resource = _resource(
        data={
            'Tags': [
                {'Key': 'z', 'Value': 'last'},
                {'Key': 'a', 'Value': 'team'},
            ]
        }
    )
    rows = dashboard.make_dashboard_rows(
        findings=[(RULE, REGION, [resource.to_cloud_resource()])],
        statistics=[],
        inventory=[],
        metadata=metadata,
        cloud=Cloud.AWS,
        scan_metadata=_scan_metadata(),
    )

    assert rows == [
        {
            'resource_type': 'aws.ec2',
            'rule_id': 'aws-123',
            'rule_title': 'Check resource configuration',
            'resource_id': 'resource-1',
            'resource_tags': '{"a": "team", "z": "last"}',
            'check_status': 'Failed',
            'severity': 'High',
            'severity_priority': 40,
            'remediation_complexity': 'Low-Medium',
            'remediation_complexity_priority': 4,
            'remediation_priority': 44,
            'region_namespace': REGION,
            'date': '2025-01-01T00:00:00+00:00',
            'service': 'EC2',
            'service_section': 'Compute',
            'category': 'Security.Identity',
        }
    ]


def test_make_dashboard_rows_matches_rule_region_and_resource_type(metadata):
    second_rule = 'ecc-aws-124-other'
    inventory = [
        _resource(),
        _resource('other-region', region='eu-central-1'),
        _resource('other-type', resource_type='aws.s3'),
    ]
    rows = dashboard.make_dashboard_rows(
        findings=[],
        statistics=[_statistic(), _statistic(rule=second_rule)],
        inventory=inventory,
        metadata=metadata,
        cloud=Cloud.AWS,
        scan_metadata={
            **_scan_metadata(),
            **_scan_metadata(rule=second_rule, resource_type='aws.ec2'),
        },
    )

    assert [
        (row['rule_id'], row['resource_id'], row['check_status'])
        for row in rows
    ] == [
        ('aws-123', 'resource-1', 'Passed'),
        ('aws-124', 'resource-1', 'Passed'),
    ]


def test_make_dashboard_rows_does_not_infer_passes_without_statistics(
    metadata,
):
    finding = _resource('failed')
    rows = dashboard.make_dashboard_rows(
        findings=[(RULE, REGION, [finding.to_cloud_resource()])],
        statistics=[],
        inventory=[finding, _resource('not-evaluated')],
        metadata=metadata,
        cloud=Cloud.AWS,
        scan_metadata=_scan_metadata(),
    )

    assert [(row['resource_id'], row['check_status']) for row in rows] == [
        ('failed', 'Failed')
    ]


@pytest.mark.parametrize(
    'cloud,resource_type,expected',
    [
        (Cloud.AWS, 'aws.ec2', ['Failed', 'Passed']),
        (Cloud.AZURE, 'azure.vm', ['Failed']),
        (Cloud.GOOGLE, 'gcp.instance', ['Failed', 'Passed']),
        (Cloud.KUBERNETES, 'k8s.pod', ['Failed', 'Passed']),
    ],
)
def test_make_dashboard_rows_deduplicates_with_cloud_specific_id_matching(
    metadata, cloud, resource_type, expected
):
    inventory = _resource(
        'RESOURCE', resource_type=resource_type, region='global'
    )
    finding = _resource(
        'resource', resource_type=resource_type, region='global'
    ).to_cloud_resource()
    rows = dashboard.make_dashboard_rows(
        findings=[(RULE, 'global', iter([finding, finding]))],
        statistics=iter([_statistic(region='global')]),
        inventory=iter([inventory, inventory]),
        metadata=metadata,
        cloud=cloud,
        scan_metadata=_scan_metadata(resource_type=resource_type),
    )

    assert sorted(row['check_status'] for row in rows) == expected
    assert len(rows) == len(expected)


def test_make_dashboard_rows_emits_exact_error_placeholder(metadata):
    rows = dashboard.make_dashboard_rows(
        findings=[],
        statistics=[_statistic(error_type=PolicyErrorType.ACCESS)],
        inventory=[],
        metadata=metadata,
        cloud=Cloud.AWS,
        scan_metadata=_scan_metadata(),
    )

    assert len(rows) == 1
    assert rows[0] == {
        'resource_type': 'aws.ec2',
        'rule_id': 'aws-123',
        'rule_title': 'Check resource configuration',
        'resource_id': '',
        'resource_tags': '{}',
        'check_status': 'Not evaluated',
        'severity': 'High',
        'severity_priority': 40,
        'remediation_complexity': 'Low-Medium',
        'remediation_complexity_priority': 4,
        'remediation_priority': 44,
        'region_namespace': REGION,
        'date': '',
        'service': 'EC2',
        'service_section': 'Compute',
        'category': 'Security.Identity',
    }


def test_make_dashboard_rows_empty_inputs(empty_metadata):
    assert (
        dashboard.make_dashboard_rows(
            iter(()), iter(()), iter(()), empty_metadata, Cloud.AWS
        )
        == []
    )


def test_make_dashboard_rows_missing_scan_and_rule_metadata(empty_metadata):
    rows = dashboard.make_dashboard_rows(
        [(RULE, REGION, [_resource().to_cloud_resource()])],
        [],
        [],
        empty_metadata,
        Cloud.AWS,
    )

    assert len(rows) == 1
    assert rows[0]['rule_title'] == ''
    assert rows[0]['severity'] == 'Unknown'
    assert rows[0]['remediation_priority'] == 0
    assert rows[0]['service'] == ''
    assert rows[0]['category'] == ''


@pytest.mark.parametrize('comment', ['', 'scan-comment'])
def test_make_dashboard_rows_selects_metadata_fallback_arguments(
    rule_metadata, comment
):
    metadata = Mock(rule=Mock(return_value=rule_metadata))

    dashboard.make_dashboard_rows(
        [(RULE, REGION, [_resource().to_cloud_resource()])],
        [],
        [],
        metadata,
        Cloud.AWS,
        _scan_metadata(comment=comment),
    )

    metadata.rule.assert_called_once_with(
        RULE,
        comment=comment or None,
        resource='aws.ec2' if comment else None,
    )


@pytest.mark.parametrize('comment', ['', 'scan-comment'])
def test_make_dashboard_rows_caches_metadata_by_rule_and_effective_type(
    rule_metadata, comment
):
    metadata = Mock(rule=Mock(return_value=rule_metadata))
    second_rule = 'ecc-aws-124-check'
    rows = dashboard.make_dashboard_rows(
        findings=[
            (
                rule,
                region,
                [
                    _resource('a', resource_type, region).to_cloud_resource(),
                    _resource('b', resource_type, region).to_cloud_resource(),
                ],
            )
            for rule in (RULE, second_rule)
            for resource_type in ('aws.ec2', 'aws.s3')
            for region in (REGION, 'eu-central-1')
        ],
        statistics=[],
        inventory=[],
        metadata=metadata,
        cloud=Cloud.AWS,
        scan_metadata={
            **_scan_metadata(comment=comment),
            **_scan_metadata(rule=second_rule, comment=comment),
        },
    )

    assert len(rows) == 16
    expected_types = ('aws.ec2', 'aws.s3') if comment else (None,)
    metadata.rule.assert_has_calls(
        [
            call(rule, comment=comment or None, resource=resource_type)
            for rule in (RULE, second_rule)
            for resource_type in expected_types
        ],
        any_order=True,
    )
    assert metadata.rule.call_count == 2 * len(expected_types)


def test_make_dashboard_rows_preserves_first_inventory_resource_details(
    metadata,
):
    finding = _resource(data={'Tags': [{'Key': 'source', 'Value': 'finding'}]})
    inventory = _resource(
        data={'Tags': [{'Key': 'source', 'Value': 'inventory'}]}
    )
    duplicate = _resource(
        data={'Tags': [{'Key': 'source', 'Value': 'duplicate'}]}
    )

    rows = dashboard.make_dashboard_rows(
        [(RULE, REGION, [finding.to_cloud_resource()])],
        [_statistic()],
        [inventory, duplicate],
        metadata,
        Cloud.AWS,
        _scan_metadata(),
    )

    assert len(rows) == 1
    assert rows[0]['check_status'] == 'Failed'
    assert rows[0]['resource_tags'] == '{"source": "inventory"}'


def test_make_dashboard_rows_preserves_first_duplicate_finding(metadata):
    first = _resource(data={'Tags': [{'Key': 'source', 'Value': 'first'}]})
    duplicate = _resource(
        data={'Tags': [{'Key': 'source', 'Value': 'duplicate'}]}
    )
    rows = dashboard.make_dashboard_rows(
        [
            (RULE, REGION, [first.to_cloud_resource()]),
            (RULE, REGION, [duplicate.to_cloud_resource()]),
        ],
        [],
        [],
        metadata,
        Cloud.AWS,
        _scan_metadata(),
    )

    assert len(rows) == 1
    assert rows[0]['resource_tags'] == '{"source": "first"}'


def test_make_dashboard_rows_empty_findings_use_scan_metadata_for_passes(
    metadata,
):
    rows = dashboard.make_dashboard_rows(
        [(RULE, REGION, iter(()))],
        iter([_statistic()]),
        iter([_resource()]),
        metadata,
        Cloud.AWS,
        _scan_metadata(),
    )

    assert [(row['resource_id'], row['check_status']) for row in rows] == [
        ('resource-1', 'Passed')
    ]


def test_make_dashboard_rows_keeps_finding_types_separate(metadata):
    rows = dashboard.make_dashboard_rows(
        [
            (RULE, REGION, [_resource(resource_type=rt).to_cloud_resource()])
            for rt in ('aws.ec2', 'aws.s3')
        ],
        [],
        [],
        metadata,
        Cloud.AWS,
        _scan_metadata(),
    )

    assert [
        (row['resource_type'], row['resource_id'], row['check_status'])
        for row in rows
    ] == [
        ('aws.ec2', 'resource-1', 'Failed'),
        ('aws.s3', 'resource-1', 'Failed'),
    ]


def test_make_dashboard_rows_sorts_by_type_rule_region_and_id(metadata):
    inputs = [
        ('ecc-aws-124-check', 'eu-west-1', 'aws.s3', 'z'),
        (RULE, 'eu-west-1', 'aws.ec2', 'z'),
        (RULE, 'eu-central-1', 'aws.ec2', 'z'),
        ('ecc-aws-124-check', 'eu-west-1', 'aws.ec2', 'a'),
        (RULE, 'eu-west-1', 'aws.ec2', 'a'),
    ]
    rows = dashboard.make_dashboard_rows(
        [
            (
                rule,
                region,
                [
                    _resource(
                        resource_id, resource_type, region
                    ).to_cloud_resource()
                ],
            )
            for rule, region, resource_type, resource_id in inputs
        ],
        [],
        [],
        metadata,
        Cloud.AWS,
    )

    assert [
        (
            row['resource_type'],
            row['rule_id'],
            row['region_namespace'],
            row['resource_id'],
        )
        for row in rows
    ] == [
        ('aws.ec2', 'aws-123', 'eu-central-1', 'z'),
        ('aws.ec2', 'aws-123', 'eu-west-1', 'a'),
        ('aws.ec2', 'aws-123', 'eu-west-1', 'z'),
        ('aws.ec2', 'aws-124', 'eu-west-1', 'a'),
        ('aws.s3', 'aws-124', 'eu-west-1', 'z'),
    ]


@pytest.mark.parametrize(
    'rule_id,expected',
    [
        ('ecc-aws-123-long-description', 'aws-123'),
        ('ecc-aws-123', 'aws-123'),
        ('aws-123-description', 'aws-123'),
        ('ecc-custom-rule', 'custom-rule'),
        ('custom-rule', 'custom-rule'),
    ],
)
def test_row_shortens_rule_id(rule_metadata, rule_id, expected):
    row = dashboard._row(
        rule_id,
        None,
        REGION,
        'Not evaluated',
        rule_metadata,
        _scan_metadata()[RULE],
        Cloud.AWS,
    )

    assert row['rule_id'] == expected


@pytest.mark.parametrize(
    'severity,severity_priority',
    [
        (Severity.HIGH, 40),
        (Severity.MEDIUM, 30),
        (Severity.LOW, 20),
        (Severity.INFO, 10),
        (Severity.UNKNOWN, 0),
    ],
)
@pytest.mark.parametrize(
    'complexity,complexity_priority',
    [
        (RemediationComplexity.LOW, 5),
        (RemediationComplexity.LOW_MEDIUM, 4),
        (RemediationComplexity.MEDIUM, 3),
        (RemediationComplexity.MEDIUM_HIGH, 2),
        (RemediationComplexity.HIGH, 1),
        (RemediationComplexity.UNKNOWN, 0),
    ],
)
def test_row_calculates_priorities(
    severity, severity_priority, complexity, complexity_priority
):
    metadata = RuleMetadata(
        cloud='AWS',
        source='',
        category='',
        service_section='',
        service='',
        article='',
        impact='',
        remediation='',
        severity=severity,
        remediation_complexity=complexity,
    )
    row = dashboard._row(
        RULE,
        None,
        REGION,
        'Not evaluated',
        metadata,
        _scan_metadata()[RULE],
        Cloud.AWS,
    )

    assert row['severity'] == severity.value
    assert row['severity_priority'] == severity_priority
    assert row['remediation_complexity'] == complexity.value
    assert row['remediation_complexity_priority'] == complexity_priority
    assert row['remediation_priority'] == (
        severity_priority + complexity_priority
    )


@pytest.mark.parametrize(
    'cloud,resource_type,expected',
    [
        (Cloud.AWS, 'ec2', 'aws.ec2'),
        (Cloud.AZURE, 'vm', 'azure.vm'),
        (Cloud.GOOGLE, 'instance', 'gcp.instance'),
        (Cloud.KUBERNETES, 'pod', 'k8s.pod'),
        (Cloud.AWS, 'aws.s3', 'aws.s3'),
    ],
)
def test_row_normalizes_placeholder_resource_type(
    rule_metadata, cloud, resource_type, expected
):
    row = dashboard._row(
        RULE,
        None,
        REGION,
        'Not evaluated',
        rule_metadata,
        _scan_metadata(resource_type=resource_type)[RULE],
        cloud,
    )

    assert row['resource_type'] == expected


@pytest.mark.parametrize('namespace', ['application', None, ''])
def test_row_uses_kubernetes_namespace(rule_metadata, namespace):
    resource = _resource(
        resource_type='k8s.pod', region='global', namespace=namespace
    ).to_cloud_resource()
    row = dashboard._row(
        RULE,
        resource,
        'global',
        'Failed',
        rule_metadata,
        _scan_metadata(resource_type='pod')[RULE],
        Cloud.KUBERNETES,
    )

    assert row['region_namespace'] == (namespace or '')


@pytest.mark.parametrize(
    'cloud,resource_type,mapping,expected',
    [
        (Cloud.AWS, '', {}, ''),
        (Cloud.AWS, 'ec2', {'AWS': {'aws.ec2': 'Compute'}}, 'Compute'),
        (Cloud.AWS, 'aws.ec2', {'AWS': {'aws.ec2': 'Compute'}}, 'Compute'),
        (Cloud.AWS, 'aws.ec2', {}, 'Ec2'),
        (Cloud.AWS, 'ec2', {'AZURE': {}}, 'Ec2'),
        (Cloud.AWS, 'ec2', {'AWS': {}}, 'Ec2'),
        (Cloud.AWS, 'ec2', {'AWS': {'aws.ec2': ''}}, 'Ec2'),
        (Cloud.AZURE, 'vm', {'AZURE': {'azure.vm': 'VM'}}, 'VM'),
        (Cloud.GOOGLE, 'instance', {'GOOGLE': {'gcp.instance': 'VM'}}, 'VM'),
        (
            Cloud.KUBERNETES,
            'pod',
            {'KUBERNETES': {'k8s.pod': 'Pods'}},
            'Pods',
        ),
    ],
)
def test_resolve_resource_type_name(cloud, resource_type, mapping, expected):
    assert (
        dashboard.resolve_resource_type_name(resource_type, cloud, mapping)
        == expected
    )


@pytest.mark.parametrize('platform_id', [None, 'platform-id'])
@pytest.mark.parametrize('page_count', [0, 1, 3])
def test_iter_tenant_inventory_paginates(platform_id, page_count):
    resources = [_resource(str(index)) for index in range(page_count)]
    pages = (
        [_page()]
        if not resources
        else [
            _page(
                [resource],
                {'cursor': index + 1} if index + 1 < page_count else None,
            )
            for index, resource in enumerate(resources)
        ]
    )
    resources_service = Mock(get_resources=Mock(side_effect=pages))

    assert (
        list(
            dashboard._iter_tenant_inventory(
                resources_service, 'tenant', 'customer', platform_id
            )
        )
        == resources
    )
    common = {
        'tenant_name': 'tenant',
        'customer_name': 'customer',
        'platform_id': platform_id,
        'limit': 50,
    }
    assert resources_service.get_resources.call_args_list == [
        call(**common),
        *[
            call(**common, last_evaluated_key={'cursor': index})
            for index in range(1, page_count)
        ],
    ]


def test_iter_tenant_inventory_continues_after_empty_page():
    resource = _resource()
    resources_service = Mock(
        get_resources=Mock(
            side_effect=[
                _page(last_evaluated_key={'cursor': 1}),
                _page([resource]),
            ]
        )
    )

    assert list(
        dashboard._iter_tenant_inventory(
            resources_service, 'tenant', 'customer'
        )
    ) == [resource]
    assert resources_service.get_resources.call_args_list[-1] == call(
        tenant_name='tenant',
        customer_name='customer',
        platform_id=None,
        limit=50,
        last_evaluated_key={'cursor': 1},
    )


@pytest.mark.parametrize('is_platform', [False, True])
def test_collect_raw_data_persists_transformed_rows(
    service,
    dependencies,
    job,
    aws_tenant,
    k8s_platform,
    metadata,
    monkeypatch,
    is_platform,
):
    cloud = Cloud.KUBERNETES if is_platform else Cloud.AWS
    if is_platform:
        job.platform_id = k8s_platform.id
        keys = PlatformReportsBucketKeysBuilder(k8s_platform)
    else:
        keys = TenantReportsBucketKeysBuilder(aws_tenant)
    region = 'global' if is_platform else REGION
    resource_type = 'k8s.pod' if is_platform else 'aws.ec2'
    resource = _resource(resource_type=resource_type, region=region)
    collection = ShardsCollectionFactory.from_cloud(cloud)
    collection.meta = _scan_metadata(resource_type=resource_type)
    findings = Mock(
        return_value=iter([(RULE, region, [resource.to_cloud_resource()])])
    )
    monkeypatch.setattr(dashboard, 'iter_rule_region_resources', findings)
    dependencies.resources_service.get_resources.return_value = _page(
        [
            resource,
            _resource('passing', resource_type, region),
        ]
    )

    service.collect_raw_data(
            job, aws_tenant, collection, [_statistic(region=region)], keys
    )
    findings.assert_called_once_with(collection, cloud, metadata)
    dependencies.license_service.get_customer_metadata.assert_called_once_with(
        aws_tenant.customer_name
    )
    dependencies.resources_service.get_resources.assert_called_once_with(
        tenant_name=aws_tenant.name,
        customer_name=aws_tenant.customer_name,
        platform_id=k8s_platform.id if is_platform else None,
        limit=50,
    )
    saved = dependencies.s3_client.gz_put_json.call_args
    assert dependencies.s3_client.gz_put_json.call_count == 1
    assert saved.kwargs['bucket'] == 'reports'
    assert saved.kwargs['key'] == keys.job_dashboard_raw(job)
    assert [
        (row['resource_id'], row['check_status'])
        for row in saved.kwargs['obj']
    ] == [('passing', 'Passed'), ('resource-1', 'Failed')]
    assert all(
        row['resource_type'] == resource_type for row in saved.kwargs['obj']
    )


def test_collect_raw_data_passes_transformation_inputs_and_persists_empty_rows(
    service,
    dependencies,
    job,
    aws_tenant,
    metadata,
    monkeypatch,
):
    collection = ShardsCollectionFactory.from_cloud(Cloud.AWS)
    collection.meta = _scan_metadata()
    statistics = iter([_statistic()])
    findings = iter(())
    inventory = iter(())
    iter_findings = Mock(return_value=findings)
    iter_inventory = Mock(return_value=inventory)
    make_rows = Mock(return_value=[])
    monkeypatch.setattr(dashboard, 'iter_rule_region_resources', iter_findings)
    monkeypatch.setattr(dashboard, '_iter_tenant_inventory', iter_inventory)
    monkeypatch.setattr(dashboard, 'make_dashboard_rows', make_rows)
    keys = TenantReportsBucketKeysBuilder(aws_tenant)

    service.collect_raw_data(job, aws_tenant, collection, statistics, keys)

    iter_inventory.assert_called_once_with(
        dependencies.resources_service,
        aws_tenant.name,
        aws_tenant.customer_name,
        None,
    )
    iter_findings.assert_called_once_with(collection, Cloud.AWS, metadata)
    make_rows.assert_called_once_with(
        findings=findings,
        statistics=statistics,
        inventory=inventory,
        metadata=metadata,
        cloud=Cloud.AWS,
        scan_metadata=collection.meta,
    )
    dependencies.s3_client.gz_put_json.assert_called_once_with(
        bucket='reports',
        key=keys.job_dashboard_raw(job),
        obj=[],
    )


@pytest.mark.parametrize(
    'failing_stage', ['metadata', 'inventory', 'findings', 'rows', 'storage']
)
def test_collect_raw_data_propagates_dependency_failures(
    service,
    dependencies,
    job,
    aws_tenant,
    monkeypatch,
    failing_stage,
):
    error = RuntimeError(f'{failing_stage} unavailable')
    findings = Mock(return_value=iter(()))
    monkeypatch.setattr(dashboard, 'iter_rule_region_resources', findings)
    if failing_stage == 'metadata':
        dependencies.license_service.get_customer_metadata.side_effect = error
    elif failing_stage == 'inventory':
        dependencies.resources_service.get_resources.side_effect = error
    elif failing_stage == 'findings':
        findings.side_effect = error
    elif failing_stage == 'rows':
        monkeypatch.setattr(
            dashboard, 'make_dashboard_rows', Mock(side_effect=error)
        )
    else:
        dependencies.s3_client.gz_put_json.side_effect = error

    with pytest.raises(RuntimeError) as raised:
        service.collect_raw_data(
            job,
            aws_tenant,
            ShardsCollectionFactory.from_cloud(Cloud.AWS),
            [],
            TenantReportsBucketKeysBuilder(aws_tenant),
        )

    assert raised.value is error
    if failing_stage == 'storage':
        assert dependencies.s3_client.gz_put_json.call_count == 1
    else:
        dependencies.s3_client.gz_put_json.assert_not_called()


@pytest.mark.parametrize(
    'status,stopped_at',
    [
        (JobState.SUBMITTED, None),
        (JobState.RUNNING, None),
        (JobState.FAILED, '2025-01-01T00:02:00Z'),
        (JobState.INTERRUPTED, '2025-01-01T00:02:00Z'),
        (JobState.SUCCEEDED, None),
    ],
)
def test_generate_rejects_unsuccessful_or_unfinished_jobs(
    service,
    dependencies,
    job,
    aws_tenant,
    renderer,
    status,
    stopped_at,
):
    job.status = status.value
    job.stopped_at = stopped_at

    with pytest.raises(dashboard.DashboardReportError) as raised:
        service.generate(job, aws_tenant)

    assert raised.value.status_code == 400
    assert str(raised.value) == (
        'Dashboard reports require a completed successful job'
    )
    assert dependencies.s3_client.mock_calls == []
    dependencies.platform_service.get_nullable.assert_not_called()
    renderer.assert_not_called()


@pytest.mark.parametrize('field', ['tenant_name', 'customer_name'])
def test_generate_rejects_job_ownership_mismatch(
    service,
    dependencies,
    job,
    aws_tenant,
    renderer,
    field,
):
    setattr(job, field, 'different')

    with pytest.raises(dashboard.DashboardReportError) as raised:
        service.generate(job, aws_tenant)

    assert raised.value.status_code == 400
    assert str(raised.value) == (
        'The requested job does not belong to this tenant'
    )
    assert dependencies.s3_client.mock_calls == []
    dependencies.platform_service.get_nullable.assert_not_called()
    renderer.assert_not_called()


def test_generate_rejects_missing_platform(
    service,
    dependencies,
    job,
    aws_tenant,
    renderer,
):
    job.platform_id = 'missing'
    dependencies.platform_service.get_nullable.return_value = None

    with pytest.raises(dashboard.DashboardReportError) as raised:
        service.generate(job, aws_tenant)

    assert raised.value.status_code == 404
    assert str(raised.value) == (
        "Platform 'missing' not found for this tenant"
    )
    dependencies.platform_service.get_nullable.assert_called_once_with(
        hash_key='missing'
    )
    dependencies.s3_client.gz_get_json.assert_not_called()
    renderer.assert_not_called()


@pytest.mark.parametrize(
    'error', [ValueError('invalid JSON'), OSError('read')]
)
def test_generate_wraps_raw_data_read_failure(
    service,
    dependencies,
    job,
    aws_tenant,
    renderer,
    error,
):
    dependencies.s3_client.gz_get_json.side_effect = error

    with pytest.raises(dashboard.DashboardReportError) as raised:
        service.generate(job, aws_tenant)

    assert raised.value.status_code == 400
    assert str(raised.value) == (
        f'Dashboard raw data is invalid for job {job.id}'
    )
    assert raised.value.__cause__ is error
    dependencies.settings_service.get.assert_not_called()
    dependencies.s3_client.get_object.assert_not_called()
    renderer.assert_not_called()


@pytest.mark.parametrize('rows', [None, []])
def test_generate_rejects_unavailable_raw_data(
    service,
    dependencies,
    job,
    aws_tenant,
    renderer,
    rows,
):
    dependencies.s3_client.gz_get_json.return_value = rows

    with pytest.raises(dashboard.DashboardReportError) as raised:
        service.generate(job, aws_tenant)

    assert raised.value.status_code == 404
    assert str(raised.value) == (
        f'Dashboard raw data is unavailable for job {job.id}'
    )
    dependencies.settings_service.get.assert_not_called()
    dependencies.s3_client.get_object.assert_not_called()
    renderer.assert_not_called()


def test_generate_rejects_unavailable_template(
    service,
    dependencies,
    job,
    aws_tenant,
    renderer,
):
    dependencies.s3_client.gz_get_json.return_value = [
        {'resource_type': 'aws.ec2'}
    ]
    dependencies.s3_client.get_object.return_value = None

    with pytest.raises(dashboard.DashboardReportError) as raised:
        service.generate(job, aws_tenant)

    assert raised.value.status_code == 503
    assert str(raised.value) == (
        f'Dashboard workbook template is missing at {dashboard.TEMPLATE_KEY}'
    )
    dependencies.s3_client.get_object.assert_called_once_with(
        bucket='reports',
        key=dashboard.TEMPLATE_KEY,
    )
    renderer.assert_not_called()


def test_generate_wraps_delegated_renderer_failure(
    service,
    dependencies,
    job,
    aws_tenant,
    renderer,
):
    dependencies.s3_client.gz_get_json.return_value = [
        {'resource_type': 'aws.ec2'}
    ]
    error = ValueError('incompatible input')
    renderer.side_effect = error

    with pytest.raises(dashboard.DashboardReportError) as raised:
        service.generate(job, aws_tenant)

    assert raised.value.status_code == 503
    assert str(raised.value) == (
        'Incompatible dashboard workbook template: incompatible input'
    )
    assert raised.value.__cause__ is error
    assert renderer.call_count == 1


@pytest.mark.parametrize('is_platform', [False, True])
@pytest.mark.parametrize('has_names_map', [False, True])
def test_generate_delegates_rows_and_job_details(
    service,
    dependencies,
    job,
    aws_tenant,
    k8s_platform,
    renderer,
    is_platform,
    has_names_map,
):
    if is_platform:
        job.platform_id = k8s_platform.id
        dependencies.platform_service.get_nullable.return_value = k8s_platform
        keys = PlatformReportsBucketKeysBuilder(k8s_platform)
        resource_type = 'k8s.pod'
        configured_name = 'Pods'
        fallback_name = 'Pod'
    else:
        keys = TenantReportsBucketKeysBuilder(aws_tenant)
        resource_type = 'aws.ec2'
        configured_name = 'Compute'
        fallback_name = 'Ec2'
    row = {'resource_type': resource_type, 'resource_id': 'resource-1'}
    dependencies.s3_client.gz_get_json.return_value = [row]
    dependencies.settings_service.get.return_value = (
        {keys.cloud.value: {resource_type: configured_name}}
        if has_names_map
        else None
    )

    result = service.generate(job, aws_tenant)

    assert result is renderer.return_value
    dependencies.s3_client.gz_get_json.assert_called_once_with(
        bucket='reports',
        key=keys.job_dashboard_raw(job),
    )
    dependencies.s3_client.get_object.assert_called_once_with(
        bucket='reports',
        key=dashboard.TEMPLATE_KEY,
    )
    dependencies.settings_service.get.assert_called_once_with(
        SettingKey.CUSTODIAN_RESOURCE_NAMES_MAP
    )
    expected_details = {
        'Job ID': job.id,
        'Tenant name': aws_tenant.display_name,
        'Cloud': keys.cloud.value,
        'Submitted at': job.submitted_at,
        'Started at': job.started_at,
        'Finished at': job.stopped_at,
    }
    if is_platform:
        expected_details['Platform'] = k8s_platform.description
        dependencies.platform_service.get_nullable.assert_called_once_with(
            hash_key=k8s_platform.id
        )
    else:
        expected_details['Account ID'] = aws_tenant.project
        dependencies.platform_service.get_nullable.assert_not_called()
    renderer.assert_called_once_with(
        dependencies.s3_client.get_object.return_value,
        [
            {
                'resource_type': (
                    configured_name if has_names_map else fallback_name
                ),
                'resource_id': 'resource-1',
            }
        ],
        job_details=expected_details,
    )
    dependencies.resources_service.get_resources.assert_not_called()
    dependencies.license_service.get_customer_metadata.assert_not_called()
    dependencies.s3_client.gz_put_json.assert_not_called()


def test_generate_uses_display_name_account_and_started_at_fallbacks(
    service,
    dependencies,
    job,
    aws_tenant,
    renderer,
):
    aws_tenant.display_name = None
    aws_tenant.project = None
    job.started_at = None
    dependencies.s3_client.gz_get_json.return_value = [
        {'resource_type': 'aws.ec2'}
    ]

    service.generate(job, aws_tenant)

    details = renderer.call_args.kwargs['job_details']
    assert details['Tenant name'] == aws_tenant.name
    assert details['Account ID'] == 'Not recorded'
    assert details['Started at'] == 'Not recorded'
