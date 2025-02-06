from datetime import datetime, timezone

import pytest

from handlers.high_level_reports_handler import MaestroModelBuilder
from helpers.constants import COMPOUND_KEYS_SEPARATOR, ReportType
from models.metrics import ReportMetrics


@pytest.fixture
def build_customer_report():
    def factory(type_: ReportType, now: datetime):
        return ReportMetrics(
            key=COMPOUND_KEYS_SEPARATOR.join(
                (type_.value, 'TEST_CUSTOMER', '', '', '', '')
            ),
            end=type_.end(now),
            start=type_.start(now),
            customer='TEST_CUSTOMER',
        )

    return factory


class TestMaestroModelBuilder:
    def test_top_compliance_by_cloud(self, build_customer_report):
        now = datetime(2025, 2, 3, 13, 11, 6, 116366, tzinfo=timezone.utc)
        rep = build_customer_report(
            ReportType.DEPARTMENT_TOP_COMPLIANCE_BY_CLOUD, now
        )
        res = MaestroModelBuilder().convert(
            rep=rep,
            data={
                'data': {
                    'AWS': [
                        {
                            'data': {'Standard1 v1.0.0': 0.5},
                            'sort_by': 0.5,
                            'tenant_display_name': 'testing1',
                        },
                        {
                            'data': {'Standard1 v2.0.0': 0.25},
                            'sort_by': 0.25,
                            'tenant_display_name': 'testing2',
                        },
                    ],
                    'AZURE': [
                        {
                            'data': {'HIPAA': 0.2},
                            'sort_by': 0.2,
                            'tenant_display_name': 'testing1',
                        }
                    ],
                },
                'outdated_tenants': [],
            },
            previous_data={
                'data': {
                    'AWS': [
                        {
                            'data': {'not_existing': 0.25},
                            'sort_by': 0.25,
                            'tenant_display_name': 'testing2',
                        },
                        {
                            'data': {'Standard1 v1.0.0': 0.3},
                            'sort_by': 0.3,
                            'tenant_display_name': 'testing1',
                        },
                    ]
                },
                'outdated_tenants': [],
            },
        )
        assert res == {
            'receivers': (),
            'report_type': 'COMPLIANCE_BY_CLOUD',
            'customer': 'TEST_CUSTOMER',
            'from': datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc),
            'to': datetime(2025, 2, 1, 0, 0, tzinfo=timezone.utc),
            'outdated_tenants': [],
            'externalData': False,
            'data': {
                'AWS': [
                    {
                        'data': [
                            {
                                'name': 'Standard1 v1.0.0',
                                'value': 50.0,
                                'diff': 20.0,
                            }
                        ],
                        'sort_by': 0.5,
                        'tenant_display_name': 'testing1',
                    },
                    {
                        'data': [{'name': 'Standard1 v2.0.0', 'value': 25.0}],
                        'sort_by': 0.25,
                        'tenant_display_name': 'testing2',
                    },
                ],
                'AZURE': [
                    {
                        'data': [{'name': 'HIPAA', 'value': 20.0}],
                        'sort_by': 0.2,
                        'tenant_display_name': 'testing1',
                    }
                ],
            },
        }

    def test_top_resources_by_cloud(self, build_customer_report):
        now = datetime(2025, 2, 3, 13, 11, 6, 116366, tzinfo=timezone.utc)
        rep = build_customer_report(
            ReportType.DEPARTMENT_TOP_RESOURCES_BY_CLOUD, now
        )
        res = MaestroModelBuilder().convert(
            rep=rep,
            data={
                'data': {
                    'AWS': [
                        {
                            'data': {
                                'activated_regions': [
                                    'eu-central-1',
                                    'eu-north-1',
                                    'eu-west-1',
                                    'eu-west-3',
                                ],
                                'failed_scans': 1,
                                'last_scan_date': '2025-02-01T00:14:37.917014Z',
                                'resource_types_data': {'Cloudtrail': 1},
                                'resources_violated': 1,
                                'severity_data': {'Unknown': 1},
                                'succeeded_scans': 2,
                                'tenant_name': 'AWS-TESTING',
                                'total_scans': 3,
                            },
                            'sort_by': 1,
                            'tenant_display_name': 'testing1',
                        },
                        {
                            'data': {
                                'activated_regions': [
                                    'eu-central-1',
                                    'eu-north-1',
                                    'eu-west-1',
                                    'eu-west-3',
                                ],
                                'failed_scans': 1,
                                'last_scan_date': '2025-02-01T00:14:37.917014Z',
                                'resource_types_data': {'Cloudtrail': 2},
                                'resources_violated': 2,
                                'severity_data': {'Unknown': 2},
                                'succeeded_scans': 1,
                                'tenant_name': 'AWS-TESTING-2',
                                'total_scans': 3,
                            },
                            'sort_by': 1,
                            'tenant_display_name': 'testing2',
                        },
                    ]
                },
                'outdated_tenants': [],
            },
            previous_data={
                'data': {
                    'AWS': [
                        {
                            'data': {
                                'activated_regions': [
                                    'eu-central-1',
                                    'eu-north-1',
                                    'eu-west-1',
                                    'eu-west-3',
                                ],
                                'failed_scans': 1,
                                'last_scan_date': '2025-02-01T00:14:37.917014Z',
                                'resource_types_data': {'Cloudtrail': 1},
                                'resources_violated': 1,
                                'severity_data': {'Unknown': 1},
                                'succeeded_scans': 2,
                                'tenant_name': 'AWS-TESTING-2',
                                'total_scans': 3,
                            },
                            'sort_by': 1,
                            'tenant_display_name': 'testing2',
                        }
                    ]
                },
                'outdated_tenants': [],
            },
        )
        assert res == {
            'receivers': (),
            'report_type': 'RESOURCES_BY_CLOUD',
            'customer': 'TEST_CUSTOMER',
            'from': datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc),
            'to': datetime(2025, 2, 1, 0, 0, tzinfo=timezone.utc),
            'outdated_tenants': [],
            'externalData': False,
            'data': {
                'AWS': [
                    {
                        'data': {
                            'activated_regions': [
                                'eu-central-1',
                                'eu-north-1',
                                'eu-west-1',
                                'eu-west-3',
                            ],
                            'failed_scans': {'value': 1},
                            'last_scan_date': '2025-02-01T00:14:37.917014Z',
                            'resource_types_data': {
                                'Cloudtrail': {'value': 1}
                            },
                            'resources_violated': {'value': 1},
                            'severity_data': {'Unknown': {'value': 1}},
                            'succeeded_scans': {'value': 2},
                            'tenant_name': 'AWS-TESTING',
                            'total_scans': {'value': 3},
                        },
                        'sort_by': 1,
                        'tenant_display_name': 'testing1',
                    },
                    {
                        'data': {
                            'activated_regions': [
                                'eu-central-1',
                                'eu-north-1',
                                'eu-west-1',
                                'eu-west-3',
                            ],
                            'failed_scans': {'value': 1, 'diff': 0},
                            'last_scan_date': '2025-02-01T00:14:37.917014Z',
                            'resource_types_data': {
                                'Cloudtrail': {'value': 2, 'diff': 1}
                            },
                            'resources_violated': {'value': 2, 'diff': 1},
                            'severity_data': {
                                'Unknown': {'value': 2, 'diff': 1}
                            },
                            'succeeded_scans': {'value': 1, 'diff': -1},
                            'tenant_name': 'AWS-TESTING-2',
                            'total_scans': {'value': 3, 'diff': 0},
                        },
                        'sort_by': 1,
                        'tenant_display_name': 'testing2',
                    },
                ]
            },
        }
