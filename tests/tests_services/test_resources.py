from types import SimpleNamespace

import pytest

from helpers.constants import Cloud
from services import resources
from services.resources import service_to_resource_type


@pytest.mark.parametrize(
    ('service', 'cloud', 'expected'),
    [
        ('ClusterRole', Cloud.KUBERNETES, 'k8s.cluster-role'),
        ('ConfigMap', Cloud.KUBERNETES, 'k8s.config-map'),
        ('Deployment', Cloud.KUBERNETES, 'k8s.deployment'),
        ('Namespace', Cloud.KUBERNETES, 'k8s.namespace'),
        ('ClusterRole',Cloud.KUBERNETES, 'k8s.cluster-role'),
        ('Pod', Cloud.KUBERNETES, 'k8s.pod'),
        ('Pod', Cloud.K8S, 'k8s.pod'),
    ],
)
def test_service_to_resource_type_prefixes_kebab_case(service, cloud, expected):
    assert service_to_resource_type(service, cloud) == expected


def test_iter_rule_region_resources_uses_description_for_deprecation(monkeypatch):
    monkeypatch.setattr(resources, 'load_cc_providers', lambda: None)
    collection = SimpleNamespace(
        meta={
            'stable-deprecated-name': {
                'description': 'Deprecated in prose, but not marked',
                'resource': 'aws.s3',
            },
            'stable-rule-id': {
                'description': '[DEPRECATED] This rule is retired',
                'resource': 'aws.s3',
            },
        },
        iter_parts=lambda: iter(
            (
                SimpleNamespace(
                    policy='stable-deprecated-name',
                    location='us-east-1',
                    resources=[],
                ),
                SimpleNamespace(
                    policy='stable-rule-id',
                    location='us-east-1',
                    resources=[],
                ),
            )
        ),
    )

    resources_found = list(
        resources.iter_rule_region_resources(collection, Cloud.AWS)
    )

    assert [policy for policy, _, _ in resources_found] == [
        'stable-deprecated-name'
    ]
