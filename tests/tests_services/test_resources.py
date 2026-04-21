import pytest

from helpers.constants import Cloud
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
