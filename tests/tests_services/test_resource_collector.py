from c7n.resources.resource_map import ResourceMap as AWSResourceMap
from c7n_azure.resources.resource_map import ResourceMap as AzureResourceMap
from c7n_gcp.resources.resource_map import ResourceMap as GCPResourceMap

from services.resources_collector import ResourceCollector

def test_aws_policies():
    policies = ResourceCollector.get_aws_policies()

    assert all(policy.resource_type.startswith('aws.') for policy in policies)
    assert all(policy.resource_type in AWSResourceMap for policy in policies)

def test_azure_policies():
    policies = ResourceCollector.get_azure_policies()

    assert all(policy.resource_type.startswith('azure.') for policy in policies)
    assert all(policy.resource_type in AzureResourceMap for policy in policies)

def test_gcp_policies():
    policies = ResourceCollector.get_gcp_policies()

    assert all(policy.resource_type.startswith('gcp.') for policy in policies)
    assert all(policy.resource_type in GCPResourceMap for policy in policies)