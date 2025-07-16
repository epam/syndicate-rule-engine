from unittest.mock import patch, MagicMock

from c7n.resources.resource_map import ResourceMap as AWSResourceMap
from c7n_azure.resources.resource_map import ResourceMap as AzureResourceMap
from c7n_gcp.resources.resource_map import ResourceMap as GCPResourceMap

from services.resources_collector import CustodianResourceCollector

def test_aws_policies():
    policies = CustodianResourceCollector.get_aws_policies()

    assert all(policy.resource_type.startswith('aws.') for policy in policies)
    assert all(policy.resource_type in AWSResourceMap for policy in policies)

@patch('c7n_azure.session.AzureCredential')
@patch('azure.identity._credentials.azure_cli._run_command')
def test_azure_policies(mock_run_command, mock_credential):
    mock_run_command.return_value = '{"id": "test-subscription", "name": "Test Subscription"}'
    
    mock_credential_instance = MagicMock()
    mock_credential.return_value = mock_credential_instance
    
    policies = CustodianResourceCollector.get_azure_policies()

    assert all(policy.resource_type.startswith('azure.') for policy in policies)
    assert all(policy.resource_type in AzureResourceMap for policy in policies)

def test_gcp_policies():
    policies = CustodianResourceCollector.get_gcp_policies()

    assert all(policy.resource_type.startswith('gcp.') for policy in policies)
    assert all(policy.resource_type in GCPResourceMap for policy in policies)