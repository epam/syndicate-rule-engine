from importlib import import_module

from boto3 import Session
from azure.identity import AzureCliCredential
from azure.mgmt.resource import ResourceManagementClient

from c7n.resources import load_available
from c7n.resources.resource_map import ResourceMap as AWSResourceMap
from c7n_azure.resources.resource_map import ResourceMap as AzureResourceMap

load_available()

def get_aws_custodian_supported_types():
    for _, resource_class in AWSResourceMap.items():
        parts = resource_class.split('.')
        module = import_module('.'.join(parts[:-1]))
        resource_class = getattr(module, parts[-1])
        if hasattr(resource_class.resource_type, 'name'):
            r_t = resource_class.resource_type.service.lower()
            if hasattr(resource_class.resource_type, 'arn_type') \
               and resource_class.resource_type.arn_type:
                r_t += ":" + resource_class.resource_type.arn_type.lower()
            yield r_t

def get_all_aws_resource_types():
    session = Session()
    client = session.client('resource-explorer-2')
    paginator = client.get_paginator('list_supported_resource_types')
    for page in paginator.paginate():
        for resource_type in page['ResourceTypes']:
            yield resource_type['ResourceType'].lower()

def get_azure_custodian_supported_types():
    for _, resource_class in AzureResourceMap.items():
        parts = resource_class.split('.')
        module = import_module('.'.join(parts[:-1]))
        resource_class = getattr(module, parts[-1])
        if hasattr(resource_class.resource_type, 'resource_type'):
            yield resource_class.resource_type.resource_type.lower()

def get_all_azure_resource_types():
    credential = AzureCliCredential()
    
    subscription_id ="dcd55c7a-80c4-4c81-b360-f15c2ac10154"
    
    resource_client = ResourceManagementClient(credential, subscription_id)
    
    providers = resource_client.providers.list()
    
    for provider in providers:
        provider_namespace = provider.namespace
        
        for resource_type in provider.resource_types:
            full_resource_type = f"{provider_namespace}/{resource_type.resource_type}".lower()
            yield full_resource_type

aws_custodian_set = set(get_aws_custodian_supported_types())
aws_set = set(get_all_aws_resource_types())

print("======= AWS Resource Types Comparison ========")
print(f"Cloud Custodian supports {len(aws_custodian_set)} resource types.")
print(f"AWS Resource Explorer supports {len(aws_set)} resource types.")
diff = aws_custodian_set - aws_set
print(f"Difference: {len(diff)} resource types.")
if diff:
    print("AWS Resource Types not supported by Cloud Custodian:")
    for resource_type in sorted(diff):
        print(resource_type)
else:
    print("All AWS Resource Types are supported by Cloud Custodian.")

azure_custodian_set = set(get_azure_custodian_supported_types())
azure_set = set(get_all_azure_resource_types())

print("======= Azure Resource Types Comparison ========")
print(f"Cloud Custodian supports {len(azure_custodian_set)} Azure Resource Types.")
print(f"Azure SDK supports {len(azure_set)} Azure Resource Types.")

diff = azure_custodian_set - azure_set
print(f"Difference: {len(diff)} resource.")
if diff:
    print("Azure Resource Types not supported by Cloud Custodian:")
    for resource_type in sorted(diff):
        print(resource_type)
else:
    print("All Azure Resource Types are supported by Cloud Custodian.")

    
