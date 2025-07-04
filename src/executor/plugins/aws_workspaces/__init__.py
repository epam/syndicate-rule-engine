import re

import jmespath
from c7n.filters import ValueFilter, OPERATORS
from c7n.utils import local_session, type_schema


class SecurityGroupWorkspaceFilter(ValueFilter):
    schema = type_schema('security-group-workspace-filter', rinherit=ValueFilter.schema)
    permissions = ('workspaces:DescribeWorkspaces',)

    def process(self, resources, event=None):
        client = local_session(self.manager.session_factory).client('ec2')
        resulted = []
        sec_groups = client.describe_security_groups()['SecurityGroups']
        for resource in resources:
            subnet_id = resource['SubnetId']
            ip_address = resource['IpAddress']
            interfaces = client.describe_network_interfaces(
                Filters=[{'Name': 'private-ip-address', 'Values': [
                    ip_address]}, {'Name': 'subnet-id',
                                   'Values': [subnet_id]}])['NetworkInterfaces'][0]
            for interface in interfaces['Groups']:
                valid_group = self._check_if_group_is_valid(sec_groups, interface)
                if valid_group and self._check_conditions_for_valid_group(valid_group):
                    resulted.append(resource)
                    break
        return resulted

    def _check_if_group_is_valid(self, sec_groups, interface):
        for group in sec_groups:
            if interface['GroupId'] == group['GroupId']:
                return group
        return False

    def _check_conditions_for_valid_group(self, sec_group):
        return jmespath.search(self.data.get('key'), sec_group) and self._perform_op(
            jmespath.search(self.data.get('key'), sec_group), self.data.get('value'))

    def _perform_op(self, a, b):
        op = OPERATORS[self.data.get('op', 'eq')]
        return op(a, b)


class WorkspacesDirectoryVpcEndpointFilter(ValueFilter):
    """Filter workspace directories based on vpc endpoints availability.

    :example:

    .. code-block:: yaml

       policies:
         - name: workspace-directories-vpc-endpoints
           resource: aws.workspaces-directory
           filters:
            - type: check-vpc-endpoints-availability

    """
    permissions = ('workspaces:DescribeClientProperties',)

    schema = type_schema('check-vpc-endpoints-availability')

    def process(self, directories, event=None):
        ds_client = local_session(self.manager.session_factory).client('ds')
        ec2_client = local_session(self.manager.session_factory).client('ec2')
        results = []
        for directory in directories:
            try:
                vpc_settings = ds_client.describe_directories(
                    DirectoryIds=[directory['DirectoryId']]).get(
                    'DirectoryDescriptions')[0].get('VpcSettings')
                if vpc_settings:
                    vpc_id = vpc_settings.get('VpcId')
                    vpc_endpoints = ec2_client.describe_vpc_endpoints(Filters=[{
                        'Name': 'vpc-id',
                        'Values': [vpc_id]
                    }]).get('VpcEndpoints')
                    if len(vpc_endpoints) == 0 or not (
                        vpc_endpoints[0].get('VpcEndpointType') == 'Interface'
                        and re.match('^com\\.amazonaws\\..*\\.workspaces$',
                                     vpc_endpoints[0].get('ServiceName'))
                        and vpc_endpoints[0].get('State') == 'available'):
                        results.append(directory)
            except Exception:
                continue
        return results

def register() -> None:
    from c7n.resources.workspaces import Workspace, WorkspaceDirectory

    Workspace.filter_registry.register('security-group-workspace-filter', SecurityGroupWorkspaceFilter)
    WorkspaceDirectory.filter_registry.register('check-vpc-endpoints-availability', WorkspacesDirectoryVpcEndpointFilter)
