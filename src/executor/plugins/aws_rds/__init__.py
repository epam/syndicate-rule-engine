import jmespath

from c7n.filters import (
    Filter)
from c7n.filters import OPERATORS
from executor.plugins.aws_vpc import CidrEgressPortRangeNetworkAclFilter

from c7n.utils import local_session, type_schema


class EndpointPortRdsFilter(CidrEgressPortRangeNetworkAclFilter):
    """Filter RDS DB instances by the allowed endpoint ports.

    :example:

    .. code-block:: yaml

            policies:
              - name: rds-endpoint-port
                resource: aws.rds
                filters:
                  - type: endpoint-port
                    required-ports: 1433, 3306, 5432
    """
    schema = type_schema('endpoint-port',
                         rinherit=CidrEgressPortRangeNetworkAclFilter.schema)

    def __init__(self, data, manager):
        super().__init__(data, manager)
        data[
            'egress'] = CidrEgressPortRangeNetworkAclFilter.match_all_valid_value
        data[
            'cidr'] = CidrEgressPortRangeNetworkAclFilter.match_all_valid_value
        data[
            'rule-action'] = CidrEgressPortRangeNetworkAclFilter.match_all_valid_value

    def _valid_entries_jmespath(self):
        return '[Endpoint]'

    def _remap_entries_to_port_ranges(self, entries):
        if len(entries):
            # A single entry is expected, see _valid_entries_jmespath.
            port = entries[0]['Port']
            return f'{port}-{port}'
        return ''


class VpcSecurityGroupRdsFilter(CidrEgressPortRangeNetworkAclFilter):
    """Filter RDS DB instances by the allowed inbound ports in the associated VPC security groups.

    :example:

    .. code-block:: yaml

            policies:
              - name: rds-vpc-security-group-inbound-ports
                resource: aws.rds
                filters:
                  - type: vpc-security-group-inbound-ports
                    required-ports: 1433, 3306, 5432
    """
    schema = type_schema('vpc-security-group-inbound-ports',
                         rinherit=CidrEgressPortRangeNetworkAclFilter.schema)

    def __init__(self, data, manager):
        super().__init__(data, manager)
        # Handle IpPermissionsEgress before setting 'egress' to something other than '*'.
        data[
            'egress'] = CidrEgressPortRangeNetworkAclFilter.match_all_valid_value
        data[
            'cidr'] = CidrEgressPortRangeNetworkAclFilter.match_all_valid_value
        data[
            'rule-action'] = CidrEgressPortRangeNetworkAclFilter.match_all_valid_value

    def _valid_entries_jmespath(self):
        # As only inbound rules are of interest, IpPermissionsEgress[] are ignored.
        return 'IpPermissions[]'

    def _port_from_key(self):
        return 'FromPort'

    def _port_to_key(self):
        return 'ToPort'

    def _port_range_jmespath(self):
        return '@'

    def process(self, resources, event=None):
        valid_resources = []
        ec2_client = local_session(self.manager.session_factory).client('ec2')
        for resource in resources:
            security_groups = self.fetch_vpc_security_groups(resource,
                                                             ec2_client)
            if super(VpcSecurityGroupRdsFilter, self).process(security_groups):
                valid_resources.append(resource)
        return valid_resources

    def fetch_vpc_security_groups(self, rds_resource, ec2_client):
        security_group_definitions = jmespath.search('VpcSecurityGroups[]',
                                                     rds_resource)
        security_group_ids = [r['VpcSecurityGroupId'] for r in
                              security_group_definitions
                              if r['Status'] == 'active']
        # Using Filters instead of GroupIds as SecurityGroups is present even for 0 records.
        sg_filters = [{'Name': 'group-id', 'Values': security_group_ids}]
        security_groups = \
            ec2_client.describe_security_groups(Filters=sg_filters)[
                'SecurityGroups']
        return security_groups

class RdsVpcFilter(Filter):
    schema = type_schema('rds-vpc-filter',
                         key={'type': 'string'},
                         op={'type': 'string'},
                         value={'$ref': '#/definitions/filters_common/value'})
    permissions = ('rds:DescribeDBEngineVersions',)

    def process(self, resources, event=None):
        client_rds = local_session(self.manager.session_factory).client('rds')
        ec2 = local_session(self.manager.session_factory).client('ec2')
        op = OPERATORS[self.data.get('op')]
        value = self.data.get('value')
        db_identifiers = []
        result = []
        instances = client_rds.describe_db_instances()
        for r in instances['DBInstances']:
            inbound = ec2.describe_security_groups(
                GroupIds=jmespath.search(
                    'VpcSecurityGroups[].VpcSecurityGroupId', r))
            cidr_ips = jmespath.search(self.data.get('key'), inbound)
            for cidr_ip in cidr_ips:
                if op(cidr_ip, value):
                    if 'DBInstanceIdentifier' in r:
                        db_identifiers.append(r['DBInstanceIdentifier'])

        for db_identifier in db_identifiers:
            for resource_db_identifier in resources:
                if resource_db_identifier.get('DBInstanceIdentifier') and \
                        db_identifier == resource_db_identifier['DBInstanceIdentifier']:
                    result.append(resource_db_identifier)

        return result

def register() -> None:
    from c7n.resources.rds import RDS

    RDS.filter_registry.register('endpoint-port', EndpointPortRdsFilter)
    RDS.filter_registry.register('vpc-security-group-inbound-ports', VpcSecurityGroupRdsFilter)
    RDS.filter_registry.register('rds-vpc-filter', RdsVpcFilter)
