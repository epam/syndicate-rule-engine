from executor.plugins.aws_elb import CidrEgressPortRangeELBFilter
from c7n.utils import type_schema


class CidrIpSecurityGroupEC2Filter(CidrEgressPortRangeELBFilter):
    schema = type_schema('cidrip-security-group-ec2-filter',
                         **{"required": ['required-ports', 'egress', 'cidr'],
                            "required-ports": {
                                '$ref': '#/definitions/filters_common/value'},
                            "egress": {
                                '$ref': '#/definitions/filters_common/value'},
                            "cidr": {
                                '$ref': '#/definitions/filters_common/value'}})

    def _is_valid_security_group_id(self, security_group, group_id):
        for security_group in security_group['SecurityGroups']:
            if group_id['GroupId'] == security_group['GroupId']:
                return security_group
        return False


def register() -> None:
    from c7n.resources.ec2 import EC2

    EC2.filter_registry.register('cidrip-security-group-ec2-filter',
                                 CidrIpSecurityGroupEC2Filter)
