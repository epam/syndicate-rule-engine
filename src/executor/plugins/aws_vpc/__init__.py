import jmespath
from c7n.utils import (
    type_schema,
)

from executor.plugins.aws_elb import PortRangeFilter


class CidrEgressPortRangeNetworkAclFilter(PortRangeFilter):
    """Filter network acls by those that match multiple conditions

    The filter is an extension of PortRangeFilter and allows to find
    nacls that match egress/ingress, cidr blocks, allow/deny, and specific ports.

    The 'egress' property is either a boolean or a string that means ingress if set to False.
    The 'cidr' property is a string that expects a valid CIDR-range.
    The 'rule-action' property is a string that expects either 'allow' or 'deny'.
    Each of these accepts "*" (match_all_valid_value) meaning a match to all values.

    The 'required-ports' field is the same as in the parent filter,
    with the 'allow-partial' option set implicitly as True.

    The '_remap_entry_to_port_range' method acts as an adapter to the
    parent Filter class where the port ranges are converted as defined in AWS.

    :example:

        Find all nacls that allow port 22 or 3389 for all incoming connections.

    .. code-block:: yaml

            policies:
              - name: all-incoming-port-22-cidr-egress-port-range-nacls
                resource: aws.network-acl
                filters:
                  - type: cidr-egress-port-range
                    egress: false
                    required-ports: 22,3389
                    cidr: 0.0.0.0/0
                    rule-action: allow
    """
    egress_key = 'egress'
    cidr_key = 'cidr'
    rule_action_key = 'rule-action'
    match_all_valid_value = '*'
    schema = type_schema(
        'cidr-egress-port-range',
        required=[PortRangeFilter.ranges_key, egress_key, cidr_key],
        **{
            PortRangeFilter.ranges_key:
                {'$ref': '#/definitions/filters_common/value'},
            egress_key: {'$ref': '#/definitions/filters_common/value'},
            cidr_key: {'$ref': '#/definitions/filters_common/value'},
            rule_action_key: {'$ref': '#/definitions/filters_common/value'}
        })
    permissions = ('ec2:DescribeNetworkAcls',)

    def __init__(self, data, manager):
        super(CidrEgressPortRangeNetworkAclFilter,
              self).__init__(data, manager)
        self.data[PortRangeFilter.partial_key] = True

    def process(self, resources, event=None):
        policy_ranges = self.extract_policy_port_ranges()

        accepted_resources = []
        for resource in resources:
            valid_entries = self._find_valid_entries(resource)
            if valid_entries:
                resource_ranges = self._remap_entries_to_port_ranges(
                    valid_entries)
                if super(CidrEgressPortRangeNetworkAclFilter, self
                         ).check_ranges_match(policy_ranges, resource_ranges):
                    accepted_resources.append(resource)
        return accepted_resources

    def _valid_entries_jmespath(self):
        return 'Entries[]'

    def _jmespaths_by_valid_values(self):
        return {
            'Egress': self.data[
                CidrEgressPortRangeNetworkAclFilter.egress_key],
            'CidrBlock': self.data[
                CidrEgressPortRangeNetworkAclFilter.cidr_key],
            'RuleAction': self.data[
                CidrEgressPortRangeNetworkAclFilter.rule_action_key]
        }

    def _port_from_key(self):
        return 'From'

    def _port_to_key(self):
        return 'To'

    def _port_range_jmespath(self):
        return 'PortRange'

    def _find_valid_entries(self, resource):
        entries_jmespath = self._valid_entries_jmespath()
        jmespaths_by_valid_values = self._jmespaths_by_valid_values()

        valid_entries = []
        for entry in jmespath.search(entries_jmespath, resource):
            valid = True
            for path, valid_value in jmespaths_by_valid_values.items():
                if not self._has_valid_value(entry, path, valid_value):
                    valid = False
                    break
            if valid:
                valid_entries.append(entry)
        return valid_entries

    def _has_valid_value(self, entry, path, valid_value):
        return (valid_value == CidrEgressPortRangeNetworkAclFilter.match_all_valid_value
                or valid_value == jmespath.search(path, entry))

    def _remap_entries_to_port_ranges(self, entries):
        def _remap_entry_to_port_range(entry):
            include_all_ports_from = 0
            include_all_ports_to = 65535
            port_from_key = self._port_from_key()
            port_to_key = self._port_to_key()
            port_range_jmespath = self._port_range_jmespath()

            port_range_record = jmespath.search(port_range_jmespath, entry)
            if not port_range_record or not port_range_record.get(
                    port_from_key) or \
                    not port_range_record.get(port_to_key):
                port_from = include_all_ports_from
                port_to = include_all_ports_to
            else:
                port_from = port_range_record[port_from_key]
                port_to = port_range_record[port_to_key]
            return '{}-{}'.format(port_from, port_to)

        remapped_entries = [_remap_entry_to_port_range(entry) for entry in
                            entries]
        return ','.join(remapped_entries)


def register() -> None:
    from c7n.resources.vpc import NetworkAcl

    NetworkAcl.filter_registry.register(
        'cidr-egress-port-range',
        CidrEgressPortRangeNetworkAclFilter)
