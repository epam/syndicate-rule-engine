import re

import jmespath
from c7n.filters import Filter
from c7n.utils import local_session, type_schema


class PortRangeFilter(Filter):
    """
    Allows to check if all the ports specified in the policy
    are within the ones stored in the firewall rule.
    Example 1: ports 10-20 are within 10, 11-25.
    Example 2: port 19 is within 18-22.
    Example 3: ports 20-24 are within 20-21, 22-24.
    Example 4: ports 20-24 are NOT within 20-21, 23-24.
               (or specify allow-partial: True)
    Example 5: ports 20,23-24 are within 20-21, 23-24.
    Usage example:
      filters:
      - type: port-range
        key: allowed[?IPProtocol=='tcp'].ports[]
        required-ports: 20, 50-60
        allow-partial: False
    """
    key_key = 'key'
    ranges_key = 'required-ports'
    partial_key = 'allow-partial'
    pattern = '^(-?\\d+)-(-?\\d+)$'
    schema = type_schema(
        'port-range',
        required=[key_key, ranges_key],
        **{
            key_key: {'$ref': '#/definitions/filters_common/value'},
            ranges_key: {'$ref': '#/definitions/filters_common/value'},
            partial_key: {'type': 'boolean'}
        })

    def __init__(self, data, manager):
        super(PortRangeFilter, self).__init__(data, manager)
        if PortRangeFilter.partial_key not in self.data:
            self.data[PortRangeFilter.partial_key] = False

    def process(self, resources, event=None):
        return list(filter(lambda resource: self.is_valid_resource(resource),
                           resources))

    def extract_policy_port_ranges(self) -> str:
        return str(self.data[PortRangeFilter.ranges_key])

    def extract_resource_port_ranges(self, resource) -> str:
        ranges = jmespath.search(self.data[PortRangeFilter.key_key], resource)
        return ','.join(ranges) if ranges else ''

    def is_valid_resource(self, resource):
        policy_ranges = self.extract_policy_port_ranges()
        resource_ranges = self.extract_resource_port_ranges(resource)
        return self.check_ranges_match(policy_ranges, resource_ranges)

    def check_ranges_match(self, policy_ranges: str, resource_ranges: str):
        """
        :param policy_ranges: a comma-separated string containing either
               integers or ranges; e.g. 0,25-443,1024,3389
        :param resource_ranges: in the same format as policy_ranges
        :return: True or False depending on PortRangeFilter.partial_key
        """
        unmerged_policy_ports = PortRangeFilter._parse_ranges(policy_ranges)
        unmerged_resource_ports = PortRangeFilter._parse_ranges(
            resource_ranges)
        policy_ports = PortRangeFilter._sort_and_merge_intersecting_ranges(
            unmerged_policy_ports)
        resource_ports = PortRangeFilter._sort_and_merge_intersecting_ranges(
            unmerged_resource_ports)
        if self.data[PortRangeFilter.partial_key]:
            return PortRangeFilter._is_partial_match(policy_ports,
                                                     resource_ports)
        return PortRangeFilter._is_subset(policy_ports, resource_ports)

    @classmethod
    def _is_subset(cls, maybe_range_container_subset, range_container):
        """
        :param maybe_range_container_subset: sorted and merged
        :param range_container: sorted and merged
        """
        range_container_index = 0
        range_container_last_index = len(range_container) - 1
        is_subset = True
        for maybe_range_container_subset_element in maybe_range_container_subset:
            is_subset_element = False
            while range_container_index <= range_container_last_index:
                range_container_element = range_container[
                    range_container_index]
                if cls._is_range_before_another_range(
                    range_container_element,
                    maybe_range_container_subset_element):
                    pass
                elif cls._is_range_within_another_range(
                    maybe_range_container_subset_element,
                    range_container_element):
                    is_subset_element = True
                    break
                range_container_index += 1
            if not is_subset_element:
                is_subset = False
            if not is_subset:
                break
        return is_subset

    @classmethod
    def _is_partial_match(cls, maybe_range_container_partial_match,
                          range_container):
        """
        :param maybe_range_container_partial_match: sorted and merged
        :param range_container: sorted and merged
        """
        a = range_container
        b = maybe_range_container_partial_match
        range_container_last_index = len(a) - 1
        partial_match = False
        for maybe_range_container_subset_element in b:
            range_container_index = 0
            while range_container_index <= range_container_last_index:
                range_container_element = a[range_container_index]
                if cls._is_range_intersecting_another_range(
                    range_container_element,
                    maybe_range_container_subset_element):
                    partial_match = True
                    break
                range_container_index += 1
            if partial_match:
                break
        return partial_match

    @classmethod
    def _is_range_within_another_range(cls, range_to_check, another_range):
        return another_range[0] <= range_to_check[0] and range_to_check[1] <= \
            another_range[1]

    @classmethod
    def _is_range_before_another_range(cls, range_to_check, another_range):
        return range_to_check[1] < another_range[0]

    @classmethod
    def _is_range_before_and_next_to_another_range(cls, range_to_check,
                                                   another_range):
        return range_to_check[1] + 1 == another_range[0]

    @classmethod
    def _is_range_intersecting_or_touching_another_range(cls, range_to_check,
                                                         another_range):
        if cls._is_range_before_another_range(range_to_check, another_range):
            return cls._is_range_before_and_next_to_another_range(
                range_to_check, another_range)
        if cls._is_range_before_another_range(another_range, range_to_check):
            return cls._is_range_before_and_next_to_another_range(
                another_range, range_to_check)
        return True

    @classmethod
    def _is_range_intersecting_another_range(cls, range_to_check,
                                             another_range):
        if cls._is_range_before_another_range(range_to_check, another_range):
            return False
        if cls._is_range_before_another_range(another_range, range_to_check):
            return False
        return True

    @classmethod
    def _sort_and_merge_intersecting_ranges(cls, ranges):
        if len(ranges) > 1:
            merged_ranges = []
            sorted_ranges = sorted(ranges)
            current_merged_range = [sorted_ranges[0][0], sorted_ranges[0][1]]
            for current_range in sorted_ranges[1:]:
                if cls._is_range_intersecting_or_touching_another_range(
                    current_merged_range, current_range):
                    current_merged_range_max = max(current_range[1],
                                                   current_merged_range[1])
                    current_merged_range = [current_merged_range[0],
                                            current_merged_range_max]
                else:
                    merged_ranges.append(tuple(current_merged_range))
                    current_merged_range = [current_range[0], current_range[1]]
            merged_ranges.append(tuple(current_merged_range))
            return merged_ranges
        return sorted(ranges)

    @classmethod
    def _parse_ranges(cls, raw_ranges):
        tokens = cls._parse_tokens(raw_ranges)
        ranges = cls._parse_port_range_or_port_tokens(tokens)
        return ranges

    @classmethod
    def _parse_port_range_or_port_tokens(cls, port_range_or_port_tokens):
        ranges = set()
        for token in port_range_or_port_tokens:
            ranges.add(ParseMaxAndMinPorts.parse_port_range_token(token)
                       if cls._is_port_range_token(token)
                       else cls._parse_port_token_as_port_range(token))
        return ranges

    @classmethod
    def _parse_tokens(cls, raw_tokens):
        tokens = [token.strip() for token in raw_tokens.split(',')]
        if len(tokens) == 1 and tokens[0] == '':
            tokens = []
        return tokens

    @classmethod
    def _is_port_range_token(cls, token):
        return re.match(PortRangeFilter.pattern, token) is not None

    @classmethod
    def _parse_port_token_as_port_range(cls, port_token):
        port = ParseMaxAndMinPorts.parse_port_token(port_token)
        return port, port


class ParseMaxAndMinPorts:
    pattern = '^(-?\\d+)-(-?\\d+)$'

    @classmethod
    def parse_port_range_token(cls, port_range_token):
        (min_port, max_port) = re.match(ParseMaxAndMinPorts.pattern,
                                        port_range_token).groups()
        parsed_min_port, parsed_max_port = cls.parse_port_token(
            min_port), cls.parse_port_token(max_port)
        if parsed_min_port == -1:
            parsed_min_port = 0
        if parsed_max_port == -1:
            parsed_max_port = 65535
        return parsed_min_port, parsed_max_port

    @classmethod
    def parse_port_token(cls, port_token):
        return int(port_token)


class CidrEgressPortRangeELBFilter(PortRangeFilter):
    """That filter allows to check all open ports for specific ip scope.
    We can use range, list of ports for checking or a specific port.
    :example
    required_ports: 1, 2, 3 - will be checked 3 ports (1,2,3)
    ...
    required_ports: 4-11 - will be checked range of ports from 4 to 11 port
    ...
    required_ports: 6,9,11 - will be checked 6,9 and 11 port
    ...
    required_ports: 9 - will be checked only 9 port

    :example policy:

        Find all security group for elb that allow port 22 or 3389 for all incoming
        connections.

    .. code-block:: yaml

        policies:
          - name: test
            resource: aws.elb
            filters:
              - type: cidr-egress-port-range-elb-filter
                required-ports: 23
                egress: false
                cidr: ["0.0.0.0/0"]

    """
    egress = 'egress'
    cidr = 'cidr'
    required_ports = 'required-ports'
    schema = type_schema('cidr-egress-port-range-elb-filter',
                         **{"required": ['required-ports', 'egress', 'cidr'],
                            "required-ports": {
                                '$ref': '#/definitions/filters_common/value'},
                            "egress": {
                                '$ref': '#/definitions/filters_common/value'},
                            "cidr": {
                                '$ref': '#/definitions/filters_common/value'},
                            "ipv6": {
                                '$ref': '#/definitions/filters_common/value'}})
    permissions = ('elasticloadbalancing:DescribeLoadBalancers',)

    def __init__(self, data, manager):
        super(CidrEgressPortRangeELBFilter,
              self).__init__(data, manager)
        self.data[PortRangeFilter.partial_key] = True
        self.data['path'] = 'SecurityGroups'

    def process(self, resources, event=None):
        ingress = 'IpPermissions'
        egress = 'IpPermissionsEgress'
        self.check_ingress_or_egress = self.data.get(self.egress)
        self.valid_cidr = self.data.get(self.cidr)
        self.valid_ports = self.data.get(self.required_ports)
        accepted_resource = []
        client = local_session(self.manager.session_factory).client('ec2')
        sec_groups = client.describe_security_groups()
        if self.check_ingress_or_egress:
            self.choose_ingress_or_egress = egress
        else:
            self.choose_ingress_or_egress = ingress
        for resource in resources:
            security_groups = jmespath.search(self.data['path'], resource)
            if security_groups:
                for sec_group in security_groups:
                    check_is_valid_security_group = self._is_valid_security_group_id(
                        sec_groups, sec_group)
                    if check_is_valid_security_group and \
                        self._is_valid_security_group(
                            check_is_valid_security_group):
                        accepted_resource.append(resource)
                        break
        return accepted_resource

    def _is_valid_security_group_id(self, security_group, group_id):
        #  Choosing of valid security group for elb resource
        for security_group in security_group['SecurityGroups']:
            if group_id == security_group['GroupId']:
                return security_group
        return False

    def _is_valid_security_group(self, security_group):
        data_scope = self.choose_ingress_or_egress
        data = security_group[data_scope]
        #  Check only scope of elements; Foresee
        #  in advance cases with large quantity of values

        if isinstance(self.valid_cidr, list):
            cidrs = self.valid_cidr
        else:
            cidrs = [self.valid_cidr]
        from_port = 0
        to_port = 65535
        policy_ranges = self.extract_policy_port_ranges()

        for d in data:
            if 'FromPort' in d:
                from_port = d['FromPort']
            if 'ToPort' in d:
                to_port = d['ToPort']
            resource_ranges = "{}-{}".format(from_port, to_port)
            if self.check_ranges_match(policy_ranges, resource_ranges) and \
                self._is_valid_cidr_ip(d, cidrs) or \
                self._is_valid_ipv6_cidr_ip(d, cidrs):
                return True
        return False

    def _is_valid_cidr_ip(self, data, cidrs):
        for ip in data['IpRanges']:
            if ip['CidrIp'] in cidrs:
                return True
        return False

    def _is_valid_ipv6_cidr_ip(self, data, cidrs):
        for ip in data['Ipv6Ranges']:
            if ip['CidrIpv6'] in cidrs:
                return True
        return False


def register() -> None:
    from c7n.resources.elb import ELB
    ELB.filter_registry.register('cidr-egress-port-range-elb-filter',
                                 CidrEgressPortRangeELBFilter)
