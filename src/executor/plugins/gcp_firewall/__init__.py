"""
https://github.com/cloud-custodian/cloud-custodian/pull/8814
"""

import jmespath
from c7n.filters.core import OPERATORS
from c7n.filters.core import ValueFilter
from c7n.utils import type_schema, local_session

from executor.plugins.aws_elb import PortRangeFilter


class VPCDNSPolicyFilter(ValueFilter):
    schema = type_schema('vpc-dns-policy-filter',
                         rinherit=ValueFilter.schema, )
    permissions = ('dns.policies.list',)

    def _perform_op(self, a, b):
        op = OPERATORS[self.data.get('op', 'eq')]
        return op(a, b)

    def process(self, resources, event=None):
        session = local_session(self.manager.session_factory)
        client = session.client(service_name='dns', version='v1beta2',
                                component='policies')
        # Getting project_id from client
        accepted_resources = []
        project = session.get_default_project()
        dns_policies = client.execute_query('list', {'project': project})
        if not dns_policies:
            return accepted_resources

        for resource in resources:
            if self._is_valid_vpc(vpc=resource['name'],
                                  dns_policies=dns_policies):
                accepted_resources.append(resource)

        return accepted_resources

    def _is_valid_vpc(self, vpc, dns_policies):
        for policy in dns_policies['policies']:
            for network in policy['networks']:
                key = jmespath.search(self.data['key'], policy)
                if network['networkUrl'].endswith(vpc) and \
                    self._perform_op(key, self.data['value']):
                    return True
        return False


class PortRangeFirewallFilter(PortRangeFilter):
    permissions = ('compute.firewalls.get', 'compute.firewalls.list')


class AttachedToClusterFirewallFilter(ValueFilter):
    """
    Checks if a firewall rule belongs to the network among the available clusters.
    Usage example:
      policies:
       - name: gcp-firewall-attached-to-cluster-filter
         resource: gcp.firewall
         filters:
         - attached-to-cluster
    """
    permissions = ('container.clusters.list',)

    def process(self, resources, event=None):
        clusters = self.manager.get_resource_manager('gke-cluster').resources()
        networks = set(
            [jmespath.search('networkConfig.network', cluster) for cluster in
             clusters])
        return self.filter_firewalls_if_attached_to_networks(resources,
                                                             networks)

    def filter_firewalls_if_attached_to_networks(self, firewalls, networks):
        return [firewall for network in networks
                for firewall in
                list(filter(lambda f: f['network'].endswith(network),
                            firewalls))]


def register() -> None:
    from c7n_gcp.resources.network import Firewall, Network
    Network.filter_registry.register('vpc-dns-policy-filter',
                                     VPCDNSPolicyFilter)
    Firewall.filter_registry.register('port-range', PortRangeFirewallFilter)
    Firewall.filter_registry.register('attached-to-cluster',
                                      AttachedToClusterFirewallFilter)
