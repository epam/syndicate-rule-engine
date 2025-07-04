"""
https://github.com/cloud-custodian/cloud-custodian/pull/8899
"""
import re

from c7n.utils import jmespath_search
from c7n_gcp.query import ChildResourceManager, ChildTypeInfo


class LoadBalancerTargetHttpsProxy(ChildResourceManager):

    class resource_type(ChildTypeInfo):
        service = 'compute'
        version = 'v1'
        component = 'targetHttpsProxies'
        enum_spec = ('list', 'items[]', None)
        scope = 'project'
        id = name = 'name'
        default_report_fields = [id]
        parent_spec = {
            'resource': 'loadbalancer-backend-service',
            'child_enum_params': [],
        }
        asset_type = "compute.googleapis.com/TargetHttpsProxy"

        @staticmethod
        def get(client, resource_info):
            return client.execute_command(
                'get', {
                    'backendService': resource_info['backendService'],
                    'notification': resource_info['notification_id']
                })


class LoadBalancerBackendSslPolicy(ChildResourceManager):
    """
    GC resource: https://cloud.google.com/compute/docs/reference/rest/v1/sslPolicies
    """
    class resource_type(ChildTypeInfo):
        service = 'compute'
        version = 'v1'
        component = 'sslPolicies'
        enum_spec = ('list', 'items[]', None)
        id = name = 'name'
        default_report_fields = [id, 'profile', 'minTlsVersion']
        parent_spec = {
            'resource': 'loadbalancer-target-https-proxy',
        }
        asset_type = "compute.googleapis.com/SslPolicy"

        @staticmethod
        def get(client, resource_info):
            return client.execute_command(
                'get', {
                    'backendService': resource_info['backendService'],
                    'notification': resource_info['notification_id']
                })

    def _get_child_enum_args_list(self, parent_instance):
        ssl_policy_full_name = jmespath_search('sslPolicy', parent_instance)
        if ssl_policy_full_name:
            ssl_policy_name = re.match('.*/global/sslPolicies/(.*)', ssl_policy_full_name
                                       ).group(1)
            return [{'filter': 'name={}'.format(ssl_policy_name)}]
        return []


class LoadBalancerSslPolicy(ChildResourceManager):
    """
    GC resource: https://cloud.google.com/compute/docs/reference/rest/v1/sslPolicies
    Unlike loadbalancer-ssl-policy, returns only policies that are tied to
    loadbalancer-target-https-proxy resources.
    """

    class resource_type(ChildTypeInfo):
        service = 'compute'
        version = 'v1'
        component = 'sslPolicies'
        enum_spec = ('list', 'items[]', None)
        scope = 'project'
        name = id = 'name'
        get_requires_event = True
        default_report_fields = [
            name, "description", "sslPolicy", "urlMap"
        ]
        parent_spec = {
            'resource': 'loadbalancer-target-https-proxy',
        }
        asset_type = "compute.googleapis.com/SslPolicy"

        @staticmethod
        def get(client, event):
            self_link = jmespath_search('protoPayload.request.sslPolicy', event)
            parent_resource_name = jmespath_search('protoPayload.resourceName', event)
            project = re.match('.*projects/(.*?)/global/targetHttpsProxies/.*',
                               parent_resource_name).group(1)
            ssl_policy = {'project_id': project, 'parent_resource_name': parent_resource_name}
            if self_link:
                name = re.match('.*projects/.*?/global/sslPolicies/(.*)', self_link).group(1)
                ssl_policy.update(client.execute_command(
                    'get', {'project': project, 'sslPolicy': name}))
            return ssl_policy

    def _get_child_enum_args(self, parent_instance):
        child_enum_args = {}
        ssl_policy = parent_instance['sslPolicy'] if 'sslPolicy' in parent_instance else None
        if ssl_policy:
            ssl_policy_name = re.match('.*/global/sslPolicies/(.*)', ssl_policy).group(1)
        else:
            ssl_policy_name = 'GCP default'
        child_enum_args['filter'] = 'name = \"%s\"' % ssl_policy_name
        return child_enum_args

    def _get_parent_resource_info(self, child_instance):
        return {'project_id': child_instance['project_id'],
                'resourceName': child_instance['parent_resource_name']}

def register() -> None:
    from c7n_gcp.provider import resources
    from c7n_gcp.resources.resource_map import ResourceMap

    resources.register('loadbalancer-target-https-proxy', LoadBalancerTargetHttpsProxy)
    resources.register('loadbalancer-backend-ssl-policy', LoadBalancerBackendSslPolicy)
    resources.register('loadbalancer-ssl-policy', LoadBalancerSslPolicy)

    ResourceMap['gcp.loadbalancer-target-https-proxy'] = f'{__name__}.{LoadBalancerTargetHttpsProxy.__name__}'
    ResourceMap['gcp.loadbalancer-backend-ssl-policy'] = f'{__name__}.{LoadBalancerBackendSslPolicy.__name__}'
    ResourceMap['gcp.loadbalancer-ssl-policy'] = f'{__name__}.{LoadBalancerSslPolicy.__name__}'
