"""
https://github.com/cloud-custodian/cloud-custodian/pull/8842
"""

from c7n.filters.core import ValueFilter
from c7n.utils import local_session
from c7n_gcp.filters.iampolicy import IamPolicyFilter
from c7n_gcp.query import (QueryResourceManager, TypeInfo)


class KubernetesClusterNodePoolIamPolicyFilter(IamPolicyFilter):
    """GKE node is configured with privileged service account

    :example:

    .. code-block:: yaml

        policies:
          - name: iam-gke-nodepool-filter
            description: GKE node is configured with privileged service account
            resource: gcp.gke-nodepool
            filters:
              - type: iam-policy
                doc:
                  key: bindings[?(role=='roles/owner') || ?(role=='roles/editor')]
                  op: ne
                  value: []
    """
    permissions = ('resourcemanager.projects.getIamPolicy',)

    def process(self, resources, event=None):
        session = local_session(self.manager.session_factory)
        client = session.client(
            service_name='cloudresourcemanager', version='v1',
            component='projects')
        project = session.get_default_project()
        iams = client.execute_command('getIamPolicy',
                                      {'resource': project}).get('bindings')
        if 'doc' in self.data:
            for resource in resources:
                iam_policies = [iam for iam in iams
                                if
                                'serviceAccount:' + resource.get('config').get(
                                    'serviceAccount')
                                in iam['members']]
                resource["c7n:iamPolicy"] = iam_policies
        return self.process_resources(resources)

    def process_resources(self, resources):
        value_filter = ValueFilter(self.data['doc'], self.manager)
        return value_filter.process(resources)


class KubernetesClusterBeta(QueryResourceManager):
    """GCP resource:
    https://cloud.google.com/kubernetes-engine/docs/reference/rest/v1/projects.zones.clusters
    """

    class resource_type(TypeInfo):
        service = 'container'
        version = 'v1beta1'
        component = 'projects.locations.clusters'
        enum_spec = ('list', 'clusters[]', None)
        scope = 'project'
        scope_key = 'parent'
        scope_template = "projects/{}/locations/-"
        name = id = "name"
        default_report_fields = [
            'name', 'description', 'status', 'currentMasterVersion',
            'currentNodeVersion',
            'currentNodeCount', 'location']
        asset_type = 'container.googleapis.com/Cluster'


def register() -> None:
    from c7n_gcp.resources.gke import KubernetesClusterNodePool
    from c7n_gcp.resources.resource_map import ResourceMap
    from c7n_gcp.provider import resources

    KubernetesClusterNodePool.filter_registry.register('iam-policy',
                                                       KubernetesClusterNodePoolIamPolicyFilter)

    resources.register('gke-cluster-beta-api', KubernetesClusterBeta)
    ResourceMap[
        'gcp.gke-cluster-beta-api'] = f'{__name__}.{KubernetesClusterBeta.__name__}'
