from concurrent.futures import as_completed

import jmespath
from c7n.filters import ValueFilter
from c7n.filters.core import OPERATORS
from c7n.utils import local_session, type_schema


def op(data, a, b):
    op = OPERATORS[data.get('op', 'eq')]
    return op(a, b)


class GCPIamPolicyFilter(ValueFilter):
    schema = type_schema('gcp-iam-policy-filter',
                         rinherit=ValueFilter.schema)
    permissions = ('resourcemanager.projects.getIamPolicy',)

    def process(self, resources, event=None):
        session = local_session(self.manager.session_factory)
        project = session.get_default_project()
        client = session.client(
            service_name=self.manager.resource_type.service,
            version=self.manager.resource_type.version,
            component=self.manager.resource_type.component)
        accepted_resources = []
        resource_req = 'resource'
        if self.manager.resource_type.component == 'buckets':
            resource_req = 'bucket'
        with self.executor_factory(max_workers=1) as w:
            futures = {}
            for resource in resources:
                if self.manager.resource_type.component == 'images':
                    futures[w.submit(client.execute_command, 'getIamPolicy',
                                     {resource_req: '{}'.format(
                                         resource['name']),
                                         'project': project})] = resource
                elif self.manager.resource_type.component == 'projects.regions.clusters':
                    futures[w.submit(client.execute_command, 'getIamPolicy',
                                     {
                                         resource_req: 'projects/{}/regions/{}/clusters/{}'.format(
                                             project, resource['labels'][
                                                 'goog-dataproc-location'],
                                             resource[
                                                 'clusterName'])})] = resource
                else:
                    futures[w.submit(client.execute_command, 'getIamPolicy',
                                     {resource_req: '{}'.format(
                                         resource['name'])})] = resource
                for future in as_completed(futures):
                    iam_policies = future.result()
                    if self._is_valid_policy(iam_policies):
                        accepted_resources.append(resource)
                        futures = {}

        return accepted_resources

    def _is_valid_policy(self, iam_policies):
        if not iam_policies.get('bindings'):
            return False
        for policy in iam_policies['bindings']:
            jmespath_key = jmespath.search(self.data.get('key'), policy)
            if jmespath_key and op(self.data, jmespath_key,
                                   self.data.get('value')):
                return True
        return False


def register() -> None:
    from c7n_gcp.resources.artifactregistry import ArtifactRegistryRepository
    ArtifactRegistryRepository.filter_registry.register(
        'gcp-iam-policy-filter', GCPIamPolicyFilter)
