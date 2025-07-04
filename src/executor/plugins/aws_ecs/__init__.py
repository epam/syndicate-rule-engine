from concurrent.futures import as_completed

import jmespath
from c7n.filters import ValueFilter, Filter
from c7n.filters.core import OPERATORS
from c7n.utils import local_session, type_schema


class ECSTaskDefinitionFilter(Filter):
    schema = type_schema('ecs-task-definition-filter')
    permissions = ('ecs:DescribeTaskDefinition',)

    def process(self, resources, event=None):
        client = local_session(self.manager.session_factory).client('ecs')
        accepted = []
        with self.executor_factory(max_workers=3) as w:
            futures = {}
            for resource in resources:
                if resource.get('taskDefinition'):
                    futures[w.submit(client.describe_task_definition,
                                     taskDefinition=resource.get(
                                         'taskDefinition'))] = resource
                    for future in as_completed(futures):
                        described_task = future.result()
                        if 'taskRoleArn' not in described_task['taskDefinition']:
                            accepted.append(resource)

        return accepted

class EncryptionInstanceIdEKSEBSFilter(ValueFilter):
    schema = type_schema('encryption-instance-id-ecs-filter',
                         rinherit=ValueFilter.schema
                         )
    permissions = ('ecs:DescribeClusters',)

    def _perform_op(self, a, b):
        op = OPERATORS[self.data.get('op', 'eq')]
        return op(a, b)

    def process(self, resources, event=None):
        client_ecs = local_session(self.manager.session_factory).client('ecs')
        client_ec2 = local_session(self.manager.session_factory).client('ec2')
        self.described_volumes = client_ec2.describe_volumes()['Volumes']
        self.value = self.data.get('value')
        self.key = self.data.get('key')

        filtered_clusters = []
        filtered_containers = []
        list_cluster_names = {
            'cluster_names': [cluster['clusterName'] for cluster in resources]}
        clusters_arns = {'containerInstanceArns': []}
        for cluster_name in list_cluster_names['cluster_names']:
            for container_arn in client_ecs.list_container_instances(
                    cluster=cluster_name)['containerInstanceArns']:
                clusters_arns['containerInstanceArns'].append(container_arn)

        container_arns = {}
        for container_arn in clusters_arns['containerInstanceArns']:
            if container_arn.split('/')[-2] not in container_arns:
                container_arns[container_arn.split('/')[-2]] = []
                container_arns[container_arn.split('/')[-2]].append(
                    container_arn.split('/')[-1])
            else:
                container_arns[container_arn.split('/')[-2]].append(
                    container_arn.split('/')[-1])

        if not container_arns:
            return []

        described_containers = [
            client_ecs.describe_container_instances(
                cluster=cluster, containerInstances=container_arns[cluster])[
                'containerInstances'][0] for cluster in container_arns.keys()]

        for container in described_containers:
            if self._is_valid_volume(container['ec2InstanceId']):
                filtered_containers.append(container['containerInstanceArn'])

        for cluster in resources:
            for container_arn in filtered_containers:
                if cluster['clusterName'] == container_arn.split('/')[-2]:
                    filtered_clusters.append(cluster)

        return filtered_clusters

    def _is_valid_volume(self, ec2_id):
        def _check_volume_id(volumes):
            for volume in volumes['Attachments']:
                if volume['InstanceId'] == ec2_id:
                    return True
            return False

        for volume in self.described_volumes:
            if _check_volume_id(volume):
                jmespath_value = jmespath.search(self.key, volume)
                if self._perform_op(self.value, jmespath_value):
                    return True
        return False


def register() -> None:
    from c7n.resources.ecs import Service,ECSCluster

    Service.filter_registry.register('ecs-task-definition-filter', ECSTaskDefinitionFilter)
    ECSCluster.filter_registry.register('encryption-instance-id-ecs-filter', EncryptionInstanceIdEKSEBSFilter)
