import jmespath
from c7n.filters import Filter, ValueFilter
from c7n.filters.core import OPERATORS
from c7n.utils import (
    local_session, type_schema)



class VPCElasticCacheFilter(ValueFilter):
    schema = type_schema('vpc-elastic-cache-filter',
                         rinherit=ValueFilter.schema)
    permissions = ("ecs:*",)

    def process(self, resources, event=None):
        elastic_cache_client = local_session(
            self.manager.session_factory).client('elasticache')
        ec2_client = local_session(self.manager.session_factory).client('ec2')
        self.described_vpc = ec2_client.describe_vpcs()
        self.subnet_groups = elastic_cache_client.describe_cache_subnet_groups()
        self.op = OPERATORS[self.data.get('op')]
        self.key = self.data.get('key')
        self.value = self.data.get('value')

        accepted_resources = []
        for resource in resources:
            if 'CacheSubnetGroupName' in resource:
                vpc_id = self._is_valid_subnet_group_name(
                    resource['CacheSubnetGroupName'])
                if vpc_id:
                    valid_vpc = self._is_valid_vpc(vpc_id)
                    if valid_vpc:
                        accepted_resources.append(resource)

        return accepted_resources

    def _is_valid_subnet_group_name(self, cache_subnet_group_name):
        if 'CacheSubnetGroups' in self.subnet_groups:
            for subnet_group in self.subnet_groups['CacheSubnetGroups']:
                if subnet_group[
                    'CacheSubnetGroupName'] == cache_subnet_group_name:
                    return subnet_group['VpcId']
        return False

    def _is_valid_vpc(self, vpc_id):
        if 'Vpcs' in self.described_vpc:
            for vpc in self.described_vpc['Vpcs']:
                if vpc['VpcId'] == vpc_id:
                    key_jmespath = jmespath.search(self.key, vpc)
                    if self.op(key_jmespath, self.value):
                        return True
        return False


class RedisMemcacheFilter(Filter):
    schema = type_schema('redis-memcache-filter',
                         port={'$ref': '#/definitions/filters_common/value'})
    permissions = ("ecs:*",)

    def process(self, resources, event=None):
        elastic_cache_client = local_session(
            self.manager.session_factory).client('elasticache')
        self.value = self.convert_value()
        accepted_resources = []
        self.clusters = elastic_cache_client.describe_cache_clusters(
            ShowCacheNodeInfo=True)
        for resource in resources:
            accepted_resource = self._is_valid_cluster(resource)
            if accepted_resource:
                accepted_resources.append(resource)
        return accepted_resources

    def _is_valid_cluster(self, resource):
        if 'CacheClusters' in self.clusters:
            for cluster in self.clusters['CacheClusters']:
                if 'CacheClusterId' in cluster and 'CacheClusterId' in resource and \
                    cluster['CacheClusterId'] == resource['CacheClusterId'] and \
                    'CacheNodes' in cluster:
                    jmespath_ports = jmespath.search(
                        'CacheNodes[].Endpoint.Port', cluster)
                    proceeded_ports = [str(i) for i in jmespath_ports]
                    if self._is_valid_resource(proceeded_ports):
                        return resource
                    break
        return False

    def _is_valid_resource(self, ports):
        for port in ports:
            if port in self.value:
                return True
        return False

    def convert_value(self):
        value = self.data.get('port')
        if isinstance(value, list):
            return [str(val) for val in value]
        converted_value = str(value)
        converted_value = converted_value.replace(' ', '').split(',')
        if '-' in value:
            converted_value = [str(val) for val in
                               range(int(value.split('-')[0]),
                                     int(value.split('-')[1]))]
        return converted_value


def register() -> None:
    from c7n.resources.elasticache import ElastiCacheCluster

    ElastiCacheCluster.filter_registry.register('vpc-elastic-cache-filter',
                                                VPCElasticCacheFilter)
    ElastiCacheCluster.filter_registry.register('redis-memcache-filter',
                                                RedisMemcacheFilter)
