from c7n.filters import Filter
from c7n.utils import (
    type_schema, local_session)


class RDSClusterParameterFilter(Filter):
    """
    Filter is used for checking db cluster's parameters.

        :example:

    .. code-block:: yaml

            policies:
              - name: rds-cluster-parameter-filter
                resource: aws.rds-cluster
                filters:
                  - type: rds-cluster-parameter-filter
                    parameters:
                      - key: slow_query_log
                        value: absent
    """
    schema = type_schema('rds-cluster-parameter-filter',
                         parameters={'type': 'array', 'items': {
                             'type': 'object',
                             'required': ['key', 'value'],
                             'additionalProperties': False,
                             'properties': {
                                 'key': {'type': 'string'},
                                 'value': {
                                     '$ref': '#/definitions/filters_common/value'}
                             }
                         }})
    permissions = ('rds:DescribeDBInstances',)

    def _convert_conditions(self):
        conditions = self.data.get('parameters')
        converted_dict = {}
        converted = []
        for cond in conditions:
            for key, value in cond.items():
                if value is not isinstance(value, str):
                    converted_dict.update({key: str(value)})
                else:
                    converted_dict.update({key: value})
            converted.append(converted_dict)
            converted_dict = {}
        return converted

    def process(self, resources, event=None):
        self.rds_client = local_session(self.manager.session_factory).client(
            'rds')
        conditions = self._convert_conditions()
        result = []
        desc_param_groups = []

        self.clusters_db = self.rds_client.describe_db_clusters()

        for resource in resources:
            cluster_db = self._is_valid_cluster_db(resource)
            if cluster_db:
                desc_param_groups.append(cluster_db['DBClusterParameterGroup'])

            for desc in desc_param_groups:
                db_parameters = self._get_all_records(desc)
                is_valid_result = all([self._check_is_valid_condition(
                    condition, db_parameters) for condition in conditions])
                if is_valid_result:
                    result.append(resource)
                    desc_param_groups = []
                    break
        return result

    def _is_valid_cluster_db(self, resource):
        for cluster in self.clusters_db['DBClusters']:
            if resource['DBClusterIdentifier'] == cluster[
                'DBClusterIdentifier']:
                return cluster
        return False

    def _check_is_valid_condition(self, condition, db_parameters):
        for db_parameter in db_parameters:
            if condition[
                'value'] == 'absent' and 'ParameterName' in db_parameter and \
                db_parameter['ParameterName'] == condition['key'] and \
                self._is_absent(db_parameter):
                return True
            if 'ParameterName' in db_parameter and 'ParameterValue' in db_parameter and \
                db_parameter['ParameterName'] == condition['key'] and \
                condition['value'] == db_parameter['ParameterValue']:
                return True
        return False

    def _is_absent(self, db_parameter):
        if not db_parameter.get('ParameterValue'):
            return True
        return False

    def _get_all_records(self, desc):
        result = []
        marker = None
        while True:
            if marker:
                response_iterator = self.rds_client.describe_db_cluster_parameters(
                    DBClusterParameterGroupName=desc, MaxRecords=100,
                    Marker=marker)
            else:
                response_iterator = self.rds_client.describe_db_cluster_parameters(
                    DBClusterParameterGroupName=desc, MaxRecords=100)
            if 'Parameters' in response_iterator:
                result.extend(response_iterator['Parameters'])
            if 'Marker' in response_iterator:
                marker = response_iterator['Marker']
            else:
                return result


def register() -> None:
    from c7n.resources.rdscluster import RDSCluster
    RDSCluster.filter_registry.register(
        'rds-cluster-parameter-filter', RDSClusterParameterFilter)
