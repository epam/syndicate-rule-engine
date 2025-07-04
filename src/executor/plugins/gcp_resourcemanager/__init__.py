import re
from concurrent.futures import as_completed

import jmespath
from c7n.filters.core import ValueFilter, OPERATORS, Filter
from c7n.utils import type_schema, local_session
from c7n_gcp.query import QueryResourceManager, TypeInfo


def op(data, a, b):
    op = OPERATORS[data.get('op', 'eq')]
    return op(a, b)


class PreconditionCheckFilter(Filter):
    schema = type_schema('precondition-check-filter',
                         rinherit=ValueFilter.schema)
    permissions = ('accessapproval.settings.get',)

    def process(self, resources, event=None):
        filtered_resources = []
        session = local_session(self.manager.session_factory)
        client = session.client(service_name='accessapproval',
                                version='v1', component='projects')
        for resource in resources:
            try:
                get_access = client.execute_command(
                    'getAccessApprovalSettings', {
                        'name': 'projects/' + resource[
                            'projectId'] + '/accessApprovalSettings'})
                if get_access:
                    continue

            except Exception as e:
                if 'Precondition check failed' in str(e):
                    return resources
                else:
                    continue

        return filtered_resources


class LogProjectsSinkFilter(Filter):
    """
    This filter allows sinks in projects, if exist
    Check fields in sink resources.
    """

    schema = type_schema('log-project-sink-filter')
    permissions = ('logging.sinks.list',)

    def process(self, resources, event=None):
        session = local_session(self.manager.session_factory)
        client = session.client(
            service_name='logging', version='v2', component='projects.sinks')
        accepted = []

        with self.executor_factory(max_workers=3) as w:
            futures = {}
            for resource in resources:
                futures[w.submit(client.execute_command, 'list',
                                 {'parent': 'projects/{}'.format(
                                     resource['projectId'])})] = resource
                for future in as_completed(futures):
                    try:
                        sinks = future.result()
                    except Exception:
                        continue
                    if all(['filter' in sink for sink in sinks['sinks']]):
                        accepted.append(resource)

        return accepted


class LoggingMetricsFilter(ValueFilter):
    """
    This filter allows check metrics in projects, if exists.
    Check fields in metrics.
    """

    schema = type_schema('logging-metrics-filter', rinherit=ValueFilter.schema)
    permissions = ('logging.logMetrics.list',)

    def process(self, resources, event=None):
        session = local_session(self.manager.session_factory)
        logging_client = session.client(
            service_name='logging', version='v2', component='projects.metrics')
        alert_client = session.client(
            service_name='monitoring', version='v3',
            component='projects.alertPolicies')
        accepted = []

        with self.executor_factory(max_workers=10) as w:
            futures_alerts = {}
            futures_metrics = {}
            for resource in resources:
                futures_alerts[w.submit(alert_client.execute_command, 'list', {
                    'name': 'projects/{}'.format(
                        resource['projectId'])})] = resource
                futures_metrics[
                    w.submit(logging_client.execute_command, 'list',
                             {'parent': 'projects/{}'.format(
                                 resource['projectId'])})] = resource
                for future_alert in as_completed(futures_alerts):
                    try:
                        alerts = future_alert.result()
                    except Exception:
                        continue

                    valid_metrics = []
                    if alerts.get('alertPolicies'):
                        for alert in alerts['alertPolicies']:
                            gen_metrics = [
                                metric for metric in
                                self._check_is_valid_alert(resource, alert)]
                            for gen in gen_metrics:
                                if gen not in valid_metrics:
                                    valid_metrics.append(gen.split('/')[-1])

                for future_metric in as_completed(futures_metrics):
                    metrics = future_metric.result()
                    if metrics.get('metrics'):
                        for metric in metrics['metrics']:
                            jmespath_key = jmespath.search(
                                self.data.get('key'), metric)
                            if metric.get('name') and metric[
                                'name'] in valid_metrics and \
                                op(self.data, jmespath_key,
                                   self.data.get('value')):
                                accepted.append(resource)
                                break

        return accepted

    def _check_is_valid_alert(self, resource, alert):
        result = []
        project_id = resource['projectId']

        if 'conditions' in alert and alert.get('enabled'):
            for metric in alert['conditions']:
                if 'conditionThreshold' in metric and 'filter' in metric[
                    'conditionThreshold'] and \
                    'user' in metric['conditionThreshold']['filter']:
                    filter = re.findall(r'(?<=user/)(.*?)(?=\")',
                                        metric['conditionThreshold'][
                                            'filter'])[0]
                    metric['conditionThreshold']['filter'] = filter
                    result.append('projects/{}/metrics/{}'
                                  .format(project_id,
                                          metric['conditionThreshold'][
                                              'filter']))
                else:
                    continue

        return result


class ServiceVulnScanningFilter(ValueFilter):
    schema = type_schema('service-vuln-scanning-filter',
                         rinherit=ValueFilter.schema)
    permissions = ('serviceusage.services.list',)

    def process(self, resources, event=None):
        session = local_session(self.manager.session_factory)
        project = session.get_default_project()

        client = session.client(service_name='serviceusage', version='v1',
                                component='services')
        services = \
        client.execute_command('list', {'parent': 'projects/' + project,
                                        'filter': 'state:ENABLED',
                                        'pageSize': 200})['services']
        accepted_resources = []
        for resource in services:
            jmespath_key = jmespath.search(self.data.get('key'), resource)
            if jmespath_key is not None and op(self.data, jmespath_key,
                                               self.data.get('value')):
                accepted_resources.append(resource)

        if len(accepted_resources) == 0:
            for resource in resources:
                if resource['name'] == project:
                    return [resource]

        return []


class AuditConfigProjectFilter(Filter):
    """
    This filter allows check audit configs in projects, if exist
    Check fields in audit config resources.
    """

    schema = type_schema('audit-config-project-filter')
    permissions = ('resourcemanager.projects.getIamPolicy',)

    def process(self, resources, event=None):
        session = local_session(self.manager.session_factory)
        client = session.client(
            service_name='cloudresourcemanager', version='v1',
            component='projects')
        accepted = []

        with self.executor_factory(max_workers=3) as w:
            futures = {}
            for resource in resources:
                futures[w.submit(client.execute_command, 'getIamPolicy',
                                 {'resource': '{}'.format(
                                     resource['projectId'])})] = resource
                for future in as_completed(futures):
                    configs = future.result()
                    as_a_result = []
                    if configs.get('auditConfigs'):
                        for config in configs['auditConfigs']:
                            if config.get('service') and \
                                config['service'] == 'allServices' and \
                                config.get('auditLogConfigs') and len(
                                config['auditLogConfigs']) == 2 and config[
                                'auditLogConfigs'][0]['logType'] in \
                                ['DATA_WRITE', 'DATA_READ'] and config[
                                'auditLogConfigs'][1]['logType'] in \
                                ['DATA_WRITE', 'DATA_READ'] and \
                                'exemptedMembers' not in \
                                config['auditLogConfigs'][1] and \
                                'exemptedMembers' not in \
                                config['auditLogConfigs'][0]:
                                as_a_result.append(True)
                            else:
                                as_a_result.append(False)

                        if True in as_a_result:
                            accepted.append(resource)

        return accepted


class ProjectIamPolicyBindings(QueryResourceManager):
    """GCP resource: https://cloud.google.com/resource-manager/reference/
                     rest/v1/projects/getIamPolicy
    """

    class resource_type(TypeInfo):
        service = 'cloudresourcemanager'
        version = 'v1'
        component = 'projects'
        scope = 'project'
        enum_spec = ('getIamPolicy', 'bindings[]', {'body': {}})
        scope_key = 'resource'
        scope_template = '{}'
        name = id = 'role'
        default_report_fields = [id, 'members']
        get_multiple_resources = True

        @staticmethod
        def get(client, resource_info):
            iam_policy = client.execute_command(
                'getIamPolicy', {'resource': resource_info['project_id']})
            return iam_policy['bindings'] if 'bindings' in iam_policy else []


class ProjectIamPolicyBindingsByMembers(QueryResourceManager):
    """GCP resource: https://cloud.google.com/resource-manager/reference/
                     rest/v1/projects/getIamPolicy
    """

    class resource_type(TypeInfo):
        service = 'cloudresourcemanager'
        version = 'v1'
        component = 'projects'
        scope = 'project'
        enum_spec = ('getIamPolicy', 'bindings[]', {'body': {}})
        scope_key = 'resource'
        scope_template = '{}'
        name = id = 'member'
        default_report_fields = [id, 'roles']
        get_multiple_resources = True

    def _fetch_resources(self, query):
        fetched_resources = super()._fetch_resources(query)
        remapped_resources = []
        remapped_members = []
        for fetched_resource in fetched_resources:
            for member in fetched_resource['members']:
                if member in remapped_members:
                    for remapped_resource in remapped_resources:
                        if remapped_resource['member'] == member:
                            remapped_resource['roles'].append(
                                fetched_resource['role'])
                else:
                    remapped_resources.append(
                        {'member': member,
                         'roles': [fetched_resource['role']]})
                    remapped_members.append(member)
        return remapped_resources


class NewRolesFilter(Filter):
    schema = type_schema('new-roles-filter',
                         op={'$ref': '#/definitions/filters_common/value'},
                         value={'$ref': '#/definitions/filters_common/value'},
                         by={'$ref': '#/definitions/filters_common/value'})
    permissions = ('resourcemanager.projects.list',)

    def process(self, resources, event=None):
        filtered = []
        session = local_session(self.manager.session_factory)
        client_simple = session.client(service_name='iam', version='v1',
                                       component='roles')
        client_custom = session.client(service_name='iam', version='v1',
                                       component='projects.roles')
        by_who = self.data.get('by')

        for resource in resources:
            if by_who == 'user' and resource['member'].startswith(by_who):
                for role in resource['roles']:
                    if role.startswith('project'):
                        permissions = client_custom.execute_command('get', {
                            "name": role})['includedPermissions']
                        if op(self.data, permissions, self.data.get('value')):
                            filtered.append(resource)
                            break
                    else:
                        permissions = client_simple.execute_command('get', {
                            "name": role})['includedPermissions']
                        if op(self.data, permissions, self.data.get('value')):
                            filtered.append(resource)
                            break

            elif by_who == 'serviceAccount' and resource['member'].startswith(
                by_who):
                for role in resource['roles']:
                    if role.startswith('project'):
                        permissions = client_custom.execute_command('get', {
                            "name": role})['includedPermissions']
                        if op(self.data, permissions, self.data.get('value')):
                            filtered.append(resource)
                            break
                    else:
                        permissions = client_simple.execute_command('get', {
                            "name": role})['includedPermissions']
                        if op(self.data, permissions, self.data.get('value')):
                            filtered.append(resource)
                            break
            else:
                continue
        return filtered


def register() -> None:
    from c7n_gcp.resources.resourcemanager import Project
    from c7n_gcp.resources.resource_map import ResourceMap
    from c7n_gcp.provider import resources

    Project.filter_registry.register('precondition-check-filter',
                                     PreconditionCheckFilter)
    Project.filter_registry.register('log-project-sink-filter',
                                     LogProjectsSinkFilter)
    Project.filter_registry.register('logging-metrics-filter',
                                     LoggingMetricsFilter)
    Project.filter_registry.register('service-vuln-scanning-filter',
                                     ServiceVulnScanningFilter)
    Project.filter_registry.register('audit-config-project-filter',
                                     AuditConfigProjectFilter)

    resources.register('project-iam-policy-bindings', ProjectIamPolicyBindings)
    ResourceMap[
        'gcp.project-iam-policy-bindings'] = f'{__name__}.{ProjectIamPolicyBindings.__name__}'
    resources.register('project-iam-policy-bindings-by-members',
                       ProjectIamPolicyBindingsByMembers)
    ResourceMap[
        'gcp.project-iam-policy-bindings-by-members'] = f'{__name__}.{ProjectIamPolicyBindingsByMembers.__name__}'

    ProjectIamPolicyBindingsByMembers.filter_registry.register(
        'new-roles-filter', NewRolesFilter)
