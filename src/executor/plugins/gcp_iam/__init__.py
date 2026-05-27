from c7n.filters import Filter, OPERATORS
from c7n.utils import local_session
from c7n.utils import type_schema
from c7n_gcp.query import ChildResourceManager, ChildTypeInfo


def op(data, a, b):
    op = OPERATORS[data.get('op', 'eq')]
    return op(a, b)


class ServiceAccountBindings(ChildResourceManager):
    class resource_type(ChildTypeInfo):
        service = 'iam'
        version = 'v1'
        component = 'projects.serviceAccounts'
        enum_spec = ('getIamPolicy', 'bindings[]', None)
        scope = 'resource'
        scope_key = 'resource'
        id = name = 'name'
        default_report_fields = [name, 'description']
        parent_spec = {
            'resource': 'service-account',
            'child_enum_params': {
                ('name', 'resource')},
        }


class NewRolesIAMFilter(Filter):
    schema = type_schema('new-roles-iam-filter',
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
            for member in resource['members']:
                if by_who == 'user' and member.startswith(by_who):
                    if resource['role'].startswith('project'):
                        permissions = client_custom.execute_command('get', {
                            "name": resource['role']})['includedPermissions']
                        if op(self.data, permissions, self.data.get('value')):
                            filtered.append(resource)
                            break
                    else:
                        permissions = client_simple.execute_command('get', {
                            "name": resource['role']})['includedPermissions']
                        if op(self.data, permissions, self.data.get('value')):
                            filtered.append(resource)
                            break

                elif by_who == 'serviceAccount' and member.startswith(by_who):
                    if resource['role'].startswith('project'):
                        permissions = client_custom.execute_command('get', {
                            "name": resource['role']})['includedPermissions']
                        if op(self.data, permissions, self.data.get('value')):
                            filtered.append(resource)
                            break
                    else:
                        permissions = client_simple.execute_command('get', {
                            "name": resource['role']})['includedPermissions']
                        if op(self.data, permissions, self.data.get('value')):
                            filtered.append(resource)
                            break
                else:
                    continue
        return filtered


def register() -> None:
    from c7n_gcp.provider import resources
    from c7n_gcp.resources.resource_map import ResourceMap

    resources.register('service-account-bindings', ServiceAccountBindings)
    ResourceMap[
        'gcp.service-account-bindings'] = f'{__name__}.{ServiceAccountBindings.__name__}'

    ServiceAccountBindings.filter_registry.register('new-roles-iam-filter',
                                                    NewRolesIAMFilter)
