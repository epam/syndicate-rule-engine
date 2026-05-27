"""
https://github.com/cloud-custodian/cloud-custodian/pull/8902
"""
import re

from c7n_gcp.query import QueryResourceManager, TypeInfo, ChildResourceManager, \
    ChildTypeInfo


class LoggingSinkBucket(ChildResourceManager):

    def _get_arent_resource_info(self, child_instance):
        mappings = {}
        project_param_re = re.compile('.*?/storage/v1/b/.*')
        mappings['bucket_name'] = project_param_re.match(child_instance['selfLink']).group(1)
        return mappings

    class resource_type(ChildTypeInfo):
        service = 'storage'
        version = 'v1'
        component = 'buckets'
        scope = 'project'
        enum_spec = ('list', 'items[]', None)
        name = id = 'name'
        default_report_fields = ['name', 'destination', 'createTime',
                                 'updateTime', 'filter', 'writerIdentity']
        parent_spec = {
            'resource': 'logging-sink',
            'child_enum_params': [

            ],
            'parent_get_params': [
                ('bucket', 'bucket_name'),
            ]
        }
        asset_type = 'storage.googleapis.com/Bucket'

        @staticmethod
        def get(client, resource_info):
            return client.execute_command(
                'get', {'bucket': resource_info['bucket_name']})

    def _fetch_resources(self, query):
        if not query:
            query = {}

        resources = []
        annotation_key = self.resource_type.get_parent_annotation_key()
        parent_query = self.get_parent_resource_query()
        parent_resource_manager = self.get_resource_manager(
            resource_type=self.resource_type.parent_spec['resource'],
            data=({'query': parent_query} if parent_query else {})
        )

        for parent_instance in parent_resource_manager.resources():
            if len(parent_instance['destination'].split('/')) != 2:
                continue
            else:
                query.update(self._get_child_enum_args(parent_instance))

            children = super(ChildResourceManager, self)._fetch_resources(query)

            for child_instance in children:
                for parent in [parent_instance]:
                    if child_instance['name'] in parent['destination']:
                        child_instance[annotation_key] = parent_instance
                        resources.append(child_instance)

        return resources


class LoggingSink(QueryResourceManager):
    class resource_type(TypeInfo):
        service = 'logging'
        version = 'v2'
        component = 'projects.sinks'
        enum_spec = ('list', 'sinks[]', None)
        scope_key = 'parent'
        scope_template = "projects/{}"
        name = id = 'name'
        default_report_fields = ['name', 'kind', 'items']
        asset_type = 'logging.googleapis.com/LogSink'

        @staticmethod
        def get(client, resource_info):
            return client.get('get', {
                'sinkName': 'projects/{project_id}/sinks/{name}'.format(
                    **resource_info)})


def register() -> None:
    from c7n_gcp.provider import resources
    from c7n_gcp.resources.resource_map import ResourceMap

    resources.register('logging-sink-bucket', LoggingSinkBucket)
    ResourceMap['gcp.logging-sink-bucket'] = f'{__name__}.{LoggingSinkBucket.__name__}'
    resources.register('logging-sink', LoggingSink)
    ResourceMap['gcp.logging-sink'] = f'{__name__}.{LoggingSink.__name__}'
