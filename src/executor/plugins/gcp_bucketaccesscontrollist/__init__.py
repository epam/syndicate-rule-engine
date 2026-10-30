from c7n_gcp.query import ChildResourceManager, ChildTypeInfo

class BucketAccessControlList(ChildResourceManager):

    class resource_type(ChildTypeInfo):
        service = 'storage'
        version = 'v1'
        component = 'bucketAccessControls'
        scope = 'bucket'
        enum_spec = ('list', 'items[]', None)
        name = id = 'buckets'
        default_report_fields = [name, 'items']
        asset_type = 'storage.googleapis.com/Bucket'
        permissions = ('storage.buckets.list',)
        parent_spec = {
            'resource': 'log-project-sink',
            'child_enum_params': [
                ('name', 'bucket',),
            ]}

    def get_resource_manager(self, resource_type, data=None):
        data = data or {}
        data['filters'] = list(data.get('filters', [])) + [
            {
                'type': 'value',
                'key': 'destination',
                'op': 'regex',
                'value': r'^storage\.googleapis\.com/',
            }
        ]

        return super().get_resource_manager(resource_type, data)

    def _get_child_enum_args(self, parent_instance):
        bucket = parent_instance['destination'].split('/')[1]
        return {'bucket': bucket}


def register() -> None:
    from c7n_gcp.provider import resources
    from c7n_gcp.resources.resource_map import ResourceMap

    resources.register('bucket-access-control-list', BucketAccessControlList)
    ResourceMap['gcp.bucket-access-control-list'] = f'{__name__}.{BucketAccessControlList.__name__}'
