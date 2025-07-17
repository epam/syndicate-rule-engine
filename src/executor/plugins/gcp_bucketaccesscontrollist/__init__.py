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

    def _get_child_enum_args_list(self, parent_instance):
        if parent_instance['destination'].startswith('storage'):
            bucket = parent_instance['destination'].split('/')[1]
            return [{'bucket': bucket}]
        return []




def register() -> None:
    from c7n_gcp.provider import resources
    from c7n_gcp.resources.resource_map import ResourceMap

    resources.register('bucket-access-control-list', BucketAccessControlList)
    ResourceMap['gcp.bucket-access-control-list'] = f'{__name__}.{BucketAccessControlList.__name__}'

