"""
https://github.com/cloud-custodian/cloud-custodian/pull/9414
"""
from c7n.filters import ListItemFilter
from c7n.utils import type_schema


class DiskSnapshotsFilter(ListItemFilter):
    schema = type_schema(
        "snapshots",
        attrs={"$ref": "#/definitions/filters_common/list_item_attrs"},
        count={"type": "number"},
        count_op={"$ref": "#/definitions/filters_common/comparison_operators"}
    )
    annotate_items = True
    item_annotation_key = "c7n:Snapshots"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._snapshots = ()

    def process(self, resources, event=None):
        self._snapshots = tuple(
            item.serialize(True)
            for item in self.manager.get_client().snapshots.list()
        )
        return super().process(resources, event)

    def get_item_values(self, resource):
        uid = resource['properties']['uniqueId']
        filtered = []
        for item in self._snapshots:
            source_uid = item['properties'].get('creationData', {}).get('sourceUniqueId')
            if source_uid == uid:
                filtered.append(item)
        return filtered


def register() -> None:
    """
    Register the Azure Disk plugin.
    """
    from c7n_azure.resources.disk import Disk

    Disk.filter_registry.register('snapshots', DiskSnapshotsFilter)
