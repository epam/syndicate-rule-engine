"""
https://github.com/cloud-custodian/cloud-custodian/pull/8900
"""

from c7n.filters.core import Filter
from c7n.utils import local_session, type_schema


class CheckDiskAvailableSnapshotFilter(Filter):
    """
    That filter checks if the snapshot disk exists
    """
    schema = type_schema('disk-availability')
    permissions = ('compute.disks.get',)

    def process(self, resources, event=None):
        session = local_session(self.manager.session_factory)
        client = session.client(service_name='compute', version='v1',
                                component='disks')
        accepted_resources = []
        # Getting project_id from client
        project = session.get_default_project()
        aggregated_disks = client.execute_query('aggregatedList',
                                                {'project': project})
        for resource in resources:
            if 'sourceDisk' in resource:
                zone = resource['sourceDisk'].split('/')[-3]
                disk_name = resource['sourceDisk'].split('/')[-1]
                disks_in_zone = aggregated_disks["items"].get(f'zones/{zone}')
                if disks_in_zone and disks_in_zone.get('disks'):
                    disks = disks_in_zone.get('disks')
                    filtered_disks = [disk for disk in disks if
                                      disk['name'] == disk_name]
                    if filtered_disks:
                        accepted_resources.append(resource)
        return accepted_resources


def register() -> None:
    from c7n_gcp.resources.compute import Snapshot
    Snapshot.filter_registry.register('disk-availability',
                                      CheckDiskAvailableSnapshotFilter)
