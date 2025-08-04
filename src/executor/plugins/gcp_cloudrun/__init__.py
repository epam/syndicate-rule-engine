from c7n.filters import ValueFilter
from c7n.utils import type_schema
from c7n_gcp.resources.gke import IAMGKENodepoolFilter


class NamespaceRevisionIAMPolicyFilter(IAMGKENodepoolFilter):
    schema = type_schema('cloud-run-revision-iam-policy-filter', rinherit=ValueFilter.schema)
    permissions = ('resourcemanager.projects.getIamPolicy',)
    service_key = 'serviceAccountName'
    resource_key = 'spec'


def register() -> None:
    from c7n_gcp.resources.cloudrun import CloudRunRevision

    CloudRunRevision.filter_registry.register('cloud-run-revision-iam-policy-filter', NamespaceRevisionIAMPolicyFilter)
