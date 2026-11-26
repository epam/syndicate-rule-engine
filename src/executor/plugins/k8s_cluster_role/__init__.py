from typing import Any

from c7n_kube.resources.rbac.role import ClusterRole as _ClusterRole

from helpers.k8s_rbac import annotate_aggregation


class ClusterRole(_ClusterRole):
    """
    k8s.cluster-role with aggregationRule support. Each cluster role is
    annotated with:
    - ``c7n:aggregated-into``: aggregating cluster roles whose
      clusterRoleSelectors match this role's labels, so the subjects bound
      to them also receive this role's permissions;
    - ``c7n:aggregated-from``: (only for roles with aggregationRule) cluster
      roles selected by this role.

    Augmentation uses the full list of cluster roles fetched by the source,
    so no extra API calls are made.
    """

    def augment(self, resources: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return annotate_aggregation(super().augment(resources))


def register() -> None:
    from c7n_kube.provider import resources
    from c7n_kube.resources.resource_map import ResourceMap

    resources.register('cluster-role', ClusterRole)
    ResourceMap['k8s.cluster-role'] = f'{__name__}.{ClusterRole.__name__}'
