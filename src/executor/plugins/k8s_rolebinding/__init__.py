from typing import Any

from kubernetes.client import V1ClusterRole, V1Role
from kubernetes.client.exceptions import ApiException

from c7n_kube.query import QueryResourceManager, TypeInfo
from c7n.exceptions import PolicyExecutionError
from c7n.filters import ValueFilter
from c7n.utils import type_schema, local_session


class ClusterRoleBinding(QueryResourceManager):
    class resource_type(TypeInfo):
        group = 'RbacAuthorization'
        canonical_group = 'rbac.authorization.k8s.io'
        version = 'V1'
        patch = 'patch_cluster_role_binding'
        delete = 'delete_cluster_role_binding'
        enum_spec = ('list_cluster_role_binding', 'items', None)
        plural = 'clusterrolebindings'
        namespaced = False


class RoleBinding(QueryResourceManager):
    class resource_type(TypeInfo):
        group = 'RbacAuthorization'
        canonical_group = 'rbac.authorization.k8s.io'
        version = 'V1'
        patch = 'patch_namespaced_role_binding'
        delete = 'delete_namespaced_role_binding'
        enum_spec = ('list_role_binding_for_all_namespaces', 'items', None)
        plural = 'rolebindings'
        namespaced = True


class RoleRefFilter(ValueFilter):
    """Filter role-bindings (or cluster-role-bindings) based on the permissions
    defined in the role they reference (roleRef).

    The filter fetches the ClusterRole or Role that the binding points to via
    ``roleRef``, converts it to a plain dict, and then applies the standard
    JMESPath ``key`` / ``value`` / ``op`` match against that role's data.

    This lets you select bindings whose referenced role has specific permission
    patterns, for example finding all bindings that grant wildcard resource access:

    .. code-block:: yaml

        policies:
          - name: role-bindings-with-wildcard-resources
            resource: k8s.role-binding
            filters:
              - type: role-ref
                key: "rules[?resources && contains(resources, '*')]"
                op: ne
                value: null

          - name: role-bindings-with-wildcard-verbs
            resource: k8s.cluster-role-binding
            filters:
              - type: role-ref
                key: "rules[?verbs && contains(verbs, '*')]"
                op: ne
                value: null

    The matched role data is also annotated on the binding resource under the
    ``c7n:role-ref`` key for use in follow-up actions or notifications.
    """

    schema = type_schema('role-ref', rinherit=ValueFilter.schema)
    annotation_key = 'c7n:role-ref'

    def process(
        self,
        resources: list[dict[str, Any]],
        event: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        client = local_session(self.manager.session_factory).client(
            'RbacAuthorization', 'V1'
        )

        # Cache all cluster roles and namespaced roles to avoid repeated API calls
        # fmt: off
        cluster_roles = {
            cr.metadata.name: cr
            for cr in client.list_cluster_role().items
        }
        # fmt: on

        try:
            namespaced_roles = {
                (r.metadata.namespace, r.metadata.name): r
                for r in client.list_role_for_all_namespaces().items
            }
        except ApiException as e:
            raise PolicyExecutionError(
                f'failed to list namespaced roles for role-ref filter: {e}'
            ) from e

        result = []
        for resource in resources:
            role = self._resolve_role_ref(
                binding=resource,
                cluster_roles=cluster_roles,
                namespaced_roles=namespaced_roles,
            )
            if role is None:
                continue
            role_dict = role.to_dict()
            resource[self.annotation_key] = role_dict
            if self.match(role_dict):
                result.append(resource)
        return result

    def _resolve_role_ref(
        self,
        binding: dict[str, Any],
        cluster_roles: dict[str, V1ClusterRole],
        namespaced_roles: dict[tuple[str, str], V1Role],
    ) -> V1ClusterRole | V1Role | None:
        role_ref = binding.get('role_ref') or {}
        kind = role_ref.get('kind', '')
        name = role_ref.get('name', '')
        if not name:
            return None
        if kind == 'ClusterRole':
            return cluster_roles.get(name)
        elif kind == 'Role':
            namespace = (binding.get('metadata') or {}).get('namespace', '')
            return namespaced_roles.get((namespace, name))
        return None


def register() -> None:
    from c7n_kube.provider import resources
    from c7n_kube.resources.resource_map import ResourceMap

    # fmt: off
    resources.register('cluster-role-binding', ClusterRoleBinding)
    ResourceMap['k8s.cluster-role-binding'] = f'{__name__}.{ClusterRoleBinding.__name__}'
    # fmt: on

    resources.register('role-binding', RoleBinding)
    ResourceMap['k8s.role-binding'] = f'{__name__}.{RoleBinding.__name__}'

    RoleBinding.filter_registry.register('role-ref', RoleRefFilter)
    ClusterRoleBinding.filter_registry.register('role-ref', RoleRefFilter)
