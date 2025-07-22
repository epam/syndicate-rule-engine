from typing import TYPE_CHECKING, Generator

from c7n.exceptions import PolicyExecutionError
from c7n.filters import Filter
from c7n.filters.core import OPERATORS
from c7n.utils import jmespath_search, type_schema

if TYPE_CHECKING:
    from c7n.manager import ResourceManager
    from c7n.registry import PluginRegistry


class GenericRelatedFilter(Filter):
    """
    https://github.com/cloud-custodian/cloud-custodian/pull/10065
    """

    schema = type_schema(
        'related',
        required=['policy'],
        policy={
            'type': 'object',
            'required': ['resource'],
            'additionalProperties': False,
            'properties': {
                'resource': {
                    'oneOf': [
                        {'type': 'string'},
                        {'type': 'array', 'items': {'type': 'string'}},
                    ]
                },
                'filters': {'type': 'array'},
                'query': {'type': 'array', 'items': {'type': 'object'}},
            },
        },
        ids={'type': 'string'},
        annotation={'type': 'string'},
        count={'type': 'number'},
        count_op={'$ref': '#/definitions/filters_common/comparison_operators'},
    )
    FetchThreshold = 10

    def get_permissions(self):
        rm = self.get_related_manager()
        return rm.get_permissions() if rm else ()

    @staticmethod
    def get_related_ids(path, resources):
        return set(jmespath_search('[].%s' % path, resources))

    def get_related_manager(self):
        policy = self.data.get('policy')
        if not policy:
            return
        return self.manager.get_resource_manager(policy['resource'], policy)

    @staticmethod
    def _get_by_ids_with_filtering(manager, ids):
        resources = manager.get_resources(list(ids), cache=True, augment=True)
        # TODO: maybe add manager.ctx.tracer.subsegments here the same way
        #  as inside manager.resources().
        return manager.filter_resources(resources)

    def get_related(self, resources):
        related_manager = self.get_related_manager()
        model = related_manager.get_model()

        if 'ids' not in self.data:
            related = related_manager.resources()
        else:  # 'ids' in data
            related_ids = self.get_related_ids(self.data['ids'], resources)
            if len(related_ids) > self.FetchThreshold:
                related = [
                    r
                    for r in related_manager.resources()
                    if r[model.id] in related_ids
                ]  # pragma: no cover
            else:
                related = self._get_by_ids_with_filtering(
                    related_manager, related_ids
                )

        return {r[model.id]: r for r in related}

    def _add_annotation(self, resource, related):
        if 'annotation' not in self.data:
            return
        annotation_key = self.data['annotation']
        if annotation_key in resource:
            raise PolicyExecutionError(
                f'the annotation "{annotation_key}" overlaps with an existing key. Use another one'
            )
        resource[annotation_key] = related

    def _check_count(self, rcount):
        if 'count' not in self.data:
            return False  # pragma: no cover
        op = OPERATORS[self.data.get('count_op', 'eq')]
        return op(rcount, self.data['count'])

    def check_resource(self, resource, all_related):
        if 'ids' not in self.data:
            related = list(all_related.values())
        else:
            related_ids = self.get_related_ids(self.data['ids'], [resource])
            related = [all_related[i] for i in related_ids if i in all_related]

        self._add_annotation(resource, related)

        if 'count' in self.data and self._check_count(len(related)):
            return True
        return bool(related)

    def process(self, resources, event=None):
        related = self.get_related(resources)
        return [r for r in resources if self.check_resource(r, related)]


def _iter_loaded_resources(
    reg: 'PluginRegistry',
) -> Generator['ResourceManager', None, None]:
    assert reg.plugin_type == 'c7n.providers', (
        'Expected a registry for c7n.providers'
    )
    for provider in reg.values():
        for res in provider.resources.values():
            yield res


def register() -> None:
    """
    Register the GenericRelatedFilter for all resources loaded resources
    """
    from c7n.provider import clouds

    for res in _iter_loaded_resources(clouds):
        if not hasattr(res.filter_registry, 'register') or not callable(
            res.filter_registry.register
        ):
            continue
        if 'related' in res.filter_registry:
            continue
        res.filter_registry.register('related', GenericRelatedFilter)
