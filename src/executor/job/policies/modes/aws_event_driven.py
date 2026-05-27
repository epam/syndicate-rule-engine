import time

from c7n.policy import PolicyExecutionMode, execution
from c7n.query import ChildResourceManager
from c7n.utils import dumps, jmespath_compile, type_schema
from c7n.version import version

from .constants import SRE_AWS_EVENT_DRIVEN_MODE
from .sre_events import SREEvents


@execution.register(SRE_AWS_EVENT_DRIVEN_MODE)
class SreAwsEventDrivenMode(PolicyExecutionMode):
    """
    SRE Event-Driven mode execution of a policy.

    Queries resources from cloud provider for filtering and actions.
    """

    schema = type_schema(
        SRE_AWS_EVENT_DRIVEN_MODE,
        events={
            'type': 'array',
            'items': {
                'type': 'object',
                'required': [
                    'event',
                    'source',
                    'ids',
                ],
                'properties': {
                    'event': {'type': 'string'},
                    'source': {'type': 'string'},
                    'ids': {'type': 'string'},
                },
            },
        },
    )

    def validate(self) -> None:
        super(SreAwsEventDrivenMode, self).validate()
        events = self.policy.data['mode'].get('events')
        assert events, (
            'sre event driven mode requires specifiying events to subscribe'
        )
        for e in events:
            jmespath_compile(e['ids'])

        if isinstance(self.policy.resource_manager, ChildResourceManager):
            if not getattr(
                self.policy.resource_manager.resource_type,
                'supports_trailevents',
                False,
            ):
                raise ValueError(
                    'resource:%s does not support cloudtrail mode policies'
                    % (self.policy.resource_type)
                )

    def _cache_enabled(self):
        opts = self.policy.options
        return bool(opts.cache and opts.cache_period)

    def _ids_cache_key(self, resource_ids):
        rm = self.policy.resource_manager
        return rm.get_cache_key(
            ('get_resources', tuple(sorted(resource_ids)))
        )

    def resolve_resources(self, event):
        mode = self.policy.data.get('mode', {})
        resource_ids = SREEvents.get_sre_ids(event, mode)
        if not isinstance(resource_ids, (tuple, list, set)):
            resource_ids = [resource_ids]
        resource_ids = list(filter(None, resource_ids))
        self.policy.log.info('Found resource ids:%s', resource_ids)
        rm = self.policy.resource_manager
        resource_ids = rm.match_ids(resource_ids)
        if not resource_ids:
            self.policy.log.warning('Could not find resource ids')
            return []

        cache_key = None
        if self._cache_enabled():
            cache_key = self._ids_cache_key(resource_ids)
            with rm._cache:
                cached = rm._cache.get(cache_key)
            if cached is not None:
                self.policy.log.debug(
                    'Using cached get_resources for ids:%s', resource_ids
                )
                resources = cached
            else:
                resources = None
        else:
            resources = None

        if resources is None:
            resources = rm.get_resources(
                resource_ids,
                cache=False,
                augment=False,
            )
            if resources and cache_key is not None:
                with rm._cache:
                    rm._cache.save(cache_key, resources)

        if isinstance(event, dict) and event.get('debug'):
            self.policy.log.info('Resources %s', resources)
        return resources

    def run(self, events, *args, **kw):
        if not self.policy.is_runnable():
            return []
        if not events and not isinstance(events, list):
            return []

        with self.policy.ctx as ctx:
            self.policy.log.debug(
                'Running policy:%s resource:%s region:%s c7n:%s',
                self.policy.name,
                self.policy.resource_type,
                self.policy.options.region or 'default',
                version,
            )

            s = time.time()
            resources = []
            resource_not_found = 0

            for event in events:
                _resources = self.resolve_resources(event)
                if not _resources:
                    resource_not_found += 1
                    continue
                resources.extend(_resources)

            if resource_not_found > 0:
                ctx.metrics.put_metric(
                    'ResourceNotFoundCount',
                    resource_not_found,
                    'Count',
                    Scope='Policy',
                )

            resources = self.policy.resource_manager.filter_resources(
                resources,
                None,
            )

            rt = time.time() - s
            self.policy.log.info(
                'policy:%s resource:%s region:%s count:%d time:%0.2f',
                self.policy.name,
                self.policy.resource_type,
                self.policy.options.region,
                len(resources),
                rt,
            )
            ctx.metrics.put_metric(
                'ResourceCount', len(resources), 'Count', Scope='Policy'
            )
            ctx.metrics.put_metric(
                'ResourceTime', rt, 'Seconds', Scope='Policy'
            )
            ctx.output.write_file('resources.json', dumps(resources, indent=2))

            if not resources:
                return []

            if self.policy.options.dryrun:
                self.policy.log.debug('dryrun: skipping actions')
                return resources

            at = time.time()
            for a in self.policy.resource_manager.actions:
                s = time.time()
                with ctx.tracer.subsegment('action:%s' % a.type):
                    results = a.process(resources)
                self.policy.log.info(
                    'policy:%s action:%s'
                    ' resources:%d'
                    ' execution_time:%0.2f'
                    % (
                        self.policy.name,
                        a.name,
                        len(resources),
                        time.time() - s,
                    )
                )
                if results:
                    ctx.output.write_file('action-%s' % a.name, dumps(results))
            ctx.metrics.put_metric(
                'ActionTime', time.time() - at, 'Seconds', Scope='Policy'
            )
            return resources
