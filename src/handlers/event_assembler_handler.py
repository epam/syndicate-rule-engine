import heapq
from http import HTTPStatus
from itertools import chain
import operator
import time
from typing import Generator

from modular_sdk.models.tenant import Tenant
from modular_sdk.services.tenant_service import TenantService

from helpers import adjust_cloud
from helpers.constants import (
    AWS_VENDOR,
    BatchJobEnv,
    BatchJobType,
    CAASEnv,
    MAESTRO_VENDOR,
)
from helpers.lambda_response import build_response
from helpers.log_helper import get_logger
from helpers.system_customer import SYSTEM_CUSTOMER
from services import SERVICE_PROVIDER
from services import modular_helpers
from services.batch_results_service import BatchResults, BatchResultsService
import services.cache as cache
from services.clients.batch import BatchClient
from services.environment_service import EnvironmentService
from services.event_processor_service import (
    AccountRegionRuleMap,
    BaseEventProcessor,
    CloudTenantRegionRulesMap,
    EventBridgeEventProcessor,
    EventProcessorService,
    MaestroEventProcessor,
    RegionRuleMap,
)
from services.event_service import Event, EventService
from services.license_service import License, LicenseService
from services.ruleset_service import Ruleset, RulesetService
from services.setting_service import EVENT_CURSOR_TIMESTAMP_ATTR, SettingsService

DEFAULT_NOT_FOUND_RESPONSE = 'No events to assemble and process.'
DEFAULT_UNRESOLVED_RESPONSE = 'Request has run into an unresolvable issue.'

_LOG = get_logger(__name__)


class EventAssemblerHandler:
    _code: int
    _content: str | None

    def __init__(
            self, event_service: EventService,
            settings_service: SettingsService,
            tenant_service: TenantService,
            ruleset_service: RulesetService,
            event_processor_service: EventProcessorService,
            license_service: LicenseService,
            environment_service: EnvironmentService,
            batch_results_service: BatchResultsService,
            batch_client: BatchClient,

    ):
        self._event_service = event_service
        self._event_processor_service = event_processor_service
        self._settings_service = settings_service
        self._tenant_service = tenant_service
        self._ruleset_service = ruleset_service
        self._environment_service = environment_service
        self._license_service = license_service
        self._batch_results_service = batch_results_service
        self._batch_client = batch_client

        # this cache does not affect user directly, so we can put custom
        # ttl that does not depend on env
        self._rulesets_cache = cache.factory(ttu=lambda k, v, now: now + 900)
        self._reset()

    def _log_cache(self) -> None:
        """
        Just for debug
        """
        attrs = filter(
            lambda name: name.endswith('_cache') and name != '_log_cache',
            dir(self)
        )
        for attr in attrs:
            _LOG.debug(f'{attr}: {getattr(self, attr)}')

    @cache.cachedmethod(operator.attrgetter('_rulesets_cache'))
    def get_ruleset(self, _id: str) -> Ruleset | None:
        """
        Supposed to be used with licensed rule-sets.
        """
        return self._ruleset_service.by_lm_id(_id)

    @classmethod
    def instantiate(cls) -> 'EventAssemblerHandler':
        return cls(
            event_service=SERVICE_PROVIDER.event_service,
            settings_service=SERVICE_PROVIDER.settings_service,
            tenant_service=SERVICE_PROVIDER.modular_client.tenant_service(),
            ruleset_service=SERVICE_PROVIDER.ruleset_service,
            event_processor_service=SERVICE_PROVIDER.event_processor_service,
            license_service=SERVICE_PROVIDER.license_service,
            environment_service=SERVICE_PROVIDER.environment_service,
            batch_results_service=SERVICE_PROVIDER.batch_results_service,
            batch_client=SERVICE_PROVIDER.batch,
        )

    @property
    def response(self):
        _unresolved = DEFAULT_UNRESOLVED_RESPONSE
        _def_code_map = {
            HTTPStatus.NOT_FOUND.value: DEFAULT_NOT_FOUND_RESPONSE
        }
        _code, _content = self._code, self._content
        if not _content:
            _content = _def_code_map.get(_code, _unresolved)
        self._reset()
        return build_response(code=_code, content=_content)

    def _reset(self):
        self._code: int | None = HTTPStatus.INTERNAL_SERVER_ERROR
        self._content: str | None = None

    def get_allowed_event_driven_license(self, tenant: Tenant
                                         ) -> License | None:
        """
        Makes all the necessary steps to retrieve a license item, which
        allows event-driven for the given tenant if such a license exists
        """
        lic = self._license_service.get_tenant_license(tenant)
        if not lic:
            _LOG.info(f'Tenant {tenant} does not have linked license')
            return
        if lic.is_expired():
            _LOG.info(f'The license {lic.license_key}has expired')
            return
        if not self._license_service.is_subject_applicable(
                lic=lic,
                customer=tenant.customer_name,
                tenant_name=tenant.name):
            _LOG.info(f'License {lic.license_key} is not applicable')
            return
        if not lic.event_driven.get('active'):
            _LOG.info(f'Event driven is not active for '
                      f'license {lic.license_key}')
            return
        return lic

    def handler(self, event):
        self._log_cache()

        # Collect all events since the last execution.
        _LOG.info('Going to obtain cursor value of the event assembler.')
        event_cursor = None
        config = self._settings_service.get_event_assembler_configuration()
        if config and EVENT_CURSOR_TIMESTAMP_ATTR in config:
            event_cursor = float(config[EVENT_CURSOR_TIMESTAMP_ATTR])
            _LOG.info(f'Cursor was obtained: {event_cursor}')
        # Establishes Events: List[$oldest, ... $nearest]
        events = self._obtain_events(since=event_cursor)

        if not events:
            _LOG.info('No events have been collected.')

            self._code = HTTPStatus.NOT_FOUND
            return self.response

        start_event = events[0]
        end_event = events[-1]
        config = self._settings_service.create_event_assembler_configuration(
            cursor=end_event.timestamp
        )
        self._settings_service.save(setting=config)
        _LOG.info('Cursor value of the event assembler has bee updated '
                  f'to - {end_event.timestamp}')

        vendor_maps = self.vendor_rule_map(events)
        tenant_batch_result: list[tuple[Tenant, BatchResults]] = []
        for vendor, mapping in vendor_maps.items():
            if not mapping:
                _LOG.warning(f'{vendor}`s mapping is empty. Skipping')
                continue
            # handler must yield tuples (Tenant, BatchResult)
            if vendor == MAESTRO_VENDOR:
                tenant_batch_result.extend(self.handle_maestro_vendor(mapping))
            elif vendor == AWS_VENDOR:
                tenant_batch_result.extend(self.handle_aws_vendor(mapping))

        if not tenant_batch_result:
            self._code = 404
            self._content = 'Could derive no BatchResults. Skipping'
            return self.response

        # here we leave only batch_results of tenants for which ed is
        # enabled by license. And also leave only rules that available by
        # that license.
        allowed_batch_results = []
        for tenant, br in tenant_batch_result:
            _license = self.get_allowed_event_driven_license(tenant)
            if not _license:
                continue
            # by here we have license item which allows event-driven for
            # current tenant. Now we only have to restrict the list or rules

            # here just a chain of generators in order to make the
            # total number of iterations by all the rules equal to 1 with
            # small number of lines or code

            # all rule-sets ids provided by the license
            ids = iter(set(_license.ruleset_ids or []))
            # all rule-sets items
            _rule_sets = (self.get_ruleset(_id) for _id in ids)
            # Only rule-sets for tenant's cloud
            _rule_sets = filter(
                lambda r: r.cloud == adjust_cloud(tenant.cloud), _rule_sets
            )
            # all the rules ids from rule-sets
            allowed_rules = set(chain.from_iterable(
                iter(r.rules) for r in _rule_sets
            ))
            # all the rules Custom Core's names (without versions)
            # allowed_rules = set(
            #     self._rule_service.i_without_version(raw_rules)
            # )

            _LOG.debug(f'Tenant {br.tenant_name} is allowed to use '
                       f'such rules: {allowed_rules}')
            region_rules_map = br.rules.as_dict() if not isinstance(
                br.rules, dict) else br.rules
            _LOG.debug(f'Restricting {region_rules_map} from event to '
                       f'the scope of allowed rules')
            restricted_map = self.restrict_region_rule_map(region_rules_map,
                                                           allowed_rules)
            if not restricted_map:
                _LOG.info('No rules after restricting left. Skipping')
                continue
            _LOG.debug(f'Optimizing region rules map {restricted_map} size')
            br.rules = self._optimize_region_rule_map_size(restricted_map)
            allowed_batch_results.append(br)
        if not allowed_batch_results:
            self._code = 404
            self._content = 'No batch results left after checking licenses'
            return self.response
        # here we already have allowed batch_results. Just start a job.
        common_envs = self._build_common_envs()
        common_envs[BatchJobEnv.BATCH_RESULTS_IDS.value] = ','.join(
            item.id for item in allowed_batch_results)
        for br in allowed_batch_results:
            br.registration_start = str(start_event.timestamp)
            br.registration_end = str(end_event.timestamp)
        self._batch_results_service.batch_save(allowed_batch_results)
        job_id = self._submit_batch_job(common_envs)
        self._code = 202
        self._content = f'AWS Batch job were submitted: {job_id}'
        return self.response

    def handle_aws_vendor(self, cid_rg_rl_map: AccountRegionRuleMap
                          ) -> Generator[tuple[Tenant, BatchResults], None, None]:
        """
        cid_rg_rl_map = {
            '12424123423': {
                'eu-central-1': {'rule1', 'rule2'}
            }
        }
        """
        for cid in cid_rg_rl_map:
            tenant: Tenant = self._obtain_tenant_by_acc(cid)
            if not tenant:
                _LOG.info(f'Tenant {cid} not found. Skipping')
                continue

            # rules to exclude
            accessible_rg_rl_map = self._obtain_tenant_based_region_rule_map(
                tenant=tenant, region_rule_map=cid_rg_rl_map[cid]
            )
            if not accessible_rg_rl_map:
                _LOG.warning(f'No rules within region(s) are accessible to '
                             f'\'{tenant.name}\' tenant.')
                continue
            batch_result = self._batch_results_service.create(
                rules=accessible_rg_rl_map,
                tenant_name=tenant.name,
                customer_name=tenant.customer_name,
                cloud_identifier=tenant.project
            )
            yield tenant, batch_result

    def handle_maestro_vendor(self, cl_tn_rg_rl_map: CloudTenantRegionRulesMap
                              ) -> Generator[tuple[Tenant, BatchResults], None, None]:
        """
        Separate logic for maestro audit events
        {
            'AZURE': {
                'TEST_TENANT': {
                    'global': {'rule1', 'rule2'}
                }
            }
        }
        Azure tenants will always contain one mock region. But such a
        structure is kept in case we want to process AWS maestro events
        """
        _LOG.debug('Processing maestro vendor')
        for cloud, tn_rg_rl_map in cl_tn_rg_rl_map.items():
            for tenant_name, rg_rl_map in tn_rg_rl_map.items():
                tenant = self._obtain_tenant(tenant_name)  # only active
                if not tenant:
                    _LOG.warning(f'Tenant {tenant_name} not found. Skipping')
                    continue
                # here we skip restrictions by regions for AZURE because
                # currently I don't know how it's supposed to work
                batch_result = self._batch_results_service.create(
                    rules=rg_rl_map,
                    # sets must be casted to lists after. Otherwise it won't be saved
                    tenant_name=tenant.name,
                    customer_name=tenant.customer_name,
                    cloud_identifier=tenant.project,
                )
                yield tenant, batch_result

    def _obtain_events(self, since: float | None = None) -> list[Event]:
        """
        Makes N queries. N is a number of partitions (from envs). After that
        merges these N already sorted lists
        :param since:
        :return:
        """
        iters = []
        for partition in range(
                self._environment_service.number_of_partitions_for_events()):
            _LOG.debug(
                f'Making query for {partition} partition since: {since}')
            iters.append(
                self._event_service.get_events(partition, since=since)
            )
        # actually, there is no need to merge all the lists, we just need to
        # first and last timestamp. But this "merge" must be fast
        return list(heapq.merge(*iters, key=lambda e: e.timestamp))

    def vendor_rule_map(self, events: list[Event]) -> dict[str, dict]:
        """
        For each vendor derives rules mapping in its format. The formats of
        each vendor differ
        """
        vendor_processor = {
            MAESTRO_VENDOR: self._event_processor_service.get_processor(
                MAESTRO_VENDOR),
            AWS_VENDOR: self._event_processor_service.get_processor(AWS_VENDOR)
        }
        for event in events:
            if event.vendor not in vendor_processor:
                _LOG.warning(f'Not known vendor: {event.vendor}. Skipping')
                continue
            processor: BaseEventProcessor = vendor_processor[event.vendor]
            processor.events.extend(event.events)
        _LOG.info('All the vendor-processors are initialized with events')
        result = {}

        maestro_proc: MaestroEventProcessor = vendor_processor[MAESTRO_VENDOR]
        it = maestro_proc.without_duplicates(maestro_proc.prepared_events())
        result[MAESTRO_VENDOR] = maestro_proc.cloud_tenant_region_rules_map(it)

        aws_proc: EventBridgeEventProcessor = vendor_processor[AWS_VENDOR]
        it = aws_proc.without_duplicates(aws_proc.prepared_events())
        result[AWS_VENDOR] = aws_proc.account_region_rule_map(it)
        return result

    def _obtain_tenant(self, name: str) -> Tenant | None:
        _LOG.info(f'Going to retrieve Tenant by \'{name}\' name.')
        _head = f'Tenant:\'{name}\''
        tenant = self._tenant_service.get(name)
        if not modular_helpers.is_tenant_valid(tenant=tenant):
            _LOG.warning(_head + ' is inactive or does not exist')
            return
        return tenant

    def _obtain_tenant_by_acc(self, acc: str) -> Tenant | None:
        _LOG.info(f'Going to retrieve Tenant by \'{acc}\' cloud id')
        return next(self._tenant_service.i_get_by_acc(
            acc=str(acc),
            active=True,
            limit=1
        ), None)

    @staticmethod
    def _obtain_tenant_based_region_rule_map(tenant: Tenant,
                                             region_rule_map: RegionRuleMap):
        tn = tenant.name
        _LOG.info(f'Going to restrict {list(region_rule_map)} regions, '
                  f'based on ones accessible to \'{tn}\' Tenant.')
        ref = {}
        # Maestro's regions in tenants have attribute "is_active" ("act").
        # But currently (30.01.2023) they ignore it. They deem all the
        # regions listed in an active tenant to be active as well. So do we
        active_rg = modular_helpers.get_tenant_regions(tenant)
        for region in region_rule_map:
            if region not in active_rg:
                _LOG.warning(f'Going to exclude `{region}` region(s) based '
                             f'on `{tn}` tenant inactive region state.')
                continue
            ref[region] = region_rule_map[region]
        return ref

    @staticmethod
    def restrict_region_rule_map(mapping: RegionRuleMap,
                                 allowed_rules: set[str]) -> RegionRuleMap:
        """
        Restrict rules in RegionRuleMap to the scope of allowed rules
        """
        result = {}
        for region, rules in mapping.items():
            intersection = rules & allowed_rules
            if intersection:
                result[region] = intersection
        return result

    @staticmethod
    def _optimize_region_rule_map_size(mapping: RegionRuleMap
                                       ) -> dict[str, list[str]]:
        """
        On small payload the benefit is not clearly visible. The method can
        be skipped as well. Docker can parse both payloads.
        input = {
            'eu-central-1': {'one', 'two', 'three'},
            'eu-west-1': {'one', 'two', 'four'},
            'eu-west-2': {'one', 'five'}
        }
        output = {
            'eu-central-1,eu-west-1': ['two'],
            'eu-central-1': ['three'],
            'eu-central-1,eu-west-1,eu-west-2': ['one'],
            'eu-west-1': ['four'],
            'eu-west-2': ['five']
        }
        """
        _RuleRegionsMap = dict[str, set[str]]

        def _rule_to_regions(m: RegionRuleMap) -> _RuleRegionsMap:
            ref = {}
            for region, rules in m.items():
                for rule in rules:
                    ref.setdefault(rule, set()).add(region)
            return ref

        def _optimized(m: _RuleRegionsMap) -> dict[str, list[str]]:
            ref = {}
            for rule, regions in m.items():
                ref.setdefault(','.join(sorted(regions)), []).append(rule)
            return ref

        return _optimized(_rule_to_regions(mapping))

    def _build_common_envs(self) -> dict:
        return {
            CAASEnv.REPORTS_BUCKET_NAME.value:
                self._environment_service.default_reports_bucket_name(),
            CAASEnv.STATISTICS_BUCKET_NAME.value:
                self._environment_service.get_statistics_bucket_name(),
            CAASEnv.RULESETS_BUCKET_NAME.value:
                self._environment_service.get_rulesets_bucket_name(),
            BatchJobEnv.AWS_REGION.value:
                self._environment_service.aws_region(),
            CAASEnv.BATCH_JOB_LIFETIME_MINUTES.value:
                self._environment_service.get_job_lifetime_min(),
            BatchJobEnv.JOB_TYPE.value: BatchJobType.EVENT_DRIVEN.value,
            'LOG_LEVEL': self._environment_service.batch_job_log_level(),
            BatchJobEnv.SYSTEM_CUSTOMER_NAME.value: SYSTEM_CUSTOMER,
        }

    def _submit_batch_job(self, environment: dict[str, str]) -> str | None:

        job_name = f'event-driven-job-{int(time.time())}'
        _LOG.info(f'Submitting AWS Batch job with a name - \'{job_name}\'.')

        try:
            response = self._batch_client.submit_job(
                job_name=job_name,
                job_queue=self._environment_service.get_batch_job_queue(),
                job_definition=self._environment_service.get_batch_job_def(),
                environment_variables=environment,
            )
            _LOG.info(f'Batch response: {response}')
            return response['jobId']
        except (BaseException, Exception) as e:
            _LOG.exception(
                f'Some issue has occurred during '
                f'{job_name} Batch Job submission'
            )
            return


# probably not used anymore because events are removed by their ttl
class EventRemoverHandler:
    def __init__(self, settings_service: SettingsService,
                 event_service: EventService,
                 environment_service: EnvironmentService):
        self._settings_service = settings_service
        self._event_service = event_service
        self._environment_service = environment_service

    @classmethod
    def instantiate(cls) -> 'EventRemoverHandler':
        return cls(
            settings_service=SERVICE_PROVIDER.settings_service,
            event_service=SERVICE_PROVIDER.event_service,
            environment_service=SERVICE_PROVIDER.environment_service
        )

    def _obtain_events(self, till: float) -> list[Event]:
        """
        Makes N queries. N is a number of partitions (from envs). After that
        merges these N already sorted lists
        :param since:
        :return:
        """
        iters = []
        for partition in range(
                self._environment_service.number_of_partitions_for_events()):
            iters.append(
                self._event_service.get_events(partition, till=till)
            )
        # actually, there is no need to merge all the lists, we just need to
        # first and last timestamp. But this "merge" must be fast
        # return list(heapq.merge(*iters, key=lambda e: e.timestamp))
        return list(chain.from_iterable(iters))

    def handler(self, event: dict) -> dict:

        _LOG.info('Going to procure cursor value of the event assembler.')
        event_cursor = None
        config: dict = self._settings_service.get_event_assembler_configuration()
        if config and EVENT_CURSOR_TIMESTAMP_ATTR in config:
            event_cursor = float(config[EVENT_CURSOR_TIMESTAMP_ATTR])
        if not event_cursor:
            return build_response(
                content='Event cursor has not been initialized yet. '
                        'No events to clear.')

        events = self._obtain_events(till=event_cursor)
        _len = len(events)

        if _len == 0:
            _LOG.info('No events have been collected.')
            return build_response(
                content=f'No events till {event_cursor} exist in DB')
        _LOG.info(f'Going to remove {_len} old events from CaaSEvents')
        self._event_service.batch_delete(iter(events))
        message = f'{_len} old events were removed successfully'
        _LOG.info(message)
        return build_response(content=message)
