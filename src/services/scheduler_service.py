from typing import Set, Optional, Iterator, List

from modular_sdk.models.tenant import Tenant

from helpers.constants import NAME_ATTR, ID_ATTR
from helpers.log_helper import get_logger
from models.scheduled_job import ScheduledJob
from services.ruleset_service import RulesetName
from services.clients.scheduler import AbstractJobScheduler
from modular_sdk.models.pynamongo.convertors import instance_as_dict

_LOG = get_logger(__name__)


class SchedulerService:
    def __init__(self, client: AbstractJobScheduler):
        self._client = client

    @staticmethod
    def get(name: str, customer: Optional[str] = None,
            tenants: Optional[Set[str]] = None) -> Optional[ScheduledJob]:
        tenants = tenants or set()
        item = ScheduledJob.get_nullable(hash_key=name)
        if not item:
            return
        if customer and item.customer_name != customer:
            return
        if tenants and item.tenant_name not in tenants:
            return
        return item

    def list(self, name: Optional[str] = None, customer: Optional[str] = None,
             tenants: Optional[Set[str]] = None) -> Iterator[ScheduledJob]:
        tenants = tenants or set()
        if name:
            _LOG.info('Scheduled job name is given querying by it')
            item = self.get(name, customer, tenants)
            return iter([item]) if item else iter([])
        # no name
        if customer and tenants and len(tenants) == 1:
            _LOG.info('Querying by customer using tenant range key '
                      'condition with one tenant')
            return ScheduledJob.customer_name_principal_index.query(
                hash_key=customer,
                range_key_condition=(ScheduledJob.tenant_name == list(tenants)[0])
            )
        if customer:  # and maybe tenants
            # we cannot use IN condition on range keys, but we also cannot use
            # range key for filter conditions. Both cases gave me exceptions.
            # I hate DynamoDB
            _LOG.info('Filtering tenants using python')
            items = ScheduledJob.customer_name_principal_index.query(
                hash_key=customer
            )
            if tenants:
                items = filter(lambda x: x.tenant_name in tenants, items)
            return items
        # no customer
        condition = None
        if tenants:
            condition &= ScheduledJob.tenant_name.is_in(*tenants)
        return ScheduledJob.scan(filter_condition=condition)

    @staticmethod
    def dto(item: ScheduledJob) -> dict:
        data = instance_as_dict(item)
        _context = data.pop('context', {})
        data[NAME_ATTR] = data.pop(ID_ATTR, None)
        data.pop('type', None)
        data['schedule'] = _context.get('schedule')
        data['scan_regions'] = _context.get('scan_regions') or []
        data['enabled'] = _context.get('is_enabled')
        rulesets = []
        for r in item.context.scan_rulesets:
            rulesets.append(RulesetName(r).to_str(False))
        data['scan_rulesets'] = rulesets
        return data

    def register_job(self, tenant: Tenant, schedule: str, envs: dict,
                     name: Optional[str] = None,
                     rulesets: List[str] | None = None) -> ScheduledJob:
        _job = self._client.register_job(tenant, schedule, envs, name,
                                         rulesets)
        return _job

    def deregister_job(self, _id):
        self._client.deregister_job(_id)

    def update_job(self, item: ScheduledJob,
                   is_enabled: Optional[bool] = None,
                   schedule: Optional[str] = None):
        self._client.update_job(item, is_enabled, schedule)
