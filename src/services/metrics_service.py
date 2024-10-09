import time
import uuid
from typing import List, Union, Iterable, Generator

from pynamodb.exceptions import QueryError, PutError

from helpers import batches
from helpers import get_logger, hashable, filter_dict
from helpers.constants import GLOBAL_REGION, REPORT_FIELDS
from models.customer_metrics import CustomerMetrics
from models.tenant_metrics import TenantMetrics
from models.rule import RuleIndex
from services.mappings_collector import LazyLoadedMappingsCollector
from services.sharding import ShardsCollection, BaseShardPart

_LOG = get_logger(__name__)
ResourcesGenerator = Generator[tuple[str, str, dict, float], None, None]


class MetricsService:
    def __init__(self, mappings_collector: LazyLoadedMappingsCollector):
        self.mappings_collector = mappings_collector

    @staticmethod
    def adjust_resource_type(resource_type: str) -> str:
        """
        Removes cloud prefix from resource type
        :param resource_type:
        :return:
        """
        return resource_type.split('.', maxsplit=1)[-1]

    def batch_save(self, metrics: List[Union[CustomerMetrics, TenantMetrics]],
                   limit: int = 10,
                   retry_delay: int = 10):
        for _list in batches(metrics, limit):
            while True:
                try:
                    self._batch_write(_list)
                    break
                except PutError as e:
                    if e.cause_response_code == \
                            'ProvisionedThroughputExceededException':
                        _LOG.warning('Request rate on CaaSTenantMetrics '
                                     'table is too high!')
                        time.sleep(retry_delay)
                    else:
                        raise e

    @staticmethod
    def _batch_write(metrics: List[Union[CustomerMetrics, TenantMetrics]]):
        table = TenantMetrics if isinstance(metrics[0], TenantMetrics) \
            else CustomerMetrics
        with table.batch_write() as batch:
            for m in metrics:
                batch.save(m)

    @staticmethod
    def custom_attr(name: str) -> str:
        """
        Adds prefix to the attribute name to mark that it's custom
        :param name:
        :return:
        """
        return f'sre:{name}'

    @staticmethod
    def is_custom_attr(name: str) -> bool:
        return name.startswith('sre:')

    @staticmethod
    def allow_only_regions(it: ResourcesGenerator, regions: set[str]
                           ) -> ResourcesGenerator:
        for rule, region, dto, ts in it:
            if region in regions:
                yield rule, region, dto, ts

    def allow_only_resource_type(self, it: ResourcesGenerator, meta: dict,
                                 resource_type: str
                                 ) -> ResourcesGenerator:
        for rule, region, dto, ts in it:
            rt = self.adjust_resource_type(meta.get(rule, {}).get('resource'))
            if rt == self.adjust_resource_type(resource_type):
                yield rule, region, dto, ts

    @staticmethod
    def iter_resources(it: Iterable[BaseShardPart]) -> ResourcesGenerator:
        for part in it:
            for res in part.resources:
                yield part.policy, part.location, res, part.timestamp

    @staticmethod
    def deduplicated(it: ResourcesGenerator) -> ResourcesGenerator:
        """
        This generator goes through resources and yields only unique ones
        within rule and region
        :param it:
        :return:
        """
        emitted = {}
        for rule, region, dto, ts in it:
            _emitted = emitted.setdefault((rule, region), set())
            _hashable = hashable(dto)
            if _hashable in _emitted:
                _LOG.debug(f'Duplicate found for {rule}:{region}')
                continue
            yield rule, region, dto, ts
            _emitted.add(_hashable)

    def custom_modify(self, it: ResourcesGenerator, meta: dict
                      ) -> ResourcesGenerator:
        """
        Some resources require special treatment.
        - rules with resource type "aws.cloudtrail" are not multiregional,
        but the resource they look for can be either multiregional or not.
        So we must deduplicate them on the scope of whole account.
        252 and other glue-catalog rules are not multiregional, but they
        also do not return unique information within region.
        :param it:
        :param meta:
        :return:
        """
        # TODO in case we need more business logic here, redesign this
        #  solution. Maybe move this logic to a separate class
        for rule, region, dto, ts in it:
            rt = meta.get(rule).get('resource')
            comment = RuleIndex(meta.get(rule, {}).get('comment', ''))
            if comment.is_global:  # todo, do we need this?
                yield rule, GLOBAL_REGION, dto, ts
                continue

            rt = self.adjust_resource_type(rt)
            if rt in ('glue-catalog', 'account'):
                _LOG.debug(f'Rule with type {rt} found. Adding region '
                           f'attribute to make its dto differ from '
                           f'other regions')
                dto[self.custom_attr('region')] = region
            elif rt == 'cloudtrail':
                if dto.get('IsMultiRegionTrail'):
                    _LOG.debug('Found multiregional trail. '
                               'Moving it to multiregional region')
                    region = GLOBAL_REGION
            yield rule, region, dto, ts

    def report_fields(self, rule: str) -> set[str]:
        rf = set(self.mappings_collector.human_data.get(rule, {}).get('report_fields') or [])  # noqa
        return rf | REPORT_FIELDS

    def keep_report_fields(self, it: ResourcesGenerator) -> ResourcesGenerator:
        """
        Keeps only report fields for each resource. Custom attributes are
        not removed because they are added purposefully
        :param it:
        :return:
        """
        for rule, region, dto, ts in it:
            filtered = filter_dict(dto, self.report_fields(rule))
            filtered.update({
                k: v for k, v in dto.items() if self.is_custom_attr(k)
            })
            yield rule, region, filtered, ts

    def create_resources_generator(self, collection: ShardsCollection,
                                   active_regions: set | list
                                   ) -> ResourcesGenerator:
        # just iterate over resources
        resources = self.iter_resources(collection.iter_parts())

        # modify dto for some exceptional rules, see generator's description
        resources = self.custom_modify(resources, collection.meta)

        # keeping only report fields
        resources = self.keep_report_fields(resources)

        # removing duplicates within rule-region (probably no need)
        resources = self.deduplicated(resources)

        # keeping only active regions and global
        return self.allow_only_regions(resources,
                                       {GLOBAL_REGION, *active_regions})


class CustomerMetricsService(MetricsService):

    @staticmethod
    def get_all_metrics():
        return list(CustomerMetrics.scan())

    @staticmethod
    def create(data) -> CustomerMetrics:
        return CustomerMetrics(**data, id=str(uuid.uuid4()))

    @staticmethod
    def save(metrics: CustomerMetrics):
        metrics.save()

    @staticmethod
    def get_nearest_date(customer, nearest_to):
        after = list(CustomerMetrics.customer_date_index.query(
            hash_key=customer,
            filter_condition=(CustomerMetrics.date >= nearest_to),
            scan_index_forward=False,
            limit=1))
        after = after[0] if after else None
        before = list(CustomerMetrics.customer_date_index.query(
            hash_key=customer,
            filter_condition=(CustomerMetrics.date < nearest_to),
            scan_index_forward=True,
            limit=1))
        before = before[0] if before else None
        if after and before:
            delta_after = abs(after.date - nearest_to)
            delta_before = abs(before.date - nearest_to)

            return after if delta_after < delta_before else before
        if after:
            return after
        if before:
            return before

    @staticmethod
    def get_by_customer_date_type(customer, date, _type) -> CustomerMetrics:
        metrics = list(CustomerMetrics.customer_date_index.query(
            hash_key=customer,
            range_key_condition=CustomerMetrics.date == date,
            filter_condition=CustomerMetrics.type == _type,
            scan_index_forward=False))
        if metrics:
            return metrics[0]

    @staticmethod
    def list_by_date_and_customer(date: str, customer: str, limit: int = 50):
        result = []
        response = CustomerMetrics.customer_date_index.query(
            hash_key=customer,
            range_key_condition=CustomerMetrics.date.startswith(date),
            limit=limit)
        result.extend(list(response))
        last_evaluated_key = response.last_evaluated_key

        while last_evaluated_key:
            try:
                response = CustomerMetrics.customer_date_index.query(
                    hash_key=customer,
                    last_evaluated_key=last_evaluated_key,
                    range_key_condition=CustomerMetrics.date.startswith(date),
                    limit=limit)
                result.extend(list(response))
                last_evaluated_key = response.last_evaluated_key
            except QueryError as e:
                if e.cause_response_code == \
                        'ProvisionedThroughputExceededException':
                    _LOG.warning('Request rate on CaaSCustomerMetrics table '
                                 'is too high!')
                    time.sleep(10)
                else:
                    raise e
        return result

    @staticmethod
    def get_all_types_by_customer_date(customer: str, date: str,
                                       overview_only: bool = False):
        result = {'ATTACK_VECTOR': False,
                  'COMPLIANCE': False,
                  'OVERVIEW': False}
        if overview_only:
            result = {'OVERVIEW': False}
        for _type, _ in result.items():
            if list(CustomerMetrics.customer_date_index.query(
                    hash_key=customer, limit=1,
                    range_key_condition=CustomerMetrics.date.startswith(date),
                    filter_condition=TenantMetrics.type == _type)):
                result[_type] = True
        return result


class TenantMetricsService(MetricsService):

    @staticmethod
    def get_all_metrics():
        return list(TenantMetrics.scan())

    @staticmethod
    def create(data) -> TenantMetrics:
        return TenantMetrics(**data, id=str(uuid.uuid4()))

    @staticmethod
    def save(metrics: TenantMetrics):
        metrics.save()

    @staticmethod
    def get_by_customer_and_date(customer: str, date: str):
        return list(TenantMetrics.customer_date_index.query(
            hash_key=customer,
            range_key_condition=TenantMetrics.date == date)
        )

    @staticmethod
    def list_by_date_and_customer(date: str, customer: str, limit: int = 50):
        result = []
        response = TenantMetrics.customer_date_index.query(
            hash_key=customer,
            range_key_condition=CustomerMetrics.date.startswith(date),
            limit=limit)
        result.extend(list(response))
        last_evaluated_key = response.last_evaluated_key

        while last_evaluated_key:
            try:
                response = TenantMetrics.customer_date_index.query(
                    hash_key=customer,
                    last_evaluated_key=last_evaluated_key,
                    range_key_condition=TenantMetrics.date.startswith(date),
                    limit=limit)
                result.extend(list(response))
                last_evaluated_key = response.last_evaluated_key
            except QueryError as e:
                if e.cause_response_code == \
                        'ProvisionedThroughputExceededException':
                    _LOG.warning('Request rate on CaaSJobs table is too '
                                 'high!')
                    time.sleep(10)
                else:
                    raise e
        return result

    @staticmethod
    def get_by_tenant_date_type(tenant: str, date: str,
                                top_type: str) -> TenantMetrics:
        metrics = None
        try:
            metrics = list(TenantMetrics.tenant_date_index.query(
                limit=1,
                hash_key=tenant,
                range_key_condition=TenantMetrics.date == date,
                filter_condition=TenantMetrics.type == top_type,
                scan_index_forward=False))
        except QueryError as e:
            if e.cause_response_code == \
                    'ProvisionedThroughputExceededException':
                _LOG.warning('Request rate on CaaSCustomerMetrics table '
                             'is too high!')
                time.sleep(5)
            else:
                raise e
        if metrics:
            return metrics[0]

    @staticmethod
    def get_all_types_by_customer_date(date: str, customer: str):
        result = {'ATTACK_BY_TENANT': False,
                  'ATTACK_BY_CLOUD': False,
                  'COMPLIANCE_BY_CLOUD': False,
                  'COMPLIANCE_BY_TENANT': False,
                  'RESOURCES_BY_CLOUD': False,
                  'RESOURCES_BY_TENANT': False}
        for _type, _ in result.items():
            if list(TenantMetrics.customer_date_index.query(
                    hash_key=customer,  limit=1,
                    range_key_condition=TenantMetrics.date.startswith(date),
                    filter_condition=TenantMetrics.type == _type)):
                result[_type] = True
        return result
