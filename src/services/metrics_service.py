from typing import List, Union, Iterable, Generator, Tuple, Set

from pynamodb.exceptions import QueryError, PutError
from helpers import batches
from helpers import get_logger, generate_id, time_helper, hashable, filter_dict
from helpers.constants import MULTIREGION, AZURE_CLOUD_ATTR
from models.customer_metrics import CustomerMetrics
from models.tenant_metrics import TenantMetrics
from services.rule_meta_service import LazyLoadedMappingsCollector

_LOG = get_logger(__name__)
ResourcesGenerator = Generator[Tuple[str, str, dict], None, None]


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
                        time_helper.wait(retry_delay)
                    else:
                        raise e

    @staticmethod
    def _batch_write(metrics: List[Union[CustomerMetrics, TenantMetrics]]):
        table = TenantMetrics if isinstance(metrics[0], TenantMetrics) \
            else CustomerMetrics
        with table.batch_write() as batch:
            for m in metrics:
                batch.save(m)

    def is_multiregional(self, rule: str) -> bool:
        return self.mappings_collector.human_data.get(
            rule, {}).get('multiregional')

    @staticmethod
    def custom_attr(name: str) -> str:
        """
        Adds prefix to the attribute name to mark that it's custom
        :param name:
        :return:
        """
        return f'c7n-service:{name}'

    @staticmethod
    def is_custom_attr(name: str) -> bool:
        return name.startswith('c7n-service:')

    @staticmethod
    def iter_resources(findings: dict
                       ) -> ResourcesGenerator:
        """
        This generator goes through findings and yields a resource, it's rule
        and region where it was found.
        :param findings: raw findings
        :yield: (rule, region, res dto)
        """
        for rule, data in findings.items():
            for region, resources in (data.get('resources') or {}).items():
                for res in resources:
                    yield rule, region, res

    def expose_multiregional(self, it: ResourcesGenerator
                             ) -> ResourcesGenerator:
        """
        This generator yields multiregional region in case the rule is
        multiregional
        :param it:
        :return:
        """
        for rule, region, dto in it:
            if self.is_multiregional(rule):
                _LOG.debug(f'Rule {rule} is multiregional. '
                           f'Yielding multiregional region')
                yield rule, MULTIREGION, dto
            else:
                yield rule, region, dto

    @staticmethod
    def allow_only_regions(it: ResourcesGenerator, regions: Set[str]
                           ) -> Iterable[Tuple[str, str, dict]]:
        return filter(
            lambda item: item[1] in regions, it
        )

    def allow_only_resource_type(self, it: ResourcesGenerator, findings: dict,
                                 resource_type: str
                                 ) -> Iterable[Tuple[str, str, dict]]:
        def _check(item) -> bool:
            rt = self.adjust_resource_type(
                findings.get(item[0], {}).get('resourceType')
            )
            return rt == resource_type
        return filter(_check, it)

    @staticmethod
    def deduplicated(it: ResourcesGenerator,
                     ) -> ResourcesGenerator:
        """
        This generator goes through resources and yields only unique ones
        within rule and region
        :param it:
        :return:
        """
        emitted = {}
        for rule, region, dto in it:
            _emitted = emitted.setdefault((rule, region), set())
            _hashable = hashable(dto)
            if _hashable in _emitted:
                _LOG.debug(f'Duplicate found for {rule}:{region}')
                continue
            yield rule, region, dto
            _emitted.add(_hashable)

    def custom_modify(self, it: ResourcesGenerator,
                      findings: dict
                      ) -> ResourcesGenerator:
        """
        Some resources require special treatment.
        - rules with resource type "aws.cloudtrail" are not multiregional,
        but the resource they look for can be either multiregional or not.
        So we must deduplicate them on the scope of whole account.
        252 and other glue-catalog rules are not multiregional, but they
        also do not return unique information within region.
        :param it:
        :param findings:
        :return:
        """
        # TODO in case we need more business logic here, redesign this
        #  solution. Maybe move this logic to a separate class
        for rule, region, dto in it:
            rt = findings.get(rule).get('resourceType')
            rt = self.adjust_resource_type(rt)
            if rt in ('glue-catalog', 'account'):
                _LOG.debug(f'Rule with type {rt} found. Adding region '
                           f'attribute to make its dto differ from '
                           f'other regions')
                dto[self.custom_attr('region')] = region
                yield rule, region, dto
            elif rt == 'cloudtrail':
                _region = region
                if dto.get('IsMultiRegionTrail'):
                    _LOG.debug('Found multiregional trail. '
                               'Moving it to multiregional region')
                    _region = MULTIREGION
                yield rule, _region, dto
            else:  # no changes required
                yield rule, region, dto

    def keep_report_fields(self, it: ResourcesGenerator) -> ResourcesGenerator:
        """
        Keeps only report fields for each resource. Custom attributes are
        not removed because they are added purposefully
        :param it:
        :return:
        """
        for rule, region, dto in it:
            report_fields = self.mappings_collector.human_data.get(
                rule, {}).get('report_fields') or set()
            filtered = filter_dict(dto, report_fields)
            filtered.update({
                k: v for k, v in dto.items() if self.is_custom_attr(k)
            })
            yield rule, region, filtered

    def create_resources_generator(self, findings: dict, cloud: str,
                                   active_regions: Union[set, list]
                                   ) -> Iterable[Tuple[str, str, dict]]:
        # just iterate over resources
        resources = self.iter_resources(findings)

        # change region to multiregional in case the rule is multiregional
        if cloud != AZURE_CLOUD_ATTR:
            # All the AZURE and GCP rules are technically multiregional.
            # It means that one rule scan all the regions at once whereas
            # one AWS rule must be executed separately for each region
            # (except multiregional). So, after the following generator:
            # - GCP, all the resources become multiregional because GCP meta
            #   contains multiregional=True
            # - AZURE is exception, we distribute resources to regions
            #   manually based on 'location' attribute (this logic is
            #   currently inside executor). So here we don't need to make
            #   any changes with azure regions.
            # - AWS - can contain both global and region-dependent resources.
            #   In case a global rule was scanned on different regions, the
            #   resources it finds will be duplicates. So here we must use
            #   multiregional from meta and perform additional processing
            resources = self.expose_multiregional(resources)

        # modify dto for some exceptional rules, see generator's description
        resources = self.custom_modify(resources, findings)

        # keeping only report fields
        resources = self.keep_report_fields(resources)

        # removing duplicates within rule-region
        resources = self.deduplicated(resources)

        # keeping only active regions and multiregion
        return self.allow_only_regions(resources,
                                       {MULTIREGION, *active_regions})


class CustomerMetricsService(MetricsService):

    @staticmethod
    def get_all_metrics():
        return list(CustomerMetrics.scan())

    @staticmethod
    def create(data) -> CustomerMetrics:
        _id = generate_id()
        return CustomerMetrics(**data, id=_id)

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
                    time_helper.wait(10)
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
        _id = generate_id()
        return TenantMetrics(**data, id=_id)

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
                    time_helper.wait(10)
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
                time_helper.wait(5)
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
