from typing import List, Union

from botocore.exceptions import ClientError
from pynamodb.exceptions import QueryError

from helpers import get_logger, generate_id, time_helper
from models.customer_metrics import CustomerMetrics
from models.tenant_metrics import TenantMetrics

_LOG = get_logger(__name__)


class MetricsService:
    def batch_save(self, metrics: List[Union[CustomerMetrics, TenantMetrics]],
                   limit: int = 30,
                   retry_delay: int = 5):
        sub_metrics = [metrics[i:i + limit]
                       for i in range(0, len(metrics), limit)]
        for _list in sub_metrics:
            while True:
                try:
                    self._batch_write(_list)
                    break
                except ClientError as e:
                    if e.response['Error']['Code'] == \
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
            except ClientError as e:
                if e.response['Error']['Code'] == \
                        'ProvisionedThroughputExceededException':
                    _LOG.warning('Request rate on CaaSCustomerMetrics table '
                                 'is too high!')
                    time_helper.wait(5)
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
            except ClientError as e:
                if e.response['Error']['Code'] == \
                        'ProvisionedThroughputExceededException':
                    _LOG.warning('Request rate on CaaSJobs table is too '
                                 'high!')
                    time_helper.wait(5)
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
