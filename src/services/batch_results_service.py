import uuid
from datetime import datetime
from typing import Optional, List, Union

from pynamodb.exceptions import QueryError
from pynamodb.expressions.condition import Condition

from helpers import time_helper
from helpers.constants import JOB_SUCCEEDED_STATUS
from models.batch_results import BatchResults
from modular_sdk.models.pynamodb_extension.base_model import LastEvaluatedKey as Lek
from helpers.time_helper import ts_datetime, utc_iso

from helpers.log_helper import get_logger

_LOG = get_logger(__name__)

ATTRS_TO_CONCEAL = ('rules', 'bucket_name', 'bucket_path', 'rulesets', 'credentials_key')
SUCCEEDED_STATUS = 'SUCCEEDED'
REG_START_ATTR = 'registration_start'
REG_END_ATTR = 'registration_end'


class BatchResultsService:

    @staticmethod
    def create(data: dict) -> BatchResults:
        results_data = {}
        for attribute in BatchResults.get_attributes():
            value = data.get(attribute)
            if value is None:
                continue
            results_data[attribute] = value
        if not results_data.get('id'):
            results_data['id'] = str(uuid.uuid4())
        return BatchResults(**results_data)

    @staticmethod
    def get(batch_results: str) -> Optional[BatchResults]:
        return BatchResults.get_nullable(batch_results)

    @staticmethod
    def list() -> list:
        batch_results = []
        response = BatchResults.scan()
        batch_results.extend(list(response))
        last_evaluated_key = response.last_evaluated_key

        while last_evaluated_key:
            response = BatchResults.scan(
                last_evaluated_key=last_evaluated_key
            )
            batch_results.extend(list(batch_results))
            last_evaluated_key = response.last_evaluated_key
        return batch_results

    @staticmethod
    def batch_save(entity_list: List[BatchResults]):
        with BatchResults.batch_write() as writer:
            for batch_result in entity_list:
                writer.save(batch_result)

    @classmethod
    def get_between_period_by_tenant(
            cls, tenant_name: str, start: str = None, end: str = None,
            ascending: bool = False
    ) -> list:
        range_key_condition = cls.get_registered_scope_condition(
            start=start, end=end
        )
        _cursor = BatchResults.tn_rs_index.query(
            hash_key=tenant_name, range_key_condition=range_key_condition,
            scan_index_forward=ascending
        )
        items = list(_cursor)
        last_evaluated_key = _cursor.last_evaluated_key
        while last_evaluated_key:
            _cursor = BatchResults.tn_rs_index.query(
                hash_key=tenant_name,
                last_evaluated_key=last_evaluated_key,
                range_key_condition=range_key_condition,
                scan_index_forward=ascending
            )
            items.append(list(_cursor))
            last_evaluated_key = _cursor.last_evaluated_key
        return items

    @classmethod
    def get_between_period_by_customer(
            cls, customer_name: str, tenants: list = None, start: str = None,
            end: str = None, ascending: bool = False, limit: int = None,
            only_succeeded: bool = True, attributes_to_get: list = None) -> \
            List[BatchResults]:
        filter_condition = None
        range_key_condition = None
        if start and end:
            range_key_condition = BatchResults.stopped_at.between(
                lower=start, upper=end
            )
        elif start:
            range_key_condition = BatchResults.stopped_at >= start
        elif end:
            range_key_condition = BatchResults.stopped_at <= end

        if tenants:
            filter_condition &= BatchResults.tenant_name.is_in(*tenants)
        if only_succeeded:
            filter_condition &= BatchResults.status == SUCCEEDED_STATUS
        _cursor = BatchResults.cn_jsta_index.query(
            hash_key=customer_name, range_key_condition=range_key_condition,
            scan_index_forward=ascending, attributes_to_get=attributes_to_get,
            filter_condition=filter_condition, limit=limit
        )
        items = list(_cursor)
        last_evaluated_key = _cursor.last_evaluated_key
        while last_evaluated_key:
            try:
                _cursor = BatchResults.cn_jsta_index.query(
                    hash_key=customer_name,
                    last_evaluated_key=last_evaluated_key,
                    range_key_condition=range_key_condition,
                    scan_index_forward=ascending, limit=limit,
                    attributes_to_get=attributes_to_get
                )
                items.extend(list(_cursor))
                last_evaluated_key = _cursor.last_evaluated_key
            except QueryError as e:
                if e.cause_response_code == \
                        'ProvisionedThroughputExceededException':
                    _LOG.warning('Request rate on BatchResults table is too '
                                 'high!')
                    time_helper.wait(1)
                else:
                    raise e

        return items

    @classmethod
    def get_between_period(cls, start: datetime = None, end: datetime = None,
                           attributes_to_get: list = None) -> list:
        filter_condition = None
        if start:
            filter_condition &= \
                (BatchResults.submitted_at >= start.isoformat())
        if end:
            filter_condition &= \
                (BatchResults.submitted_at <= end.isoformat())

        _cursor = BatchResults.scan(filter_condition=filter_condition,
                                    attributes_to_get=attributes_to_get)
        items = list(_cursor)
        last_evaluated_key = _cursor.last_evaluated_key
        while last_evaluated_key:
            _cursor = BatchResults.scan(filter_condition=filter_condition,
                                        attributes_to_get=attributes_to_get,
                                        last_evaluated_key=last_evaluated_key)
            items.extend(list(_cursor))
            last_evaluated_key = _cursor.last_evaluated_key
        return items

    @classmethod
    def get_latest_by_account(cls, cloud_id: str,
                              succeeded_only: bool = True) -> list:
        filter_condition = None
        if succeeded_only:
            filter_condition &= BatchResults.status == JOB_SUCCEEDED_STATUS
        results = list(BatchResults.cid_rs_index.query(
                hash_key=cloud_id,
                scan_index_forward=True,
                limit=1,
                filter_condition=filter_condition))
        if results:
            return results[0]

    @classmethod
    def inquery(
            cls,
            customer: Optional[str] = None,
            tenants: Optional[List[str]] = None,
            cloud_ids: Optional[List[str]] = None,
            range_condition: Optional[Condition] = None,
            filter_condition: Optional[Condition] = None,
            ascending: Optional[bool] = False,
            last_evaluated_key: Optional[Union[Lek, str]] = None,
            limit: Optional[str] = None,
            attributes_to_get: Optional[List[str]] = None
    ):
        # cloud_ids now are excessive here, it's obsolete logic,
        # but it's too scary to change something here..

        action = BatchResults.scan

        last_evaluated_key = last_evaluated_key or ''
        if isinstance(last_evaluated_key, str):
            last_evaluated_key = Lek.deserialize(s=last_evaluated_key)

        params = dict(
            filter_condition=filter_condition,
            last_evaluated_key=last_evaluated_key,
            attributes_to_get=attributes_to_get,
            limit=limit
        )

        if any((cloud_ids, tenants, customer)):
            params.update(
                range_key_condition=range_condition,
                scan_index_forward=ascending
            )

            if cloud_ids and len(cloud_ids) != 1:
                # Digests composable lek: Dict[str, Union[int, Dict[str, Any]]]
                _params = cls._get_query_hash_key_ref_params(
                    last_evaluated_key=(last_evaluated_key.value or {}),
                    partition_key_list=cloud_ids, params=params
                )

                _scope = ', '.join(map("'{}'".format, _params))
                _LOG.info(f'Collecting items of {_scope} account(s).')
                params = dict(
                    hash_key_query_ref=_params, limit=limit,
                    scan_index_forward=ascending
                )
                action = BatchResults.cid_rs_index.batch_query

            elif cloud_ids:
                # len(cloud_ids) == 1.
                cloud_id = cloud_ids[0]
                _LOG.info(f'Collecting items of a \'{cloud_id}\' account.')
                params.update(hash_key=cloud_id)
                action = BatchResults.cid_rs_index.query

            elif tenants and len(tenants) != 1:
                # Digests composable lek: Dict[str, Union[int, Dict[str, Any]]]
                _params = cls._get_query_hash_key_ref_params(
                    last_evaluated_key=(last_evaluated_key.value or {}),
                    partition_key_list=tenants, params=params
                )
                _scope = ', '.join(map("'{}'".format, _params))
                _LOG.info(f'Collecting items of {_scope} tenant(s).')
                params = dict(
                    hash_key_query_ref=_params, limit=limit,
                    scan_index_forward=ascending
                )
                action = BatchResults.tn_rs_index.batch_query

            elif tenants:
                # len(tenants) == 1
                tenant = tenants[0]
                _LOG.info(f'Collecting items of a \'{tenant}\' tenant.')
                params.update(hash_key=tenant)
                action = BatchResults.tn_rs_index.query

            elif customer:
                _LOG.info(f'Collecting items of a \'{customer}\' customer.')
                params.update(hash_key=customer)
                action = BatchResults.cn_rs_index.query
        else:
            # Scan
            params.update(limit=limit)

        return action(**params)

    @staticmethod
    def timerange_captured_between(since: str, until: str):
        return BatchResults.registration_start.between(
            lower=since, upper=until
        )

    @staticmethod
    def get_registered_scope_condition(
            start: Optional[str] = None, end: Optional[str] = None
    ):
        """
        :param start: Optional[str], the lower bound
        :param end: Optional[str], the upper bound
        """
        cdn = None
        if start and end:
            cdn = BatchResults.registration_start.between(
                lower=start, upper=end
            )
        elif start:
            cdn = BatchResults.registration_start >= start
        elif end:
            cdn = BatchResults.registration_start <= end
        return cdn

    @staticmethod
    def get_succeeded_condition(succeeded: bool):
        # todo query optimization: $status#$submitted-at
        _status = BatchResults.status
        op = _status.__eq__ if succeeded else _status.__ne__
        return op(SUCCEEDED_STATUS)

    @staticmethod
    def dto(entity: BatchResults):
        data = {
            k: v for k, v in entity.get_json().items()
            if k not in ATTRS_TO_CONCEAL
        }
        attrs = (REG_START_ATTR, REG_END_ATTR)
        for each in attrs:
            value = data.get(each)
            if value:
                data[each] = utc_iso(ts_datetime(float(value)))
        if data.get('status') == SUCCEEDED_STATUS:
            data.pop('job_id', None)
        return data

    @staticmethod
    def _get_query_hash_key_ref_params(
            last_evaluated_key: dict, partition_key_list: List[str],
            params: dict
    ):
        """
        Returns `hash_key_ref` payload, digesting a last_evaluated_key,
        presumably composed out of partition key pointers, reference to
        which is stored within the respective list.
        """
        output = {}
        last_evaluated_key = last_evaluated_key or {}
        for partition_key in partition_key_list:
            _output = params.copy()
            if partition_key in last_evaluated_key:
                _output.update(last_evaluated_key=last_evaluated_key)
            output[partition_key] = _output
        return output
