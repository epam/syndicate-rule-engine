import uuid
from typing import Optional, List

from helpers import get_logger
from models.job_statistics import JobStatistics

_LOG = get_logger(__name__)


class JobStatisticsService:

    @staticmethod
    def save(data: dict):
        result_data = {}
        for attribute in JobStatistics.get_attributes():
            value = data.get(attribute)
            if value is None:
                continue
            result_data[attribute] = value
        if not result_data.get('id'):
            result_data['id'] = str(uuid.uuid4())
        JobStatistics(**result_data).save()

    @staticmethod
    def get(item_id: str) -> Optional[JobStatistics]:
        return JobStatistics.get_nullable(item_id)

    @staticmethod
    def list(limit: bool = 50) -> list:
        items = []
        response = JobStatistics.scan(limit=limit)
        items.extend(list(response))
        last_evaluated_key = response.last_evaluated_key

        while last_evaluated_key:
            response = JobStatistics.scan(
                last_evaluated_key=last_evaluated_key,
                limit=limit
            )
            items.extend(list(items))
            last_evaluated_key = response.last_evaluated_key
        return items

    @staticmethod
    def batch_save(entity_list: List[JobStatistics]):
        with JobStatistics.batch_write() as writer:
            for item in entity_list:
                writer.save(item)

    @staticmethod
    def get_by_customer_and_date(customer: str, from_date, to_date=None,
                                 limit=None) -> list:
        # [cry] pagination is automatic
        filter_condition = None
        range_key_condition = None

        range_key_condition &= JobStatistics.from_date >= from_date
        if to_date:
            filter_condition &= JobStatistics.to_date <= to_date

        _cursor = JobStatistics.customer_name_from_date_index.query(
            hash_key=customer, range_key_condition=range_key_condition,
            limit=limit, filter_condition=filter_condition
        )
        items = list(_cursor)
        last_evaluated_key = _cursor.last_evaluated_key
        while last_evaluated_key:
            _cursor = JobStatistics.customer_name_from_date_index.query(
                hash_key=customer,
                limit=limit, filter_condition=filter_condition,
                last_evaluated_key=last_evaluated_key,
                range_key_condition=range_key_condition
            )
            items.append(list(_cursor))
            last_evaluated_key = _cursor.last_evaluated_key
        return items
