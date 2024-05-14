import uuid
from datetime import datetime
from typing import Optional, Any

from pynamodb.expressions.condition import Condition
from pynamodb.pagination import ResultIterator

from helpers.constants import JobState
from helpers.log_helper import get_logger
from helpers.time_helper import utc_iso
from models.batch_results import BatchResults
from services.base_data_service import BaseDataService

_LOG = get_logger(__name__)


class BatchResultsService(BaseDataService[BatchResults]):
    def create(self, customer_name: str, tenant_name: str,
               rules: dict, cloud_identifier: str) -> BatchResults:
        return super().create(
            id=str(uuid.uuid4()),
            customer_name=customer_name,
            tenant_name=tenant_name,
            rules=rules,
            cloud_identifier=cloud_identifier
        )

    def update(self, job: BatchResults, batch_job_id: str = None,
               reason: str = None,
               status: JobState = None, stopped_at: str = None):
        actions = []
        if batch_job_id:
            actions.append(BatchResults.job_id.set(batch_job_id))
        if reason:
            actions.append(BatchResults.reason.set(reason))
        if status:
            actions.append(BatchResults.status.set(status.value))
        if stopped_at:
            actions.append(BatchResults.stopped_at.set(stopped_at))
        if actions:
            job.update(actions)

    def get_by_customer_name(self, customer_name: str, status: JobState = None,
                             start: datetime = None, end: datetime = None,
                             filter_condition: Optional[Condition] = None,
                             ascending: bool = False, limit: int = None,
                             last_evaluated_key: dict = None,
                             ) -> ResultIterator[BatchResults]:
        if start and end:
            rkc = BatchResults.submitted_at.between(
                lower=utc_iso(start),
                upper=utc_iso(end)
            )
        elif start:
            rkc = (BatchResults.submitted_at >= utc_iso(start))
        elif end:
            rkc = (BatchResults.submitted_at < utc_iso(end))
        else:
            rkc = None
        if status:
            filter_condition &= (BatchResults.status == status.value)
        return BatchResults.customer_name_submitted_at_index.query(
            hash_key=customer_name,
            range_key_condition=rkc,
            filter_condition=filter_condition,
            scan_index_forward=ascending,
            limit=limit,
            last_evaluated_key=last_evaluated_key
        )

    def get_by_tenant_name(self, tenant_name: str, status: JobState = None,
                           start: datetime = None, end: datetime = None,
                           filter_condition: Optional[Condition] = None,
                           ascending: bool = False, limit: int = None,
                           last_evaluated_key: dict = None,
                           ) -> ResultIterator[BatchResults]:
        if start and end:
            rkc = BatchResults.submitted_at.between(
                lower=utc_iso(start),
                upper=utc_iso(end)
            )
        elif start:
            rkc = (BatchResults.submitted_at >= utc_iso(start))
        elif end:
            rkc = (BatchResults.submitted_at < utc_iso(end))
        else:
            rkc = None
        if status:
            filter_condition &= (BatchResults.status == status.value)
        return BatchResults.tenant_name_submitted_at_index.query(
            hash_key=tenant_name,
            range_key_condition=rkc,
            filter_condition=filter_condition,
            scan_index_forward=ascending,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
        )

    def dto(self, item: BatchResults) -> dict[str, Any]:
        raw = super().dto(item)
        raw.pop('rules', None)
        raw.pop('job_id', None)
        raw.pop('credentials_key', None)
        raw.pop('reason', None)
        raw.pop('registration_start', None)
        raw.pop('registration_end', None)
        return raw
