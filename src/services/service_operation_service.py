from datetime import date, datetime
from typing import Iterator

from modular_sdk.models.job import Job
from pynamodb.expressions.condition import Condition

from helpers.constants import ServiceOperationType
from helpers.time_helper import utc_iso


class ServiceOperationService:
    """
    Service for managing service operation status tracking.
    """
    
    @staticmethod
    def get_by_type(
        service_operation_type: ServiceOperationType | str,
        start: datetime | date | None = None,
        end: datetime | date | None = None,
        limit: int | None = None,
        ascending: bool = False,
    ) -> Iterator[Job]:
        """
        Query service operations by type with optional date range filtering.
        
        :param service_operation_type: Type of service operation to query
        :param start: Start date/datetime for filtering (inclusive)
        :param end: End date/datetime for filtering (exclusive for upper bound)
        :param limit: Maximum number of results to return
        :param ascending: If True, return oldest first; if False, return newest first
        :return: Iterator of Job objects
        """
        if isinstance(service_operation_type, ServiceOperationType):
            service_operation_type = service_operation_type.value
            
        rkc = ServiceOperationService._build_range_key_condition(start, end)
        
        return Job.job_started_at_index.query(
            hash_key=service_operation_type,
            range_key_condition=rkc,
            limit=limit,
            scan_index_forward=ascending,
        )
    
    @staticmethod
    def get_latest_by_type(
        service_operation_type: ServiceOperationType | str,
    ) -> Job | None:
        """
        Get the most recent service operation of a specific type.
        
        :param service_operation_type: Type of service operation to query
        :return: The latest Job object or None if not found
        """
        jobs = ServiceOperationService.get_by_type(
            service_operation_type=service_operation_type,
            limit=1,
            ascending=False,
        )
        return next(jobs, None)

    @staticmethod
    def to_dto(job: Job) -> dict:
        """
        Convert a Job object to a DTO dictionary.
        
        :param job: Job object to convert
        :return: Dictionary with started_at and state
        """
        started_at = job.started_at
        if isinstance(started_at, datetime | date):
            started_at = utc_iso(started_at)
        return {
            'started_at': started_at,
            'state': job.state,
        }
    
    @staticmethod
    def _build_range_key_condition(
        start: datetime | date | None,
        end: datetime | date | None,
    ) -> Condition | None:
        """
        Build a range key condition for date filtering.
        
        :param start: Start date/datetime (inclusive)
        :param end: End date/datetime (exclusive)
        :return: PynamoDB Condition or None
        """
        if start and end:
            return Job.started_at.between(
                lower=utc_iso(start),
                upper=utc_iso(end),
            )
        elif start:
            return Job.started_at >= utc_iso(start)
        elif end:
            return Job.started_at < utc_iso(end)
        return None

