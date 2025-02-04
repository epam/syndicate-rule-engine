import heapq
import operator
from datetime import datetime
from functools import cached_property
from typing import Callable, Iterable, Iterator, Optional, Union

from helpers.constants import JobState, JobType
from services.batch_results_service import BatchResults, BatchResultsService
from services.job_service import Job, JobService

Source = Job | BatchResults


class AmbiguousJob:
    """
    Resembles our base job interface
    """

    __slots__ = ('job',)

    def __init__(self, job: Source):
        self.job: Source = job

    def __repr__(self) -> str:
        return f'{self.class_.__name__}<{self.id}>'

    def __hash__(self):
        return self.job.__hash__()

    def __eq__(self, other: Union['AmbiguousJob', Job | BatchResults]) -> bool:
        if type(other) is AmbiguousJob:
            return self.job == other.job
        return type(other) is self.class_ and self.job == other

    @property
    def class_(self) -> type[Source]:
        return type(self.job)

    @property
    def type(self) -> JobType:
        if self.is_ed_job:
            return JobType.REACTIVE
        return JobType.MANUAL

    @property
    def is_ed_job(self) -> bool:
        return self.class_ is BatchResults

    @property
    def is_platform_job(self) -> bool:
        return not self.is_ed_job and bool(self.platform_id)

    @property
    def id(self) -> str:
        return self.job.id

    @property
    def batch_job_id(self) -> str:
        if self.is_ed_job:
            return self.job.job_id
        return self.batch_job_id

    @property
    def tenant_name(self) -> str:
        return self.job.tenant_name

    @property
    def customer_name(self) -> str:
        return self.job.customer_name

    @property
    def status(self) -> JobState | None:
        raw = self.job.status
        if not raw:
            return
        return JobState[raw]

    @property
    def reason(self) -> str | None:
        return self.job.reason

    @property
    def submitted_at(self) -> str:
        return self.job.submitted_at

    @property
    def stopped_at(self) -> str | None:
        return self.job.stopped_at

    @property
    def is_succeeded(self) -> bool:
        return self.status == JobState.SUCCEEDED.value

    @property
    def is_failed(self) -> bool:
        return self.status == JobState.FAILED.value

    def __getattr__(self, item):
        """
        All the other attributes can be accessed directly
        """
        return getattr(self.job, item, None)

    def is_finished(self) -> bool:
        return bool(self.stopped_at) and self.status in (
            JobState.SUCCEEDED,
            JobState.FAILED,
        )


class AmbiguousJobService:
    def __init__(
        self,
        job_service: JobService,
        batch_results_service: BatchResultsService,
    ):
        self._manual_source_service = job_service
        self._reactive_source_service = batch_results_service

    @property
    def job_service(self) -> JobService:
        return self._manual_source_service

    @property
    def batch_results_service(self) -> BatchResultsService:
        return self._reactive_source_service

    @cached_property
    def typ_job_getter_ref(self) -> dict[JobType, Callable]:
        return {
            JobType.MANUAL: self._manual_source_service.get_nullable,
            JobType.REACTIVE: self._reactive_source_service.get_nullable,
        }

    def get(self, uid: str, typ: Optional[JobType] = None) -> Optional[Source]:
        ref = self.typ_job_getter_ref
        if typ:
            assert typ in ref, 'Invalid type provided'
            return ref[typ](uid)
        source = filter(lambda x: x, (get(uid) for get in ref.values()))
        return next(source, None)

    def get_job(
        self,
        job_id: str,
        typ: Optional[JobType] = None,
        tenant: Optional[str] = None,
        customer: Optional[str] = None,
    ) -> AmbiguousJob | None:
        item = self.get(job_id, typ)
        if not item:
            return
        item = AmbiguousJob(item)
        if tenant and item.tenant_name != tenant:
            return
        if customer and item.customer_name != customer:
            return
        return item

    @staticmethod
    def merged(
        jobs: Iterator[Job],
        brs: Iterator[BatchResults],
        ascending: bool = False,
    ) -> Iterable[Source]:
        key = operator.attrgetter('submitted_at')
        return heapq.merge(jobs, brs, key=key, reverse=not ascending)

    def get_by_customer_name(
        self,
        customer_name: str,
        job_type: JobType | None = None,
        status: JobState | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        ascending: bool = False,
        limit: int | None = None,
    ) -> Iterable[Source]:
        cursor1 = self.job_service.get_by_customer_name(
            customer_name=customer_name,
            status=status,
            start=start,
            end=end,
            ascending=ascending,
            limit=limit,
        )
        cursor2 = self.batch_results_service.get_by_customer_name(
            customer_name=customer_name,
            status=status,
            start=start,
            end=end,
            ascending=ascending,
            limit=limit,
        )
        match job_type:
            case JobType.MANUAL:
                return cursor1
            case JobType.REACTIVE:
                return cursor2
            case _:
                return self.merged(cursor1, cursor2, ascending)

    def get_by_tenant_name(
        self,
        tenant_name: str,
        job_type: JobType | None = None,
        status: JobState | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        ascending: bool = False,
        limit: int | None = None,
    ) -> Iterable[Source]:
        cursor1 = self.job_service.get_by_tenant_name(
            tenant_name=tenant_name,
            status=status,
            start=start,
            end=end,
            ascending=ascending,
            limit=limit,
        )
        cursor2 = self.batch_results_service.get_by_tenant_name(
            tenant_name=tenant_name,
            status=status,
            start=start,
            end=end,
            ascending=ascending,
            limit=limit,
        )
        match job_type:
            case JobType.MANUAL:
                return cursor1
            case JobType.REACTIVE:
                return cursor2
            case _:
                return self.merged(cursor1, cursor2, ascending)

    @staticmethod
    def to_ambiguous(it: Iterable[Source]) -> Iterator[AmbiguousJob]:
        return map(AmbiguousJob, it)

    def dto(self, job: Source) -> dict:
        if isinstance(job, BatchResults):
            return self.batch_results_service.dto(job)
        return self.job_service.dto(job)
