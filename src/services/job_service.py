import datetime

from botocore.exceptions import ClientError

from helpers import time_helper
from models.job import Job
from helpers.constants import JOB_STARTED_STATUS, JOB_RUNNABLE_STATUS, \
    JOB_RUNNING_STATUS, JOB_SUCCEEDED_STATUS, JOB_FAILED_STATUS
from helpers.time_helper import utc_iso
from helpers.log_helper import get_logger
from typing import Iterable, Optional, List, Union, Generator, Callable
from pynamodb.pagination import ResultIterator
from pynamodb.indexes import Condition
from modular_sdk.models.pynamodb_extension.pynamodb_to_pymongo_adapter import \
    Result
from modular_sdk.models.pynamodb_extension.base_model import \
    LastEvaluatedKey as Lek
from services.rbac.restriction_service import RestrictionService

JOB_PENDING_STATUSES = (JOB_STARTED_STATUS, JOB_RUNNABLE_STATUS,
                        JOB_RUNNING_STATUS)
JOB_DTO_SKIP_ATTRS = {'job_definition', 'job_queue', 'reason', 'created_at',
                      'rules_to_scan', 'ttl'}
ISO_TIMESTAMP_ATTRS = ('submitted_at',
                       'stopped_at', 'started_at')
JOB_SUBMITTED_AT = 'submitted_at'
JOB_STARTED_AT = 'started_at'
JOB_STOPPED_AT = 'stopped_at'
JOB_SCAN_RULESETS = 'scan_rulesets'
JOB_ID = 'job_id'
DEFAULT_LIMIT = 30

_LOG = get_logger(__name__)


class JobService:
    def __init__(self, restriction_service: RestrictionService):
        self._restriction_service = restriction_service

    def get_last_tenant_job(self, tenant_name: str,
                            status: Optional[Iterable] = None) -> Optional[Job]:
        """
        Returns the latest job made by tenant
        """
        if isinstance(status, Iterable):
            condition = Job.status.is_in(*list(status))
        return next(
            Job.tenant_display_name_index.query(
                hash_key=tenant_name,
                scan_index_forward=False,
                filter_condition=condition
            ), None
        )

    @staticmethod
    def create(data: dict) -> Job:
        job_data = {}
        for attribute in Job.get_attributes():
            value = data.get(attribute)
            if value is None:
                continue
            job_data[attribute] = value
        return Job(**job_data)

    @staticmethod
    def get_job(job_id: str) -> Optional[Job]:
        return Job.get_nullable(job_id)

    @classmethod
    def inquery(
        cls, customer: Optional[str] = None,
        tenants: Optional[List[str]] = None, limit: Optional[int] = 10,
        last_evaluated_key: Optional[Union[str, dict]] = None,
        attributes_to_get: Optional[list] = None,
        filter_condition: Optional[Condition] = None,
        range_condition: Optional[Condition] = None,
        ascending: bool = False
    ):
        query: Callable = Job.scan
        last_evaluated_key = last_evaluated_key or ''
        if isinstance(last_evaluated_key, str):
            last_evaluated_key = Lek.deserialize(s=last_evaluated_key)

        params: dict = dict(
            filter_condition=filter_condition,
            last_evaluated_key=last_evaluated_key,
            attributes_to_get=attributes_to_get,
            limit=limit
        )

        if any((tenants, customer)):
            params.update(
                range_key_condition=range_condition,
                scan_index_forward=ascending
            )

            if tenants and len(tenants) != 1:

                _params = {}

                # Digests composable lek: Dict[str, Union[int, Dict[str, Any]]]
                _params = cls._get_query_hash_key_ref_params(
                    last_evaluated_key=(last_evaluated_key.value or {}),
                    partition_key_list=tenants, params=params
                )

                _scope = ', '.join(map("'{}'".format, _params))
                _LOG.info(f'Collecting job-items of {_scope} tenants.')
                params = dict(
                    hash_key_query_ref=_params, limit=limit,
                    scan_index_forward=ascending
                )
                query = Job.tenant_display_name_index.batch_query

            elif tenants:
                # len(tenants) == 1
                tenant = tenants[0]

                _LOG.info(f'Collecting job-items of a \'{tenant}\' tenant.')
                params.update(hash_key=tenant)
                query = Job.tenant_display_name_index.query

            elif customer:
                _LOG.info(
                    f'Collecting job-items of a \'{customer}\' customer.')
                params.update(hash_key=customer)
                query = Job.customer_display_name_index.query
        else:
            # Scan
            params.update(limit=limit)

        return query(**params)

    @staticmethod
    def get_succeeded_condition(succeeded: bool):
        # todo query optimization: $status#$submitted-at
        op = Job.status.__eq__ if succeeded else Job.status.__ne__
        return op(JOB_SUCCEEDED_STATUS)

    @staticmethod
    def get_submitted_scope_condition(
        start: Optional[str] = None, end: Optional[str] = None
    ):
        """
        :param start: Optional[str], the lower bound
        :param end: Optional[str], the upper bound
        """
        cdn = None
        if start and end:
            cdn = Job.submitted_at.between(lower=start, upper=end)
        elif start:
            cdn = Job.submitted_at >= start
        elif end:
            cdn = Job.submitted_at <= end
        return cdn

    @staticmethod
    def get_tenant_related_condition(tenant: str):
        return Job.tenant_display_name == tenant

    @staticmethod
    def get_tenants_related_condition(tenants: List[str]):
        return Job.tenant_display_name.is_in(*tenants)

    @staticmethod
    def _get_query_hash_key_ref_params(
        last_evaluated_key: dict, partition_key_list: List[str], params: dict
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

    def list(self, tenants: Optional[list] = None,
             customer: Optional[str] = None,
             succeeded: bool = None,
             attributes_to_get: Optional[list] = None,
             limit: Optional[int] = 10,
             lek: Optional[Union[str, dict]] = None) -> Union[ResultIterator,
                                                              Result]:
        """
        Describes all the jobs based on the given params. Make sure that
        the given tenants are available for the user making the request
        :parameter tenants: Optional[list] - list of tenant names which jobs
            to describe. If the list is empty, all jobs of all the tenants
            within the customer are described. Make sure the param is
            restricted.
        :parameter customer: Optional[str]
        :parameter succeeded: bool
        :parameter attributes_to_get: Optional[list]
        :parameter limit: Optional[int]
        :parameter lek: Optional[Union[str, dict]] - last evaluated key.
            For DynamoDB it's a dict, for MondoDB it's an integer.
        :return: Union[ResultIterator, Result]
        """
        _condition = None
        if isinstance(succeeded, bool):
            _condition &= Job.status == JOB_SUCCEEDED_STATUS

        _c_condition = _condition
        if customer:
            _c_condition &= Job.customer_display_name == customer
        params = dict(scan_index_forward=False, limit=limit,
                      last_evaluated_key=lek,
                      filter_condition=_c_condition,
                      attributes_to_get=attributes_to_get)  # commons
        if tenants and len(tenants) == 1:
            _tenant = tenants[0]
            params.update(hash_key=_tenant)
            _cursor = Job.tenant_display_name_index.query(**params)
        elif customer:  # and maybe multiple tenants
            if tenants:
                _condition &= Job.tenant_display_name.is_in(*tenants)
            params.update(hash_key=customer, filter_condition=_condition)
            _cursor = Job.customer_display_name_index.query(**params)
        else:  # not account & not customer & maybe multiple tenants.
            # Apparently the action is restricted by handler -> there
            # will be no possibility the customer is not given.
            if tenants:
                _c_condition &= Job.tenant_display_name.is_in(*tenants)
            params.pop('scan_index_forward')
            params.update(filter_condition=_c_condition)
            _cursor = Job.scan(**params)
        return _cursor

    def filter_by_tenants(self, entities: Iterable[Job]
                          ) -> Generator[Job, None, None]:
        _tenants = self._restriction_service.user_tenants
        if not _tenants:
            yield from entities
            return
        for entity in entities:
            if entity.tenant_display_name in _tenants:
                yield entity

    @staticmethod
    def jobs_between(jobs: Iterable[Job],
                     start: Optional[datetime.datetime],
                     end: Optional[datetime.datetime]) -> Iterable[Job]:
        start_s = utc_iso(start) if start else None
        end_s = utc_iso(end) if end else None
        for job in jobs:
            start_condition = job.submitted_at >= start_s if start_s else True
            end_condition = job.submitted_at <= end_s if end_s else True
            if start_condition and end_condition:
                yield job

    @staticmethod
    def is_allowed(entity: Job, customer: Optional[str] = None,
                   tenants: Optional[List] = None) -> bool:
        if customer and entity.customer_display_name != customer:
            return False
        if tenants and entity.tenant_display_name not in tenants:
            return False
        return True

    # old services

    @staticmethod
    def get_jobs():
        jobs = []
        response = Job.scan()
        jobs.extend(list(response))
        last_evaluated_key = response.last_evaluated_key

        while last_evaluated_key:
            response = Job.scan(
                last_evaluated_key=last_evaluated_key
            )
            jobs.extend(list(jobs))
            last_evaluated_key = response.last_evaluated_key
        return jobs

    @staticmethod
    def get_customer_jobs(customer_display_name, limit=None, offset=None):
        jobs = []
        if limit and offset:
            limit = limit + offset
        elif not limit and offset:
            limit = DEFAULT_LIMIT + offset
        else:
            offset = 0

        condition = None

        response = Job.customer_display_name_index.query(
            scan_index_forward=False,
            hash_key=customer_display_name,
            limit=limit,
            filter_condition=condition)
        jobs.extend(list(response))
        last_evaluated_key = response.last_evaluated_key

        while last_evaluated_key:
            if limit and len(jobs) >= limit:
                return jobs[offset:]
            try:
                response = Job.customer_display_name_index.query(
                    scan_index_forward=False,
                    last_evaluated_key=last_evaluated_key,
                    hash_key=customer_display_name,
                    limit=limit,
                    filter_condition=condition)
                jobs.extend(list(response))
                last_evaluated_key = response.last_evaluated_key
            except ClientError as e:
                if e.response['Error']['Code'] == \
                        'ProvisionedThroughputExceededException':
                    _LOG.warning('Request rate on CaaSJobs table is too '
                                 'high!')
                    time_helper.wait(5)
                else:
                    raise e

        if limit and len(jobs) >= limit:
            return jobs[offset:]
        return jobs

    @staticmethod
    def get_succeeded_job(job_id):
        job = Job.get_nullable(job_id)
        if job and job.status == JOB_SUCCEEDED_STATUS:
            return job

    @staticmethod
    def get_job_by_tenant(tenant, succeeded_only=True):
        condition = None
        if succeeded_only:
            condition &= Job.status == JOB_SUCCEEDED_STATUS

        jobs = []
        response = Job.tenant_display_name_index.query(
            hash_key=tenant,
            filter_condition=condition)

        jobs.extend(list(response))
        last_evaluated_key = response.last_evaluated_key

        while last_evaluated_key:
            response = Job.tenant_display_name_index.query(
                hash_key=tenant,
                last_evaluated_key=last_evaluated_key,
                filter_condition=condition)
            jobs.extend(list(response))
            last_evaluated_key = response.last_evaluated_key
        return jobs

    @staticmethod
    def get_tenant_job_by_date(tenant, date_timestamp, succeeded_only=True):
        # TODO refactor
        target_datetime = datetime.datetime.fromtimestamp(
            date_timestamp / 1000)

        timestamp_from = datetime.datetime.timestamp(
            target_datetime.replace(hour=0, minute=0, second=0, microsecond=0))
        timestamp_from = int(timestamp_from * 1000)

        timestamp_to = datetime.datetime.timestamp(
            target_datetime.replace(hour=0, minute=0, second=0,
                                    microsecond=0) + datetime.timedelta(
                days=1))
        timestamp_to = int(timestamp_to * 1000)

        if succeeded_only:
            return list(Job.tenant_display_name_index.query(
                hash_key=tenant,
                filter_condition=(Job.submitted_at >= timestamp_from) & (
                        Job.submitted_at < timestamp_to) & (
                                         Job.status == JOB_SUCCEEDED_STATUS)))
        return list(Job.tenant_display_name_index.query(
            hash_key=tenant,
            filter_condition=(Job.submitted_at >= timestamp_from) & (
                    Job.submitted_at < timestamp_to)))

    @staticmethod
    def get_latest_tenant_job(tenant, succeeded_only=True):
        if succeeded_only:
            tenant_jobs = list(Job.tenant_display_name_index.query(
                hash_key=tenant,
                scan_index_forward=True,
                limit=1,
                filter_condition=Job.status == JOB_SUCCEEDED_STATUS))
        else:
            tenant_jobs = list(Job.tenant_display_name_index.query(
                hash_key=tenant,
                scan_index_forward=True,
                limit=1))
        if tenant_jobs:
            return tenant_jobs[0]

    @staticmethod
    def save(job: Job):
        job.save()

    def filter_jobs(self, job_id=None, tenant=None, date=None,
                    latest=None, succeeded_only=True,
                    customer=None, event_driven=None, limit=10,
                    offset=0,
                    **kwargs):
        # TODO refactor this method completely because if you pass,
        #  for instance, customer and account, and date here,
        #  you expect to receive jobs filtered by all these params together.
        #  + it must use indexes
        if job_id:
            if succeeded_only:
                jobs = [self.get_succeeded_job(job_id=job_id)]
            else:
                jobs = [self.get_job(job_id=job_id)]
            if customer:
                jobs = [jobs[0], ] if jobs[0].customer_display_name == customer else []
        elif tenant:
            if latest:
                jobs = [self.get_latest_tenant_job(
                    tenant=tenant,
                    succeeded_only=succeeded_only)]
            elif date:
                jobs = self.get_tenant_job_by_date(
                    tenant=tenant,
                    date_timestamp=date,
                    succeeded_only=succeeded_only)
            elif customer:
                jobs = self.filter_jobs(
                    tenant=tenant, customer=customer,
                )
            else:
                jobs = self.get_job_by_tenant(
                    tenant=tenant,
                    succeeded_only=succeeded_only)
        elif customer:
            jobs = self.get_customer_jobs(
                customer_display_name=customer, limit=limit, offset=offset)
        else:
            jobs = self.get_jobs()

        jobs = [job for job in jobs if job]
        if event_driven is not None:
            jobs = [job for job in jobs if
                    (getattr(job, 'is_event_driven') or False) == event_driven]

        if len(jobs) > 1:
            jobs.sort(
                key=lambda item: item.submitted_at,
                reverse=True)
        if len(jobs) >= limit + offset:
            return jobs[offset:limit + offset]
        return jobs[-limit:]

    @staticmethod
    def get_job_dto(job: Job, params_to_exclude: set = None):
        params_to_exclude = params_to_exclude or set()
        job_data = job.get_json()
        job_dto = {}
        params_to_exclude |= JOB_DTO_SKIP_ATTRS
        for attr_name, attr_value in job_data.items():
            if attr_name in params_to_exclude:
                continue
            else:
                job_dto[attr_name] = attr_value
        return job_dto

    @staticmethod
    def get_tenant_jobs_between_period(tenant: str, start_period=None,
                                       end_period=None,
                                       only_succeeded: bool = True,
                                       limit: int = None,
                                       attributes_to_get: list = None):
        conditions = None
        range_key_condition = None
        if only_succeeded:
            conditions = (Job.status == JOB_SUCCEEDED_STATUS)
        if start_period and end_period:
            range_key_condition &= (
                Job.submitted_at.between(start_period.isoformat(),
                                         end_period.isoformat()))
        elif end_period:
            range_key_condition &= (Job.submitted_at <=
                                    end_period.isoformat())
        elif start_period:
            range_key_condition &= (Job.submitted_at >=
                                    start_period.isoformat())
        _cursor = Job.tenant_display_name_index.query(
            hash_key=tenant, range_key_condition=range_key_condition,
            filter_condition=conditions, limit=limit,
            attributes_to_get=attributes_to_get)
        items = list(_cursor)
        last_evaluated_key = _cursor.last_evaluated_key
        while last_evaluated_key:
            try:
                _cursor = Job.tenant_display_name_index.query(
                    hash_key=tenant,
                    last_evaluated_key=last_evaluated_key,
                    range_key_condition=range_key_condition,
                    filter_condition=conditions, limit=limit,
                    attributes_to_get=attributes_to_get
                )
                items.extend(list(_cursor))
                last_evaluated_key = _cursor.last_evaluated_key
            except ClientError as e:
                if e.response['Error']['Code'] == \
                        'ProvisionedThroughputExceededException':
                    _LOG.warning('Request rate on CaaSJobs table is too high!')
                    time_helper.wait(5)
                else:
                    raise e
        return items

    @staticmethod
    def get_customer_jobs_between_period(
            start_period=None, end_period=None, customer=None,
            tenant=None, only_succeeded: bool = True, limit: int = None,
            attributes_to_get: list = None):
        conditions = None
        range_key_condition = None
        if only_succeeded:
            conditions = (Job.status == JOB_SUCCEEDED_STATUS)
        if tenant:
            conditions &= (Job.tenant_display_name == tenant)
        if customer:
            if start_period and end_period:
                range_key_condition &= (
                    Job.submitted_at.between(start_period.isoformat(),
                                             end_period.isoformat()))
            elif end_period:
                range_key_condition &= (
                            Job.submitted_at <= end_period.isoformat())
            elif start_period:
                range_key_condition &= (
                            Job.submitted_at >= start_period.isoformat())
            _cursor = Job.customer_display_name_index.query(
                hash_key=customer, range_key_condition=range_key_condition,
                filter_condition=conditions, limit=limit,
                attributes_to_get=attributes_to_get)
            items = list(_cursor)
            last_evaluated_key = _cursor.last_evaluated_key
            while last_evaluated_key:
                try:
                    _cursor = Job.customer_display_name_index.query(
                        hash_key=customer,
                        last_evaluated_key=last_evaluated_key,
                        range_key_condition=range_key_condition,
                        filter_condition=conditions, limit=limit,
                        attributes_to_get=attributes_to_get
                    )
                    items.extend(list(_cursor))
                    last_evaluated_key = _cursor.last_evaluated_key
                except ClientError as e:
                    if e.response['Error']['Code'] == \
                            'ProvisionedThroughputExceededException':
                        _LOG.warning(
                            'Request rate on CaaSJobs table is too '
                            'high!')
                        time_helper.wait(5)
                    else:
                        raise e
        else:
            _cursor = Job.scan(filter_condition=conditions, limit=limit,
                               attributes_to_get=attributes_to_get)
            items = list(_cursor)
            last_evaluated_key = _cursor.last_evaluated_key
            while last_evaluated_key:
                try:
                    _cursor = Job.scan(filter_condition=conditions,
                                       limit=limit,
                                       attributes_to_get=attributes_to_get,
                                       last_evaluated_key=last_evaluated_key)
                    items.extend(list(_cursor))
                    last_evaluated_key = _cursor.last_evaluated_key
                except ClientError as e:
                    if e.response['Error']['Code'] == \
                            'ProvisionedThroughputExceededException':
                        _LOG.warning(
                            'Request rate on CaaSJobs table is too '
                            'high!')
                        time_helper.wait(5)
                    else:
                        raise e
        return items

    @staticmethod
    def set_job_failed_status(job, reason='Terminating job.'):
        job.status = JOB_FAILED_STATUS
        job.stopped_at = utc_iso()
        job.reason = reason
