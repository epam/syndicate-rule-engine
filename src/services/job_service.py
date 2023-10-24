import datetime
import time
from abc import ABC, abstractmethod
from typing import Iterable, Optional, List, Union, Generator, Callable

from botocore.exceptions import ClientError
from modular_sdk.models.pynamodb_extension.base_model import \
    LastEvaluatedKey as Lek
from modular_sdk.models.pynamodb_extension.pynamodb_to_pymongo_adapter import \
    Result
from pymongo.collection import Collection
from modular_sdk.models.tenant_settings import TenantSettings
from modular_sdk.services.tenant_settings_service import TenantSettingsService
from pynamodb.indexes import Condition
from pynamodb.pagination import ResultIterator
from pynamodb.exceptions import UpdateError
from helpers import time_helper
from helpers.constants import JOB_STARTED_STATUS, JOB_RUNNABLE_STATUS, \
    JOB_RUNNING_STATUS, JOB_SUCCEEDED_STATUS, JOB_FAILED_STATUS
from helpers.log_helper import get_logger
from helpers.time_helper import utc_iso
from models.job import Job
from services import SP
from services.rbac.restriction_service import RestrictionService

JOB_PENDING_STATUSES = (JOB_STARTED_STATUS, JOB_RUNNABLE_STATUS,
                        JOB_RUNNING_STATUS)
JOB_DTO_SKIP_ATTRS = {'job_definition', 'job_queue', 'reason', 'created_at',
                      'rules_to_scan', 'ttl'}
DEFAULT_LIMIT = 30

_LOG = get_logger(__name__)


class AbstractJobLock(ABC):

    @abstractmethod
    def acquire(self, *args, **kwargs):
        pass

    @abstractmethod
    def release(self):
        pass

    @abstractmethod
    def locked(self) -> bool:
        pass


class TenantSettingJobLock(AbstractJobLock):
    TYPE = 'CUSTODIAN_JOB_LOCK'  # tenant_setting type
    EXPIRATION = 3600 * 1.5  # in seconds, 1.5h

    def __init__(self, tenant_name: str):
        """
        >>> lock = TenantSettingJobLock('MY_TENANT')
        >>> lock.locked()
        False
        >>> lock.acquire('job-1')
        >>> lock.locked()
        True
        >>> lock.job_id
        'job-1'
        >>> lock.release()
        >>> lock.locked()
        False
        >>> lock.release()
        >>> lock.locked()
        False
        :param tenant_name:
        """
        self._tenant_name = tenant_name

        self._item = None  # just cache

    @property
    def tss(self) -> TenantSettingsService:
        """
        Tenant settings service
        :return:
        """
        return SP.modular_service().modular_client.tenant_settings_service()

    @property
    def job_id(self) -> Optional[str]:
        """
        ID of a job the lock is locked with
        :return:
        """
        if not self._item:
            return
        return self._item.value.as_dict().get('jid')

    @property
    def tenant_name(self) -> str:
        return self._tenant_name

    def acquire(self, job_id: str):
        """
        You must check whether the lock is locked before calling acquire().
        :param job_id:
        :return:
        """
        item = self.tss.create(
            tenant_name=self._tenant_name,
            key=self.TYPE
        )
        self.tss.update(item, actions=[
            TenantSettings.value.set({
                'exp': time.time() + self.EXPIRATION,
                'jid': job_id,
                'locked': True
            })
        ])
        self._item = item

    def release(self):
        item = self.tss.create(
            tenant_name=self._tenant_name,
            key=self.TYPE
        )
        try:
            self.tss.update(item, actions=[
                TenantSettings.value['locked'].set(False)
            ])
        except UpdateError:
            # it's normal. It means that item.value['locked'] simply
            # does not exist and update action cannot perform its update.
            # DynamoDB raises UpdateError if you try to update not existing
            # nested key
            pass
        self._item = item

    def locked(self) -> bool:
        item = self.tss.get(self._tenant_name, self.TYPE)
        if not item:
            return False
        self._item = item
        value = item.value.as_dict()
        if not value.get('locked'):
            return False
        # locked = True
        if not value.get('exp'):
            return True  # no expiration, we locked
        return value.get('exp') > time.time()


class JobService:
    def __init__(self, restriction_service: RestrictionService):
        self._restriction_service = restriction_service

    def get_last_tenant_job(self, tenant_name: str,
                            status: Optional[Iterable] = None
                            ) -> Optional[Job]:
        """
        Returns the latest job made by tenant
        """
        condition = None
        if isinstance(status, Iterable):
            condition = Job.status.is_in(*list(status))
        return next(
            Job.tenant_display_name_index.query(
                hash_key=tenant_name,
                scan_index_forward=False,
                limit=1,
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
    def get_submitted_scope_condition(start: Optional[str] = None,
                                      end: Optional[str] = None):
        """
        :param start: Optional[str], the lower bound
        :param end: Optional[str], the upper bound
        """
        if start and end:
            return Job.submitted_at.between(lower=start, upper=end)
        elif start:
            return Job.submitted_at >= start
        else:  # only end
            return Job.submitted_at <= end

    @staticmethod
    def get_tenant_related_condition(tenant: str):
        return Job.tenant_display_name == tenant

    @staticmethod
    def get_tenants_related_condition(tenants: List[str]):
        return Job.tenant_display_name.is_in(*tenants)

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
    def save(job: Job):
        job.save()

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
