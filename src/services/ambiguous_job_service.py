from services.job_service import JobService, Job
from services.batch_results_service import BatchResultsService, BatchResults
from models.pynamodb_extension.index import IResultIterator
from helpers.log_helper import get_logger
from helpers.time_helper import utc_datetime, ts_datetime, datetime
from helpers.constants import TENANT_ATTR, TENANT_DISPLAY_NAME_ATTR, \
    TENANT_NAME_ATTR, ID_ATTR, CUSTOMER_ATTR, CUSTOMER_NAME_ATTR, \
    CUSTOMER_DISPLAY_NAME_ATTR, MANUAL_TYPE_ATTR
from typing import Union, Dict, Optional, List, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import cached_property

JOB_ID_ATTR = 'job_id'

REACTIVE_TYPE_ATTR = 'reactive'

# Sort attributes.
REACTIVE_SORT_KEY_ATTR = 'registration_start'
MANUAL_SORT_KEY_ATTR = 'submitted_at'

STARTED_AT_ATTR = 'started_at'
SUBMITTED_AT = 'submitted_at'

_LOG = get_logger(__name__)

# Establish an ambiguous source type
Source = Union[BatchResults, Job]

_DEF_ITEMS_PER_QUERY = 100


class AmbiguousJobService:
    def __init__(self, job_service: JobService,
                 batch_results_service: BatchResultsService):
        self._manual_source_service = job_service
        self._reactive_source_service = batch_results_service

    @property
    def job_service(self) -> JobService:
        return self._manual_source_service

    @property
    def batch_results_service(self) -> BatchResultsService:
        return self._reactive_source_service

    def dto(self, entity: Source) -> dict:
        extractor: Optional[Callable] = None
        output = {}
        common = set(BatchResults.get_attributes()) & set(Job.get_attributes())
        manual = isinstance(entity, Job)
        if manual:
            _extractor = self._manual_source_service.get_job_dto
        elif isinstance(entity, BatchResults):
            _extractor = self._reactive_source_service.dto

        if extractor:
            _output = extractor(entity)
            for key in common:
                if key in _output:
                    output[key] = _output[key]
            if not manual:
                output[JOB_ID_ATTR] = entity.id

        return output

    @cached_property
    def typ_job_getter_ref(self) -> Dict[str, Callable]:
        return {
            MANUAL_TYPE_ATTR: self._manual_source_service.get_job,
            REACTIVE_TYPE_ATTR: self._reactive_source_service.get
        }

    def get(self, uid: str, typ: Optional[str] = None) -> Optional[Source]:
        ref = self.typ_job_getter_ref
        if typ:
            assert typ in ref, 'Invalid type provided'
            return ref[typ](uid)
        source = filter(lambda x: x, (get(uid) for get in ref.values()))
        return next(source, None)

    def batch_list(
        self, typ_params_map: Dict[str, List[Dict]],
        customer: Optional[str] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        sort: bool = True, ascending: bool = False
    ) -> List[Source]:
        """
        Returns a list of type-aggregated jobs, based on a type-specific
        list of payloads, to run in threads.
        :param typ_params_map: Dict[str, List[Dict]]
        :param customer: Optional[str] = None
        :param start: Optional[str] = None, lower bound
        :param end: Optional[str] = None, upper bound
        :param sort: bool = True
        :param ascending: bool = False
        :return: List[Union[BatchResults, Job]]
        """
        output = []

        typ_action_ref = {
            REACTIVE_TYPE_ATTR: self.list_reactive,
            MANUAL_TYPE_ATTR: self.list_manual
        }

        with ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(
                    typ_action_ref[typ],
                    customer=customer, start=start, end=end, **kwargs
                ): (typ, kwargs)
                for typ, params in typ_params_map.items()
                for kwargs in params
                if typ in typ_action_ref
            }
            for future in as_completed(futures):
                _typ, _kwargs = futures[future]
                _head = f'{_typ.capitalize()} job, based on - {_kwargs}'
                try:
                    iterator = future.result()
                    output.extend(iterator)
                    if sort:
                        output.sort(
                            reverse=not ascending,
                            key=self.sort_value_into_datetime
                        )
                except (Exception, BaseException) as e:
                    _LOG.warning(_head + f', has run into an issue - {e}.')

        return output

    def list_reactive(
        self, customer: Optional[str] = None,
        tenants: Optional[List[str]] = None,
        cloud_ids: Optional[List[str]] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        limit: Optional[int] = None,
        last_evaluated_key: Optional[str] = None,
        items_per_query: Optional[int] = _DEF_ITEMS_PER_QUERY
    ):
        """
        Obtains Batch Result entities, based on a provided customer, tenant
        view scope.
        :return: Iterable[BatchResults]
        """
        _service = self._reactive_source_service

        rk_condition = _service.get_registered_scope_condition(
            start=str(start.timestamp()), end=str(end.timestamp())
        )
        f_condition = _service.get_succeeded_condition(True)
        if not any((cloud_ids, tenants, customer)):
            # Issued scan.
            f_condition = rk_condition & f_condition
            rk_condition = None

        params = dict(
            customer=customer, tenants=tenants, cloud_ids=cloud_ids,
            ascending=False, range_condition=rk_condition,
            filter_condition=f_condition
        )
        return self._size_restrictive_query(
            query=_service.inquery, params=params,
            items_per_query=items_per_query, items_to_retrieve=limit,
            last_evaluated_key=last_evaluated_key
        )

    def list_manual(
        self, customer: Optional[str] = None,
        tenants: Optional[List[str]] = None,
        start: Optional[datetime] = None, end: Optional[datetime] = None,
        limit: Optional[int] = None,
        last_evaluated_key: Optional[str] = None,
        items_per_query: Optional[int] = _DEF_ITEMS_PER_QUERY
    ):
        """
        Obtains Job entities, based on a provided customer, tenant
        view scope.
        :return: List[Job]
        """
        _service = self._manual_source_service

        f_condition = _service.get_succeeded_condition(True)
        rk_condition = _service.get_submitted_scope_condition(
            start=start.isoformat(), end=end.isoformat()
        )
        if tenants:
            f_condition &= _service.get_tenant_related_condition(
                tenant=tenants[0])

        if not any((tenants, customer)):
            # Issued scan.
            f_condition = rk_condition & f_condition
            rk_condition = None

        params = dict(
            customer=customer, tenants=tenants,
            ascending=False, range_condition=rk_condition,
            filter_condition=f_condition
        )

        return self._size_restrictive_query(
            query=_service.inquery, params=params,
            items_to_retrieve=limit, last_evaluated_key=last_evaluated_key,
            items_per_query=items_per_query
        )

    @staticmethod
    def sort_value_into_datetime(item: Source):
        if isinstance(item, Job):
            return utc_datetime(getattr(item, MANUAL_SORT_KEY_ATTR))
        else:
            return ts_datetime(float(getattr(item, REACTIVE_SORT_KEY_ATTR)))

    @staticmethod
    def get_attribute(item: Source, attr: str):
        ref = {
            Job: {
                TENANT_ATTR: TENANT_DISPLAY_NAME_ATTR,
                ID_ATTR: JOB_ID_ATTR,
                CUSTOMER_ATTR: CUSTOMER_DISPLAY_NAME_ATTR
            },
            BatchResults: {
                TENANT_ATTR: TENANT_NAME_ATTR,
                CUSTOMER_ATTR: CUSTOMER_NAME_ATTR,
                STARTED_AT_ATTR: SUBMITTED_AT
            }
        }
        if not hasattr(item, attr):
            attr = ref.get(item.__class__, {}).get(attr, None)
        return getattr(item, attr) if attr else None

    @staticmethod
    def get_type(item: Source):
        ref = {
            Job: MANUAL_TYPE_ATTR, BatchResults: REACTIVE_TYPE_ATTR
        }
        return ref.get(item.__class__, None)

    @staticmethod
    def _expand_reactive_attainment_params(
            tenants: Optional[List[str]] = None,
            cloud_ids: Optional[List[str]] = None
    ):
        param_list = []
        # Preference priority of cloud-id(s).
        if cloud_ids:
            attr = 'cloud_ids'
            args = cloud_ids
        elif tenants:
            attr = 'tenants'
            args = tenants
        else:
            return [{}]

        for arg in args or (None,):
            param_list.append({attr: [arg] if arg else None})

        return param_list

    @staticmethod
    def _expand_manual_attainment_params(
            tenants: Optional[List[str]] = None,
            account_dn: Optional[str] = None
    ):
        param_list = []
        kwargs = dict(account_dn=account_dn)
        for tn in tenants or (None,):
            _kwargs = kwargs.copy()
            _kwargs.update(tenants=[tn] if tn else None)
            param_list.append(_kwargs)
        return param_list

    @classmethod
    def derive_typ_param_map(
        cls, typ: Optional[str] = None,
        tenants: Optional[List[str]] = None,
        cloud_ids: Optional[List[str]] = None,
        account_dn: Optional[str] = None
    ) -> Dict[str, List[Dict]]:

        output = {}
        _expand_reactive = cls._expand_reactive_attainment_params
        _expand_manual = cls._expand_manual_attainment_params

        typ_arg_expander_ref = {
            (REACTIVE_TYPE_ATTR, tuple(cloud_ids)): _expand_reactive,
            (MANUAL_TYPE_ATTR, account_dn): _expand_manual
        }
        for ta, expander in typ_arg_expander_ref.items():
            _typ, arg = ta
            if not typ or _typ == typ:
                _LOG.info(f'Preparing {_typ} job-type batch-inquery payload.')
                output[_typ] = expander(tenants, arg)

        return output

    @staticmethod
    def _size_restrictive_query(
        query: Callable, params: Dict, items_per_query: int,
        items_to_retrieve: Optional[int] = None,
        last_evaluated_key: Optional[str] = None
    ):
        listed = []

        # Establish pending amount of items to retrieve.
        pending = items_to_retrieve
        if pending and pending > items_per_query:
            pending -= items_per_query

        _LOG.info(f'Querying for {items_per_query} item(s), using {params}.')
        iterator: IResultIterator = query(
            limit=items_per_query, last_evaluated_key=last_evaluated_key,
            **params
        )
        listed.extend(list(iterator))

        while iterator.last_evaluated_key:

            last_evaluated_key = iterator.last_evaluated_key
            _limit = items_per_query

            if pending:
                pending -= _limit
                if pending < 0:
                    # Limit is too large, cutting down on the page size.
                    _limit -= pending

            _LOG.info(f'Querying for {_limit} item(s) more, using {params}.')
            iterator: IResultIterator = query(
                limit=_limit, last_evaluated_key=last_evaluated_key, **params
            )
            listed.extend(list(iterator))

        return listed

