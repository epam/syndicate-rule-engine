from datetime import datetime
from functools import cached_property
from http import HTTPStatus
from typing import List, Optional, Union

from modular_sdk.models.pynamodb_extension.base_model import \
    LastEvaluatedKey as Lek

from helpers import build_response
from helpers.constants import (
    CUSTOMER_ATTR, TENANTS_ATTR, LIMIT_ATTR, NEXT_TOKEN_ATTR, START_ATTR,
    END_ATTR, HTTPMethod
)
from helpers.log_helper import get_logger
from lambdas.custodian_api_handler.handlers import AbstractHandler, Mapping
from services import SERVICE_PROVIDER
from services.batch_results_service import BatchResultsService

DEFAULT_UNRESOLVABLE_RESPONSE = 'Request has run into an unresolvable issue.'

BATCH_RESULTS_ID_ATTR = 'batch_results_id'

_LOG = get_logger(__name__)

BATCH_RESULTS_ENDPOINT = '/batch_results'
BATCH_RESULT_ENDPOINT = '/batch_results/{batch_results_id}'


class BatchResultsHandler(AbstractHandler):
    _code: int
    _content: Union[str, dict, list]
    _meta: Optional[dict]

    def __init__(self, batch_results_service: BatchResultsService):
        self._batch_results_service = batch_results_service
        self._reset()

    @classmethod
    def build(cls) -> 'BatchResultsHandler':
        return cls(
            batch_results_service=SERVICE_PROVIDER.batch_results_service()
        )

    @cached_property
    def mapping(self) -> Mapping:
        return {
            BATCH_RESULT_ENDPOINT: {
                HTTPMethod.GET: self.get
            },
            BATCH_RESULTS_ENDPOINT: {
                HTTPMethod.GET: self.query
            }
        }

    @property
    def response(self):
        _code, _content, _meta = self._code, self._content, self._meta
        self._reset()
        _LOG.info(f'Going to respond with the following '
                  f'code={_code}, content={_content}, meta={_meta}.')
        return build_response(code=_code, content=_content, meta=_meta)

    def _reset(self):
        self._code: Optional[int] = HTTPStatus.INTERNAL_SERVER_ERROR.value
        self._content: Optional[str] = DEFAULT_UNRESOLVABLE_RESPONSE
        self._meta: Optional[dict] = None

    def get(self, event: dict):
        _LOG.info(f'GET sole BatchResults - {event}.')
        brid = event.get(BATCH_RESULTS_ID_ATTR)
        customer = event.get(CUSTOMER_ATTR)
        tenants = event.get(TENANTS_ATTR) or []
        entity = self._attain_batch_results(
            brid=brid, customer=customer, tenants=tenants
        )
        if entity:
            self._code = HTTPStatus.OK.valueT
            self._content = self._batch_results_service.dto(entity=entity)
        return self.response

    def query(self, event: dict):
        _LOG.info(f'GET BatchResults - {event}.')
        customer = event.get(CUSTOMER_ATTR)
        tenants = event.get(TENANTS_ATTR) or []

        # Lower bound.
        start: Optional[datetime] = event.get(START_ATTR)
        if start:
            start: str = str(start.timestamp())

        # Upper bound.
        end: Optional[datetime] = event.get(END_ATTR)
        if end:
            end: str = str(end.timestamp())

        limit = event.get(LIMIT_ATTR)
        last_evaluated_key = event.get(NEXT_TOKEN_ATTR)

        i_entities = self._i_attain_batch_results(
            customer=customer, tenants=tenants,
            start=start, end=end, last_evaluated_key=last_evaluated_key,
            limit=limit
        )

        self._code = HTTPStatus.OK.value
        self._content = [
            self._batch_results_service.dto(each) for each in i_entities
        ]

        new_lek = i_entities.last_evaluated_key
        if self._content and new_lek:
            self._meta = {
                NEXT_TOKEN_ATTR: Lek(new_lek).serialize()
            }
        return self.response

    def _attain_batch_results(self, brid: str,
                              customer: Optional[str] = None,
                              tenants: Optional[List[str]] = None):
        """
        Obtains a Batch Results entity, based on a given `brid` partition key,
        verifying access, based on a customer and tenant.
        :param brid: str
        :param customer: Optional[str]
        :param tenants: Optional[List[str]]
        :return: Optional[BatchResults]
        """
        _head = f'BatchResults:\'{brid}\''
        _default_404 = _head + ' does not exist.'
        _LOG.info(_head + ' is being obtained.')
        entity = self._batch_results_service.get(batch_results=brid)

        if not entity:
            _LOG.warning(_default_404)
        elif customer and entity.customer_name != customer:
            _LOG.warning(_head + f' is not bound to \'{customer}\' customer.')
            entity = None
        elif tenants and entity.tenant_name not in tenants:
            _scope = ', '.join(map("'{}'".format, tenants)) + ' tenant(s)'
            _LOG.warning(_head + f' is not bound to any of {_scope}.')
            entity = None

        if not entity:
            self._code = HTTPStatus.NOT_FOUND.value
            self._content = _default_404

        return entity

    def _i_attain_batch_results(
            self, customer: Optional[str] = None,
            tenants: Optional[List[str]] = None,
            start: Optional[str] = None, end: Optional[str] = None,
            limit: Optional[int] = None,
            last_evaluated_key: Optional[str] = None
    ):
        """
        Obtains Batch Result entities, based on a provided customer, tenant
        view scope.
        :param customer: Optional[str]
        :param tenants: Optional[str]
        :return: Iterable[BatchResults]
        """
        _service = self._batch_results_service
        rk_condition = _service.get_registered_scope_condition(
            start=start, end=end
        )
        return self._batch_results_service.inquery(
            customer=customer, tenants=tenants,
            ascending=False, range_condition=rk_condition,
            last_evaluated_key=last_evaluated_key,
            limit=limit
        )
