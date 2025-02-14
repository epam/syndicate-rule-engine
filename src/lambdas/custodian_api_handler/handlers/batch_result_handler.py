from http import HTTPStatus

from helpers.constants import CustodianEndpoint, HTTPMethod
from helpers import NextToken
from helpers.lambda_response import ResponseFactory, build_response
from handlers import AbstractHandler, Mapping
from services import SERVICE_PROVIDER
from services.batch_results_service import BatchResultsService
from validators.swagger_request_models import BaseModel, BatchResultsQueryModel
from validators.utils import validate_kwargs


class BatchResultsHandler(AbstractHandler):

    def __init__(self, batch_results_service: BatchResultsService):
        self._batch_results_service = batch_results_service

    @classmethod
    def build(cls) -> 'BatchResultsHandler':
        return cls(
            batch_results_service=SERVICE_PROVIDER.batch_results_service
        )

    @property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.BATCH_RESULTS_JOB_ID: {
                HTTPMethod.GET: self.get
            },
            CustodianEndpoint.BATCH_RESULTS: {
                HTTPMethod.GET: self.query
            }
        }

    @validate_kwargs
    def get(self, event: BaseModel, batch_results_id: str):
        item = self._batch_results_service.get_nullable(batch_results_id)
        if not item or event.customer and item.customer_name != event.customer:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).default().exc()
        return build_response(
            content=self._batch_results_service.dto(item)
        )

    @validate_kwargs
    def query(self, event: BatchResultsQueryModel):

        if event.tenant_name:
            cursor = self._batch_results_service.get_by_tenant_name(
                tenant_name=event.tenant_name,
                limit=event.limit,
                last_evaluated_key=NextToken(event.next_token).value,
                start=event.start,
                end=event.end
            )
        else:
            cursor = self._batch_results_service.get_by_customer_name(
                customer_name=event.customer,
                limit=event.limit,
                last_evaluated_key=NextToken(event.next_token).value,
                start=event.start,
                end=event.end
            )
        jobs = list(cursor)
        return ResponseFactory().items(
            it=map(self._batch_results_service.dto, jobs),
            next_token=NextToken(cursor.last_evaluated_key)
        ).build()
