from modular_sdk.models.job import Job

from handlers import AbstractHandler, Mapping
from helpers.constants import Endpoint, HTTPMethod
from helpers.lambda_response import build_response
from helpers.log_helper import get_logger
from services import SP
from services.service_operation_service import ServiceOperationService
from validators.swagger_request_models import ServiceOperationStatusGetModel
from validators.utils import validate_kwargs

_LOG = get_logger(__name__)


class ServiceOperationStatusHandler(AbstractHandler):
    """
    Unified handler for retrieving the status of service operations.
    
    Supports multiple service operation types (e.g., metrics, metadata) through
    the query parameter `type`. Allows optional filtering by
    date range using `start_iso` and `end_iso` query parameters.
    
    Endpoint: /service-operation/status?type={service_operation_type}
    Method: GET
    
    Returns a list of operation statuses with their start time and current state.
    """
    
    def __init__(self, service_operation_service: ServiceOperationService):
        self._service_operation_service = service_operation_service

    @classmethod
    def build(cls) -> 'ServiceOperationStatusHandler':
        return cls(
            service_operation_service=SP.service_operation_service,
        )

    @property
    def mapping(self) -> Mapping:
        return {Endpoint.SERVICE_OPERATION_STATUS: {HTTPMethod.GET: self.get}}

    @validate_kwargs
    def get(self, event: ServiceOperationStatusGetModel):
        service_operation_type = event.type
        from_ = event.start_iso
        to = event.end_iso
        limit = 10

        items: list[Job] = []
        if not (from_ and to):
            _LOG.info(f'Getting latest {service_operation_type.value} operation')
            item = self._service_operation_service.get_latest_by_type(
                service_operation_type=service_operation_type,
            )
            if item:
                items.append(item)
        else:
            _LOG.info(
                f'Getting {service_operation_type.value} operations '
                f'between {from_} and {to} with limit {limit}'
            )
            items = list(
                self._service_operation_service.get_by_type(
                    service_operation_type=service_operation_type,
                    start=from_,
                    end=to,
                    limit=limit,
                    ascending=False,
                )
            )

        if not items:
            _LOG.warning(f'Cannot find {service_operation_type.value} operation')
            
        return build_response(
            content=[self._service_operation_service.to_dto(item) for item in items]
        )

