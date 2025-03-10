from http import HTTPStatus

from modular_sdk.commons.trace_helper import tracer_decorator

from helpers.lambda_response import (
    CustodianException,
    MetricsUpdateException,
    ResponseFactory,
    build_response,
)
from lambdas.custodian_metrics_updater.processors.findings_processor import \
    FindingsUpdater
from lambdas.custodian_metrics_updater.processors.new_metrics_collector import \
    MetricsCollector
from services import SERVICE_PROVIDER
from services.abs_lambda import EventProcessorLambdaHandler
from services.clients.lambda_func import LambdaClient


class MetricsUpdater(EventProcessorLambdaHandler):
    processors = ()

    def __init__(self, lambda_client: LambdaClient):
        self.lambda_client = lambda_client

    @classmethod
    def build(cls) -> 'MetricsUpdater':
        return cls(lambda_client=SERVICE_PROVIDER.lambda_client)

    @tracer_decorator(is_job=True, component='metrics')
    def handle_request(self, event, context):
        # todo validate event
        dt = event.get('data_type')
        match dt:
            case 'findings':
                handler = FindingsUpdater.build()
            case 'metrics':
                handler = MetricsCollector.build()
            case _:
                raise ResponseFactory(HTTPStatus.BAD_REQUEST).message(
                    f'Invalid pipeline type {dt}'
                ).exc()
        try:
            handler()
        except CustodianException as e:
            resp = e.response
            raise MetricsUpdateException(
                response=ResponseFactory(resp.code).message(
                    f'Stage {dt}: {resp.content}'
                )
            )
        except Exception as e:
            raise MetricsUpdateException(
                response=ResponseFactory(
                    HTTPStatus.INTERNAL_SERVER_ERROR).message(
                    f'Stage {dt}: {e}'
                )
            )
        return build_response(
            content=f'Stage \'{dt}\' executed successfully.',
        )


def lambda_handler(event, context):
    return MetricsUpdater.build().lambda_handler(event=event, context=context)
