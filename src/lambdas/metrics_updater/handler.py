from http import HTTPStatus

from modular_sdk.commons.trace_helper import tracer_decorator

from helpers.constants import BackgroundJobName
from helpers.lambda_response import (
    SREException,
    MetricsUpdateException,
    ResponseFactory,
    build_response,
)
from lambdas.metrics_updater.processors.findings_processor import \
    FindingsUpdater
from lambdas.metrics_updater.processors.metrics_collector import \
    MetricsCollector
from services.abs_lambda import EventProcessorLambdaHandler


class MetricsUpdater(EventProcessorLambdaHandler):
    processors = ()

    @classmethod
    def build(cls) -> 'MetricsUpdater':
        return cls()

    @tracer_decorator(
        is_job=True, 
        component=BackgroundJobName.METRICS.value,
    )
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
        except SREException as e:
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
