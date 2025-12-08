from http import HTTPStatus
from typing import MutableMapping, cast

from modular_sdk.commons.trace_helper import tracer_decorator

from helpers import RequestContext
from helpers.lambda_response import (
    LambdaOutput,
    MetricsUpdateException,
    ResponseFactory,
    SREException,
    build_response,
)
from lambdas.metrics_updater.processors import (
    BaseProcessor,
    ProcessorsRegistry,
)
from services.abs_lambda import EventProcessorLambdaHandler


DATA_TYPE_KEY = "data_type"


class MetricsUpdater(EventProcessorLambdaHandler):
    """
    Lambda handler for processing metrics updates.
    Supports chained processing where one processor can trigger another.
    """

    def __init__(self, registry: ProcessorsRegistry):
        self._registry = registry

    @classmethod
    def build(cls) -> "MetricsUpdater":
        return cls(
            registry=ProcessorsRegistry.build(),
        )

    @tracer_decorator(is_job=True, component="metrics")
    def handle_request(
        self,
        event: MutableMapping,
        context: RequestContext,
    ) -> LambdaOutput:
        data_type = event.get(DATA_TYPE_KEY, "")
        processor = self._registry.get_processor(data_type)

        try:
            self._execute_processor_chain(
                processor=processor,
                event=event,
                context=context,
            )
        except SREException as e:
            raise self._wrap_exception(
                stage=data_type,
                status=e.response.code,
                message=e.response.content,
            )
        except Exception as e:
            raise self._wrap_exception(
                stage=data_type,
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
                message=str(e),
            )

        return build_response(content=f"Stage '{data_type}' executed successfully.")

    def _execute_processor_chain(
        self,
        processor: BaseProcessor,
        event: MutableMapping,
        context: RequestContext,
    ) -> None:
        """
        Execute processor and handle chained next event if returned.
        Currently supports only one level of chaining.
        """
        next_event = processor(event=event, context=context)

        if not next_event:
            return

        next_data_type = next_event.get("data_type", "")
        next_processor = self._registry.get_processor(next_data_type, required=False)

        if next_processor:
            next_processor(event=cast(MutableMapping, next_event), context=context)

    @staticmethod
    def _wrap_exception(
        stage: str,
        status: HTTPStatus,
        message: str,
    ) -> MetricsUpdateException:
        """Wrap exception with stage context for better error reporting."""
        return MetricsUpdateException(
            response=ResponseFactory(status).message(f"Stage {stage}: {message}")
        )


def lambda_handler(event, context):
    return MetricsUpdater.build().lambda_handler(event=event, context=context)
