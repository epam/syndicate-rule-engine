from datetime import datetime
from http import HTTPStatus

from modular_sdk.commons.trace_helper import tracer_decorator

from helpers.constants import DATA_TYPE
from helpers.lambda_response import (
    CustodianException,
    MetricsUpdateException,
    ResponseFactory,
    build_response,
)
from lambdas.custodian_metrics_updater.processors.diagnostic_metrics_processor import (
    DIAGNOSTIC_METRICS,
)
from lambdas.custodian_metrics_updater.processors.findings_processor import (
    FINDINGS_UPDATER,
)
from lambdas.custodian_metrics_updater.processors.metric_difference_processor import (
    TENANT_METRICS_DIFF,
)
from lambdas.custodian_metrics_updater.processors.recommendation_processor import (
    RECOMMENDATION_METRICS,
)
from lambdas.custodian_metrics_updater.processors.tenant_group_metrics_processor import (
    TENANT_GROUP_METRICS,
)
from lambdas.custodian_metrics_updater.processors.tenant_metrics_processor import (
    TENANT_METRICS,
)
from lambdas.custodian_metrics_updater.processors.top_metrics_processor import (
    CUSTOMER_METRICS,
)
from lambdas.custodian_metrics_updater.processors.improved.new_tenant_metrics_processor import TENANT_METRICS as NEW_TENANT_METRICS
from lambdas.custodian_metrics_updater.processors.improved.new_tenant_group_metrics_processor import TENANT_GROUP_METRICS as NEW_TENANT_GROUP_METRICS
from lambdas.custodian_metrics_updater.processors.improved.new_metric_difference_processor import TENANT_METRICS_DIFF as NEW_TENANT_METRICS_DIFF
from services import SERVICE_PROVIDER
from services.abs_lambda import EventProcessorLambdaHandler
from services.clients.lambda_func import LambdaClient

METRICS_UPDATER_LAMBDA_NAME = 'caas-metrics-updater'


class MetricsUpdater(EventProcessorLambdaHandler):
    processors = ()

    def __init__(self, lambda_client: LambdaClient):
        self.lambda_client = lambda_client

        self.PIPELINE_TYPE_MAPPING = {
            'tenants': NEW_TENANT_METRICS,
            'tenant_groups': NEW_TENANT_GROUP_METRICS,
            'customer': CUSTOMER_METRICS,
            'difference': NEW_TENANT_METRICS_DIFF,
            'findings': FINDINGS_UPDATER,
            'recommendations': RECOMMENDATION_METRICS,
            'diagnostic': DIAGNOSTIC_METRICS
        }

        self.today = datetime.utcnow().date()
        self.compressed_info = {}
        self.customer = None

    def handle_request(self, event, context):
        data_pipeline_type = event.get(DATA_TYPE)
        handler_function = self.PIPELINE_TYPE_MAPPING.get(data_pipeline_type)
        if not handler_function:
            raise ResponseFactory(HTTPStatus.BAD_REQUEST).message(
                f'Cannot resolve pipeline type {data_pipeline_type}'
            ).exc()
        try:
            next_lambda_event = handler_function.process_data(event)
            if next_lambda_event.get(DATA_TYPE):
                self._invoke_next_step(next_lambda_event,
                                       METRICS_UPDATER_LAMBDA_NAME)
        except CustodianException as e:
            resp = e.response
            raise MetricsUpdateException(
                response=ResponseFactory(resp.code).message(
                    f'Stage {data_pipeline_type}: {resp.content}'
                )
            )
        except Exception as e:
            raise MetricsUpdateException(
                response=ResponseFactory(HTTPStatus.INTERNAL_SERVER_ERROR).message(
                    f'Stage {data_pipeline_type}: {e}'
                )
            )
        return build_response(
            content=f'Stage \'{data_pipeline_type}\' executed successfully.'
        )

    def _invoke_next_step(self, event: dict[str, str],
                          lambda_name: str = METRICS_UPDATER_LAMBDA_NAME):
        response = self.lambda_client.invoke_function_async(
            function_name=lambda_name, event=event)
        return response


HANDLER = MetricsUpdater(
    lambda_client=SERVICE_PROVIDER.lambda_client
)


@tracer_decorator(is_job=True)
def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
