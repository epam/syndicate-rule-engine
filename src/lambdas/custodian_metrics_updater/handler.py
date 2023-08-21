from datetime import datetime
from typing import Dict

from modular_sdk.commons.trace_helper import tracer_decorator

from helpers import raise_error_response, RESPONSE_BAD_REQUEST_CODE, \
    get_logger, build_response, RESPONSE_INTERNAL_SERVER_ERROR
from helpers.constants import DATA_TYPE
from helpers.exception import MetricsUpdateException, CustodianException
from lambdas.custodian_metrics_updater.processors.customer_metrics_processor \
    import CUSTOMER_METRICS_DIFF
from lambdas.custodian_metrics_updater.processors.findings_processor \
    import FINDINGS_UPDATER
from lambdas.custodian_metrics_updater.processors.recommendation_processor \
    import RECOMMENDATION_METRICS
from lambdas.custodian_metrics_updater.processors.\
    tenant_metrics_processor import TENANT_METRICS
from lambdas.custodian_metrics_updater.processors.\
    metric_difference_processor import TENANT_METRICS_DIFF
from lambdas.custodian_metrics_updater.processors.\
    tenant_group_metrics_processor import TENANT_GROUP_METRICS
from services import SERVICE_PROVIDER
from services.abstract_lambda import AbstractLambda
from services.clients.lambda_func import LambdaClient

METRICS_UPDATER_LAMBDA_NAME = 'caas-metrics-updater'
_LOG = get_logger(__name__)


class MetricsUpdater(AbstractLambda):
    def __init__(self, lambda_client: LambdaClient):
        self.lambda_client = lambda_client

        self.PIPELINE_TYPE_MAPPING = {
            'tenants': TENANT_METRICS,
            'tenant_groups': TENANT_GROUP_METRICS,
            'customer': CUSTOMER_METRICS_DIFF,
            'difference': TENANT_METRICS_DIFF,
            'findings': FINDINGS_UPDATER,
            'recommendations': RECOMMENDATION_METRICS
        }

        self.today = datetime.utcnow().date()
        self.compressed_info = {}
        self.customer = None

    def validate_request(self, event) -> dict:
        pass

    def handle_request(self, event, context):
        data_pipeline_type = event.get(DATA_TYPE)
        handler_function = self.PIPELINE_TYPE_MAPPING.get(data_pipeline_type)
        if not handler_function:
            raise_error_response(
                RESPONSE_BAD_REQUEST_CODE,
                f'Cannot resolve pipeline type {data_pipeline_type}')
        try:
            next_lambda_event = handler_function.process_data(event)
            if next_lambda_event.get(DATA_TYPE):
                self._invoke_next_step(next_lambda_event,
                                       METRICS_UPDATER_LAMBDA_NAME)
        except CustodianException as e:
            raise MetricsUpdateException(
                code=e.code,
                content=f'Stage {data_pipeline_type}: {e.content}')
        except Exception as e:
            raise MetricsUpdateException(
                code=RESPONSE_INTERNAL_SERVER_ERROR,
                content=f'Stage {data_pipeline_type}: {e}')

        return build_response(
            content=f'Stage \'{data_pipeline_type}\' executed successfully.')

    def _invoke_next_step(self, event: Dict[str, str],
                          lambda_name: str = METRICS_UPDATER_LAMBDA_NAME):
        _LOG.debug(f'Invocation of \'{lambda_name}\' lambda '
                   f'with event: {event}')
        response = self.lambda_client.invoke_function_async(
            function_name=lambda_name, event=event)
        _LOG.debug(f'Response: {response}')
        return response


HANDLER = MetricsUpdater(
    lambda_client=SERVICE_PROVIDER.lambda_func()
)


@tracer_decorator(is_job=True)
def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
