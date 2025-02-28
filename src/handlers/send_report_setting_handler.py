from http import HTTPStatus

from modular_sdk.services.customer_service import CustomerService

from handlers import AbstractHandler, Mapping
from helpers.constants import CustodianEndpoint, HTTPMethod
from helpers.lambda_response import build_response
from helpers.log_helper import get_logger
from helpers.system_customer import SystemCustomer
from services import SP
from services.clients.step_function import ScriptClient, StepFunctionClient
from services.health_check_service import RabbitMQConnectionCheck
from services.setting_service import SettingsService
from validators.swagger_request_models import ReportsSendingSettingPostModel
from validators.utils import validate_kwargs

RETRY_REPORT_STATE_MACHINE = 'retry_send_reports'
_LOG = get_logger(__name__)


class ReportsSendingSettingHandler(AbstractHandler):
    """
    Manages reports sending configuration.
    """

    def __init__(self, settings_service: SettingsService,
                 step_function_client: ScriptClient | StepFunctionClient,
                 customer_service: CustomerService):
        self.settings_service = settings_service
        self.step_function_client = step_function_client
        self.customer_service = customer_service

    @property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.SETTINGS_SEND_REPORTS: {
                HTTPMethod.POST: self.post
            }
        }

    @classmethod
    def build(cls):
        return cls(
            settings_service=SP.settings_service,
            step_function_client=SP.step_function,
            customer_service=SP.modular_client.customer_service()
        )

    @staticmethod
    def build_retry_event() -> dict:
        res = CustodianEndpoint.REPORTS_RETRY.value
        return {
            'resource': res,
            'path': res,
            'httpMethod': HTTPMethod.POST,
            'queryStringParameters': {},
            'pathParameters': {},
            'requestContext': {
                'path': f'/caas/{res}',  # stage does not matter here
                'resourcePath': res,
                'httpMethod': HTTPMethod.POST,
                'protocol': 'HTTP/1.1',
                'authorizer': {
                    'claims': {
                        'custom:customer': SystemCustomer.get_name(),
                    }
                }
            },
            'body': '',
            'isBase64Encoded': False
        }

    @validate_kwargs
    def post(self, event: ReportsSendingSettingPostModel):

        setting = self.settings_service.get_send_reports()
        if setting == event.enable:
            return build_response(
                code=HTTPStatus.OK,
                content=f'The SEND_REPORTS setting is already '
                        f'{"enabled" if event.enable else "disabled"}'
            )
        if event.enable:
            health_check_result = []
            instance = RabbitMQConnectionCheck.build()
            for customer in self.customer_service.i_get_customer():
                try:
                    result = instance.check(customer=customer.name)
                except Exception as e:
                    _LOG.exception(
                        f'An unknown exception occurred trying to execute '
                        f'check `{instance.identifier()}` for customer '
                        f'{customer.name}')
                    result = instance.unknown_result(details={'error': str(e)})
                _LOG.info(f'Check: {instance.identifier()} for customer '
                          f'{customer.name} has finished')
                health_check_result.append(result.is_ok())
            if not any(health_check_result):
                # for what?
                _LOG.warning(
                    'Could not enable reports sending system because ALL '
                    'customers have non-working RabbitMQ configuration.')
                return build_response(
                    code=HTTPStatus.OK,
                    content='Could not enable reports sending system.'
                )
            self.settings_service.enable_send_reports()
            self.step_function_client.invoke(
                RETRY_REPORT_STATE_MACHINE,
                event=self.build_retry_event()
            )
        else:
            self.settings_service.disable_send_reports()

        return build_response(
            code=HTTPStatus.OK,
            content=f'Value of SEND_REPORTS setting was changed to {event.enable}'
        )
