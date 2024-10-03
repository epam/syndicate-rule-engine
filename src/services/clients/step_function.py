import json
from abc import ABC
from functools import cached_property

from botocore.exceptions import ClientError

from helpers.log_helper import get_logger
from services.environment_service import EnvironmentService
from services.clients import Boto3ClientFactory

_LOG = get_logger(__name__)

# STEP_FUNCTION_TO_PACKAGE_MAPPING = {
#     RETRY_REPORT_STATE_MACHINE: 'custodian_report_generation_handler',
#     SEND_REPORTS_STATE_MACHINE: 'custodian_report_generation_handler'
# }


class AbstractStepFunctionClient(ABC):
    def __init__(self, environment_service: EnvironmentService):
        self._environment_service = environment_service

    def invoke(self, state_machine_name: str, event: dict,
               job_id: str | None = None) -> bool:
        pass


class ScriptClient(AbstractStepFunctionClient):
    def invoke(self, state_machine_name, event: dict, job_id: str = None):
        _LOG.warning('Step function client is not implemented for on-prem')
        return False
        # handler = self._derive_handler(state_machine_name)
        # if handler:
        #     _LOG.debug(f'Handler: {handler}')
        #     args = [{}, RequestContext()]
        #     if event:
        #         args[0] = event
        #     Thread(target=self._handle_execution, args=(
        #         handler.lambda_handler, *args)).start()
        #     response = dict(StatusCode=202)
        #     return response

    # @staticmethod
    # def _handle_execution(handler: Callable, *args):
    #     try:
    #         response = handler(*args)
    #     except CustodianException as e:
    #         resp = e.response.build()
    #         response = dict(code=resp['statusCode'], body=resp['body'])
    #     return response
    #
    # @staticmethod
    # def _derive_handler(function_name):
    #     _LOG.debug(f'Importing lambda \'{function_name}\'')
    #     package_name = STEP_FUNCTION_TO_PACKAGE_MAPPING.get(function_name)
    #     if not package_name:
    #         return
    #     return getattr(
    #         import_module(f'lambdas.{package_name}.handler'), 'HANDLER'
    #     )


class StepFunctionClient(AbstractStepFunctionClient):
    @cached_property
    def client(self):
        return Boto3ClientFactory('stepfunctions').build(
            region_name=self._environment_service.aws_region()
        )

    @staticmethod
    def build_step_function_arn(region: str, account_id: str, name: str
                                ) -> str:
        return f'arn:aws:states:{region}:{account_id}:statemachine:{name}'

    def invoke(self, state_machine_name: str, event: dict,
               job_id: str | None = None) -> bool:
        _LOG.debug(
            f'Invoke step function {state_machine_name} with event: {event}')
        params = {
            'stateMachineArn': self.build_step_function_arn(
                self._environment_service.aws_region(),
                self._environment_service.account_id(),
                state_machine_name
            ),
            'input': json.dumps(event, separators=(',', ':'))
        }
        if job_id:
            params.update(name=job_id)
        try:
            self.client.start_execution(**params)
            return True  # todo look at status code
        except ClientError:
            _LOG.warning('Could not invoke step function', exc_info=True)
            return False
