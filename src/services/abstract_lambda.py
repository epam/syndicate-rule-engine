import json
from abc import abstractmethod
from http import HTTPStatus

from modular_sdk.commons.exception import ModularException

from helpers import CustodianException
from helpers.exception import MetricsUpdateException
from helpers.log_helper import get_logger

PARAM_NATIVE_JOB_ID = 'jobId'
PARAM_ROLE = 'role'

_LOG = get_logger(__name__)

REQUEST_CONTEXT = None


class AbstractLambda:

    @abstractmethod
    def handle_request(self, event, context):
        """
        Inherited lambda function code
        :param event: lambda event
        :param context: lambda context
        :return:
        """
        pass

    def lambda_handler(self, event, context):
        global REQUEST_CONTEXT
        REQUEST_CONTEXT = context
        try:
            _LOG.debug(f'Request: {json.dumps(event)}, '
                       f'request id: \'{context.aws_request_id}\'')
            execution_result = self.handle_request(
                event=event,
                context=context
            )
            _LOG.debug(f'Response: {execution_result}')
            return execution_result
        except MetricsUpdateException as e:
            _LOG.warning(f'Metrics update exception occurred: {e}')
            raise e  # needed for ModularJobs to track failed jobs
        except ModularException as e:
            _LOG.warning(f'Modular exception occurred: {e}')
            return CustodianException(
                code=e.code,
                content=e.content
            ).response()
        except CustodianException as e:
            _LOG.warning(f'Custodian exception occurred: {e}')
            return e.response()
        except Exception:
            _LOG.exception('Unexpected error occurred.')
            return CustodianException(
                code=HTTPStatus.INTERNAL_SERVER_ERROR.value,
                content='Internal server error'
            ).response()
