from abc import abstractmethod

from helpers import build_response, CustodianException, \
    RESPONSE_INTERNAL_SERVER_ERROR
from helpers.exception import MetricsUpdateException
from helpers.log_helper import get_logger
from modular_sdk.commons.exception import ModularException

PARAM_JOB_ID = 'job_id'
PARAM_NATIVE_JOB_ID = 'jobId'
PARAM_LATEST = 'latest'
PARAM_NEAREST = 'nearest_to'
PARAM_DATE = 'date'
PARAM_HTTP_METHOD = 'http_method'
PARAM_DETAILED_REPORT = 'detailed'
PARAM_GET_URL = 'get_url'
PARAM_ROLE = 'role'
ACTION_PARAM = 'action'
TARGET_PERMISSION_PARAM = 'target_permission'

_LOG = get_logger(__name__)

REQUEST_CONTEXT = None


class AbstractLambda:

    @abstractmethod
    def validate_request(self, event) -> dict:
        """
        Validates event attributes
        :param event: lambda incoming event
        :return: dict with attribute_name in key and error_message in value
        """
        pass

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
            _LOG.debug(f'Request: {event}, '
                       f'request id: \'{context.aws_request_id}\'')
            errors = self.validate_request(event=event)
            if errors:
                return build_response(code=400,
                                      content=errors)
            execution_result = self.handle_request(event=event,
                                                   context=context)
            _LOG.debug(f'Response: {execution_result}')
            return execution_result
        except MetricsUpdateException as e:
            _LOG.warning(f'Metrics update exception occurred; Error: {e}')
            raise e  # needed for ModularJobs to track failed jobs
        except ModularException as e:
            _LOG.warning(f'Modular exception occurred; Error: {e}')
            return CustodianException(
                code=e.code,
                content=e.content
            ).response()
        except CustodianException as e:
            _LOG.warning(f'Error occurred; Event: {event}; Error: {e}')
            return e.response()
        except Exception as e:
            _LOG.error(
                f'Unexpected error occurred; Event: {event}; Error: {e}')
            return CustodianException(
                code=RESPONSE_INTERNAL_SERVER_ERROR,
                content='Internal server error').response()
