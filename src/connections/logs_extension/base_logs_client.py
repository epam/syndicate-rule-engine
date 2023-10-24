import os

from helpers.log_helper import get_logger
from services.clients.cloudwatch import CloudWatchClient
from helpers.constants import ENV_SERVICE_MODE, DOCKER_SERVICE_MODE, \
    SAAS_SERVICE_MODE

_LOG = get_logger(__name__)

SERVICE_MODE = os.getenv(ENV_SERVICE_MODE) or SAAS_SERVICE_MODE


def log_file_handler_builder():
    handler = None

    def init_handler():
        assert SERVICE_MODE == DOCKER_SERVICE_MODE, \
            "You can init log file handler only if SERVICE_MODE=docker"
        nonlocal handler
        if handler:
            return handler
        from connections.logs_extension.cw_to_log_file_adapter import \
            CWToLogFileAdapter
        handler = CWToLogFileAdapter()
        _LOG.info('Log file connection was successfully initialized')
        return handler
    return init_handler


LOGS_HANDLER = log_file_handler_builder()


class BaseLogsClient(CloudWatchClient):
    is_docker = SERVICE_MODE == DOCKER_SERVICE_MODE

    def __init__(self, region=None):
        self.region = region
        if not self.is_docker:
            super().__init__(region)

    def get_log_events(self, log_group_name, start, end, log_stream_name=None,
                       job_id=None):
        if self.is_docker:
            return LOGS_HANDLER().get_log_events(job_id)
        return super().get_log_events(log_group_name, log_stream_name, start,
                                      end)
