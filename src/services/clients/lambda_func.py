import json
from importlib import import_module
from threading import Thread
from typing import Callable

from helpers import RequestContext
from helpers.lambda_response import CustodianException
from helpers.log_helper import get_logger
from services.environment_service import EnvironmentService
from services.clients import Boto3ClientFactory

_LOG = get_logger(__name__)
RULE_META_UPDATER_LAMBDA_NAME = 'caas-rule-meta-updater'
CONFIGURATION_BACKUPPER_LAMBDA_NAME = 'caas-configuration-backupper'
LICENSE_UPDATER_LAMBDA_NAME = 'caas-license-updater'
CAAS_EVENT_HANDLER = 'caas-event-handler'
METRICS_UPDATER_LAMBDA_NAME = 'caas-metrics-updater'

LAMBDA_TO_PACKAGE_MAPPING = {
    RULE_META_UPDATER_LAMBDA_NAME: 'custodian_rule_meta_updater',
    CONFIGURATION_BACKUPPER_LAMBDA_NAME: 'custodian_configuration_backupper',
    LICENSE_UPDATER_LAMBDA_NAME: 'custodian_license_updater',
    CAAS_EVENT_HANDLER: 'custodian_event_handler',
    METRICS_UPDATER_LAMBDA_NAME: 'custodian_metrics_updater'
}


class LambdaClient:
    def __init__(self, environment_service: EnvironmentService):
        self.is_docker = environment_service.is_docker()
        self.alias = environment_service.lambdas_alias_name()
        self._client = None
        self._environment = environment_service

    @property
    def client(self):
        """Returns client for saas. For on-prem the method is not used"""
        if not self._client:
            self._client = Boto3ClientFactory('lambda').build(
                region_name=self._environment.aws_region()
            )
        return self._client

    def invoke_function_async(self, function_name, event=None):
        if self.is_docker:
            return self._invoke_function_docker(
                function_name=function_name,
                event=event
            )
        else:
            if self.alias:
                function_name = f'{function_name}:{self.alias}'
            return self.client.invoke(
                FunctionName=function_name,
                InvocationType='Event',
                Payload=json.dumps(event or {}).encode())

    def invoke_function(self, function_name, event=None):
        """
        Synchronous concern separated invocation.
        """
        # todo refactor invocation for the common signature.
        if self.is_docker:
            return self._invoke_function_docker(
                function_name=function_name,
                event=event,
                wait=True
            )
        else:
            if self.alias:
                function_name = f'{function_name}:{self.alias}'
            return self.client.invoke(
                FunctionName=function_name,
                InvocationType='RequestResponse',
                Payload=json.dumps(event or {}).encode())

    @staticmethod
    def _derive_handler(function_name):
        # todo add ability to call configuration-backupper
        """
        Produces a lambda handler function class,
        adhering to the LAMBDA_TO_PACKAGE_MAPPING.
        :return:Union[AbstractLambda, Type[None]]
        """
        _LOG.debug(f'Importing lambda \'{function_name}\'')
        package_name = LAMBDA_TO_PACKAGE_MAPPING.get(function_name)
        if not package_name:
            return
        return getattr(
            import_module(f'lambdas.{package_name}.handler'), 'HANDLER'
        )

    def _invoke_function_docker(self, function_name, event=None, wait=False):
        handler = self._derive_handler(function_name)
        if handler:
            _LOG.debug(f'Handler: {handler}')
            args = [{}, RequestContext()]
            if event:
                args[0] = event
            if wait:
                response = self._handle_execution(
                    handler.lambda_handler, *args
                )
            else:
                Thread(target=self._handle_execution, args=(
                    handler.lambda_handler, *args)).start()
                response = dict(StatusCode=202)
            return response

    @staticmethod
    def _handle_execution(handler: Callable, *args):
        try:
            response = handler(*args)
        except CustodianException as e:
            resp = e.response.build()
            response = dict(code=resp['statusCode'], body=resp['body'])
        return response
