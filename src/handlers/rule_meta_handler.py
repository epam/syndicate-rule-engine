from http import HTTPStatus

from handlers import AbstractHandler, Mapping
from helpers.constants import CustodianEndpoint, HTTPMethod
from helpers.lambda_response import build_response
from services import SERVICE_PROVIDER
from services.clients.lambda_func import LambdaClient, RULE_META_UPDATER_LAMBDA_NAME
from validators.swagger_request_models import BaseModel
from validators.utils import validate_kwargs


class RuleMetaHandler(AbstractHandler):
    """
    Manage Rule API
    """
    def __init__(self, lambda_client: LambdaClient):
        self._lambda_client = lambda_client

    @classmethod
    def build(cls) -> 'RuleMetaHandler':
        return cls(
            lambda_client=SERVICE_PROVIDER.lambda_client
        )

    @property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.META_STANDARDS: {
                HTTPMethod.POST: self.pull_standards,
            },
            CustodianEndpoint.META_META: {
                HTTPMethod.POST: self.pull_meta,
            }
        }

    @validate_kwargs
    def pull_standards(self, event: BaseModel):
        self._lambda_client.invoke_function_async(
            RULE_META_UPDATER_LAMBDA_NAME, {'action': 'standards'})
        return build_response(code=HTTPStatus.ACCEPTED,
                              content='Standards update was triggered')

    @validate_kwargs
    def pull_meta(self, event: BaseModel):
        self._lambda_client.invoke_function_async(
            RULE_META_UPDATER_LAMBDA_NAME, {'action': 'mappings'}
        )
        return build_response(code=HTTPStatus.ACCEPTED,
                              content='Meta update was triggered')
