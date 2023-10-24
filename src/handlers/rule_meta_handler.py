from handlers.abstracts.abstract_handler import AbstractHandler
from helpers import build_response
from http import HTTPStatus
from helpers.constants import HTTPMethod, USER_CUSTOMER_ATTR
from helpers.log_helper import get_logger
from helpers.system_customer import SYSTEM_CUSTOMER
from services import SERVICE_PROVIDER
from services.clients.lambda_func import LambdaClient, \
    RULE_META_UPDATER_LAMBDA_NAME

_LOG = get_logger(__name__)


class RuleMetaHandler(AbstractHandler):
    """
    Manage Rule API
    """

    @staticmethod
    def _only_for_system(event: dict):
        if event.get(USER_CUSTOMER_ATTR) != SYSTEM_CUSTOMER:
            return build_response(code=HTTPStatus.FORBIDDEN,
                                  content='Not allowed')

    def __init__(self, lambda_client: LambdaClient):
        self._lambda_client = lambda_client

    @classmethod
    def build(cls) -> 'RuleMetaHandler':
        return cls(
            lambda_client=SERVICE_PROVIDER.lambda_func()
        )

    def define_action_mapping(self):
        return {
            '/rule-meta/standards': {
                HTTPMethod.POST: self.pull_standards,
            },
            '/rule-meta/mappings': {
                HTTPMethod.POST: self.pull_mappings,
            },
            '/rule-meta/meta': {
                HTTPMethod.POST: self.pull_meta,
            }
        }

    def pull_standards(self, event: dict):
        self._only_for_system(event)
        self._lambda_client.invoke_function_async(
            RULE_META_UPDATER_LAMBDA_NAME, {'action': 'standards'})
        return build_response(code=HTTPStatus.ACCEPTED,
                              content='Standards update was triggered')

    def pull_mappings(self, event: dict):
        self._only_for_system(event)
        self._lambda_client.invoke_function_async(
            RULE_META_UPDATER_LAMBDA_NAME, {'action': 'mappings'})
        return build_response(code=HTTPStatus.ACCEPTED,
                              content='Meta mappings update was triggered')

    def pull_meta(self, event: dict):
        self._only_for_system(event)
        # Purposefully don't allow to update meta
        # because currently we use only meta from mappings
        self._lambda_client.invoke_function_async(
            RULE_META_UPDATER_LAMBDA_NAME, {'action': 'mappings'}  # not a mistake
        )
        return build_response(code=HTTPStatus.ACCEPTED,
                              content='Meta update was triggered')
