from helpers.log_helper import get_logger
from services.environment_service import EnvironmentService
from services.clients.ssm import AbstractSSMClient

_LOG = get_logger(__name__)


class SSMService:
    def __init__(self, client: AbstractSSMClient,
                 environment_service: EnvironmentService):
        self.client = client
        self.environment_service = environment_service

    def delete_secret_value(self, secret_name: str):
        return self.client.delete_parameter(secret_name=secret_name)

    def get_secret_value(self, secret_name):
        return self.client.get_secret_value(secret_name=secret_name)

    def create_secret_value(self, secret_name, secret_value):
        self.client.create_secret(secret_name=secret_name,
                                  secret_value=secret_value)