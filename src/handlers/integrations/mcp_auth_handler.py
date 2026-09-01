from http import HTTPStatus

from handlers import AbstractHandler, Mapping
from helpers.constants import (
    Endpoint,
    HTTPMethod,
    MCP_JWT_KEY_SSM_NAME,
)
from helpers.lambda_response import ResponseFactory, build_response
from helpers.log_helper import get_logger
from services import SP
from services.clients.ssm import AbstractSSMClient
from services.setting_service import SettingsService
from validators.swagger_request_models import (
    BaseModel,
    McpAuthSettingPatchModel,
    McpAuthSettingPostModel,
)
from validators.utils import validate_kwargs

_LOG = get_logger(__name__)


class McpAuthSettingHandler(AbstractHandler):
    """
    Manages the single global MCP JWT verification key/algorithm setting.
    Only one MCP auth configuration is allowed.
    """

    def __init__(
        self,
        settings_service: SettingsService,
        ssm_client: AbstractSSMClient,
    ):
        self.settings_service = settings_service
        self._ssm_client = ssm_client

    @property
    def mapping(self) -> Mapping:
        return {
            Endpoint.INTEGRATIONS_MCP_AUTH: {
                HTTPMethod.GET: self.get,
                HTTPMethod.POST: self.post,
                HTTPMethod.PATCH: self.patch,
                HTTPMethod.DELETE: self.delete,
            }
        }

    @classmethod
    def build(cls) -> 'McpAuthSettingHandler':
        return cls(
            settings_service=SP.settings_service,
            ssm_client=SP.ssm,
        )

    @staticmethod
    def get_dto(algorithm: str) -> dict:
        return {
            'algorithm': algorithm,
            'configured': True,
        }

    @validate_kwargs
    def get(self, event: BaseModel):
        configuration = self.settings_service.get_mcp_jwt_auth_configuration(
            value=True,
            consistent_read=True,
        ) or {}
        if not configuration:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                'MCP auth configuration is not found'
            ).exc()
        return build_response(
            content=self.get_dto(
                algorithm=configuration.get('algorithm', 'RS256')
            )
        )

    @validate_kwargs
    def post(self, event: McpAuthSettingPostModel):
        if self.settings_service.get_mcp_jwt_auth_configuration(
            value=False,
            consistent_read=True,
        ):
            return build_response(
                code=HTTPStatus.CONFLICT,
                content='MCP auth configuration already exists.',
            )

        algorithm = event.algorithm
        _LOG.info('Saving MCP JWT key to SSM')
        self._ssm_client.create_secret(
            secret_name=MCP_JWT_KEY_SSM_NAME,
            secret_value=event.jwt,
        )
        setting = self.settings_service.create_mcp_jwt_auth_configuration(
            algorithm=algorithm,
        )
        _LOG.info(f'Persisting MCP auth configuration: {setting.value}.')
        self.settings_service.save(setting=setting)
        return build_response(
            code=HTTPStatus.CREATED,
            content=self.get_dto(algorithm=algorithm),
        )

    @validate_kwargs
    def patch(self, event: McpAuthSettingPatchModel):
        setting = self.settings_service.get_mcp_jwt_auth_configuration(
            value=False,
            consistent_read=True,
        )
        if not setting:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content='MCP auth configuration does not exist.',
            )

        configuration = setting.value if isinstance(setting.value, dict) else {}
        algorithm = configuration.get('algorithm') or 'RS256'

        if event.jwt is not None:
            _LOG.info('Updating MCP JWT key in SSM')
            self._ssm_client.create_secret(
                secret_name=MCP_JWT_KEY_SSM_NAME,
                secret_value=event.jwt,
            )

        if event.algorithm is not None:
            algorithm = event.algorithm
            configuration = dict(configuration)
            configuration['algorithm'] = algorithm
            setting.value = configuration
            _LOG.info(f'Updating MCP auth configuration: {setting.value}.')
            self.settings_service.save(setting=setting)

        return build_response(
            code=HTTPStatus.OK,
            content=self.get_dto(algorithm=algorithm),
        )

    @validate_kwargs
    def delete(self, event: BaseModel):
        setting = self.settings_service.get_mcp_jwt_auth_configuration(
            value=False,
            consistent_read=True,
        )
        if not setting:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content='MCP auth configuration does not exist.',
            )

        _LOG.info('Removing MCP auth configuration')
        self.settings_service.delete(setting=setting)
        self._ssm_client.delete_parameter(MCP_JWT_KEY_SSM_NAME)
        return build_response(code=HTTPStatus.NO_CONTENT)
