from http import HTTPStatus
from typing import Generator, Literal

from typing_extensions import TypedDict, NotRequired

from helpers.constants import HTTPMethod


class ResponseData(TypedDict):
    status_code: str
    response_models: NotRequired[dict[str, str]]


class MethodData(TypedDict):
    integration_type: Literal['lambda',]
    enable_proxy: bool
    authorization_type: Literal['NONE', 'authorizer',]
    method_request_parameters: NotRequired[dict[str, bool]]
    method_request_models: NotRequired[dict[str, str]]
    responses: list[ResponseData]
    lambda_name: NotRequired[str]


class DeploymentResourcesApiGatewayWrapper:
    """
    Syndicate api gateway deployment resources parser
    """
    __slots__ = ('_data',)

    def __init__(self, data: dict):
        assert data.get('resource_type') == 'api_gateway', \
            'Invalid deployment resources'
        self._data = data

    def iter_path_method_data(
            self
    ) -> Generator[tuple[str, HTTPMethod, MethodData], None, None]:
        for path, path_data in (self._data.get('resources') or {}).items():
            _any = path_data.get('ANY')
            if _any:
                for method in HTTPMethod:
                    yield path, method, _any
            else:
                for method, method_data in path_data.items():
                    try:
                        method = HTTPMethod(method)
                    except ValueError:
                        continue
                    yield path, method, method_data

    def iter_path_method_lambda(
            self
    ) -> Generator[tuple[str, HTTPMethod, str, bool], None, None]:
        """
        Iterates over tuples (resource, method, lambda, has_auth)
        :return:
        """
        for path, method, data in self.iter_path_method_data():
            lambda_name = data.get('lambda_name')
            if not lambda_name:
                continue
            yield path, method, lambda_name, self.has_auth(data)

    @staticmethod
    def has_auth(data: MethodData) -> bool:
        return data.get('authorization_type') != 'NONE'

    @staticmethod
    def query_parameters(data: MethodData
                         ) -> Generator[tuple[str, bool], None, None]:
        params = data.get('method_request_parameters') or {}
        for param, required in params.items():
            yield param.split('.')[-1], required

    def json_schema(self, data: MethodData
                    ) -> tuple[str, dict] | None:
        name = data.get('method_request_models', {}).get('application/json')
        if not name:
            return None
        schema = self.get_schema(name)
        if not schema:
            return None
        return name, schema

    def get_schema(self, name: str) -> dict | None:
        models = self._data.get('models') or {}
        model = models.get(name)
        if not model:
            return None
        return model.get('schema')

    @staticmethod
    def iter_response_models(
            data: MethodData
    ) -> Generator[tuple[HTTPStatus, str | None], None, None]:
        for response in data.get('responses') or []:
            code = HTTPStatus(int(response['status_code']))
            name = (response.get('response_models') or {}).get(
                'application/json')
            yield code, name

    @property
    def stage(self) -> str:
        return self._data.get('deploy_stage') or ''
