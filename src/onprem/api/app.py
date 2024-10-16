import inspect
import json
import re
from http import HTTPStatus
from typing import Callable

from bottle import Bottle, HTTPResponse, request

from helpers import RequestContext
from helpers.lambda_response import CustodianException, LambdaResponse, \
    ResponseFactory
from helpers.log_helper import get_logger
from lambdas.custodian_api_handler.handler import \
    lambda_handler as api_handler_lambda
from lambdas.custodian_configuration_api_handler.handler import (
    lambda_handler as configuration_api_handler_lambda,
)
from lambdas.custodian_report_generation_handler.handler import (
    lambda_handler as report_generation_handler,
)
from lambdas.custodian_report_generator.handler import (
    lambda_handler as report_generator_lambda,
)
from onprem.api.deployment_resources_parser import DeploymentResourcesApiGatewayWrapper
from services import SERVICE_PROVIDER
from services.clients.mongo_ssm_auth_client import UNAUTHORIZED_MESSAGE

_LOG = get_logger(__name__)


class AuthPlugin:
    """
    Authenticates the user
    """
    __slots__ = 'name',

    def __init__(self):
        self.name = 'custodian-auth'

    @staticmethod
    def get_token_from_header(header: str) -> str | None:
        if not header or not isinstance(header, str):
            return
        parts = header.split()
        if len(parts) == 1:
            return parts[0]
        if len(parts) == 2 and parts[0].lower() == 'bearer':
            return parts[1]

    @staticmethod
    def _to_bottle_resp(resp: LambdaResponse) -> HTTPResponse:
        built = resp.build()
        return HTTPResponse(
            body=built['body'],
            status=built['statusCode'],
            headers=built['headers']
        )

    def __call__(self, callback: Callable):
        def wrapper(*args, **kwargs):
            _LOG.info('Checking whether request is authorized in AuthPlugin')
            header = (request.headers.get('Authorization') or
                      request.headers.get('authorization'))
            token = self.get_token_from_header(header)

            if not token:
                _LOG.warning('Token not found in header')
                resp = ResponseFactory(HTTPStatus.UNAUTHORIZED).message(
                    UNAUTHORIZED_MESSAGE
                )
                return self._to_bottle_resp(resp)

            try:
                decoded = SERVICE_PROVIDER.onprem_users_client.decode_token(
                    token
                )
            except CustodianException as e:
                return self._to_bottle_resp(e.response)

            _LOG.info('Token decoded successfully')
            sign = inspect.signature(callback)
            if 'decoded_token' in sign.parameters:
                _LOG.debug('Expanding callback with decoded token')
                kwargs['decoded_token'] = decoded
            return callback(*args, **kwargs)

        return wrapper


class OnPremApiBuilder:
    dynamic_resource_regex = re.compile(r'([^{/]+)(?=})')
    lambda_name_to_handler = {
        'caas-api-handler': api_handler_lambda,
        'caas-configuration-api-handler': configuration_api_handler_lambda,
        'caas-report-generator': report_generator_lambda,
        'caas-report-generation-handler': report_generation_handler
    }

    def __init__(self, dp_wrapper: DeploymentResourcesApiGatewayWrapper):
        self._dp_wrapper = dp_wrapper

        self._endpoint_to_lambda = {}

    @staticmethod
    def _build_generic_error_handler(code: HTTPStatus) -> Callable:
        """
        Builds a generic callback that handles a specific error code
        :param code:
        :return:
        """
        def f(error):
            return json.dumps({'message': code.phrase}, separators=(',', ':'))
        return f

    def _register_errors(self, application: Bottle) -> None:
        for code in (HTTPStatus.NOT_FOUND, HTTPStatus.INTERNAL_SERVER_ERROR):
            application.error_handler[code.value] = self._build_generic_error_handler(code)

    @staticmethod
    def _add_hooks(application: Bottle) -> None:
        @application.hook('before_request')
        def strip_path():
            """
            Our endpoints do not depend on trailing slashes. Api gw treats
            /one and /one/ as equal
            :return:
            """
            request.environ['PATH_INFO'] = request.environ['PATH_INFO'].rstrip(
                '/'
            )

    def build(self) -> Bottle:
        self._endpoint_to_lambda.clear()
        app = Bottle()
        self._add_hooks(app)

        custodian_app = Bottle()
        self._register_errors(custodian_app)
        it = self._dp_wrapper.iter_path_method_lambda()
        auth_plugin = AuthPlugin()
        for path, method, ln, has_auth in it:
            path = self.to_bottle_route(path)
            method = method.value

            self._endpoint_to_lambda[(path, method)] = ln
            params = dict(
                path=path,
                method=method,
                callback=self._callback
            )
            if has_auth:
                params['apply'] = [auth_plugin]
            custodian_app.route(**params)

        app.mount(self._dp_wrapper.stage.strip('/'), custodian_app)
        return app

    @classmethod
    def to_bottle_route(cls, resource: str) -> str:
        """
        Returns a proxied resource path, compatible with Bottle.
        >>> OnPremApiBuilder.to_bottle_route('/path/{id}')
        '/path/<id>'
        >>> OnPremApiBuilder.to_bottle_route('/some/data/{test}')
        /some/data/<test>
        :return: str
        """
        for match in re.finditer(cls.dynamic_resource_regex, resource):
            suffix = resource[match.end() + 1:]
            resource = resource[:match.start() - 1]
            path_input = match.group()
            path_input = path_input.strip('{+')
            resource += f'<{path_input}>' + suffix
        return resource

    def _callback(self, decoded_token: dict | None = None, **path_params):
        method = request.method
        path = request.route.rule
        ln = self._endpoint_to_lambda[(path, method)]
        handler = self.lambda_name_to_handler[ln]
        event = {
            'httpMethod': request.method,
            'path': request.path,
            'headers': dict(request.headers),
            'requestContext': {
                'stage': self._dp_wrapper.stage,
                'resourcePath': path.replace('<', '{').replace('>', '}').replace('proxy', 'proxy+'),  # kludge
                'path': request.fullpath
            },
            'pathParameters': path_params
        }
        if decoded_token:
            event['requestContext']['authorizer'] = {
                'claims': {
                    'cognito:username': decoded_token.get('cognito:username'),
                    'sub': decoded_token.get('sub'),
                    'custom:customer': decoded_token.get(
                        'custom:customer'),
                    'custom:role': decoded_token.get('custom:role'),
                    'custom:tenants': decoded_token.get('custom:tenants') or ''
                }
            }

        if method == 'GET':
            event['queryStringParameters'] = dict(request.query)
        else:
            event['body'] = request.body.read().decode()
            event['isBase64Encoded'] = False

        _LOG.info(f'Handling request: {request.method}:{request.path}')
        response = handler(event, RequestContext())
        _LOG.info('Request was handled. Returning response')

        return HTTPResponse(
            body=response['body'],
            status=response['statusCode'],
            headers=response['headers']
        )
