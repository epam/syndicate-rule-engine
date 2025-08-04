import inspect
import re
from http import HTTPStatus
from typing import Callable

from bottle import Bottle, HTTPResponse, request

from helpers import RequestContext
from helpers.constants import LambdaName
from helpers.lambda_response import CustodianException, LambdaResponse, \
    ResponseFactory
from helpers.log_helper import get_logger
from lambdas.custodian_api_handler.handler import \
    lambda_handler as api_handler_lambda
from lambdas.custodian_configuration_api_handler.handler import (
    lambda_handler as configuration_api_handler_lambda,
)
from lambdas.custodian_report_generator.handler import (
    lambda_handler as report_generator_lambda,
)
from validators import registry
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
        LambdaName.API_HANDLER: api_handler_lambda,
        LambdaName.CONFIGURATION_API_HANDLER: configuration_api_handler_lambda,
        LambdaName.REPORT_GENERATOR: report_generator_lambda,
    }
    __slots__ = ('_endpoint_to_lambda', '_stage',)

    def __init__(self, stage: str = 'caas'):
        self._endpoint_to_lambda = {}
        self._stage = stage.strip('/')

    @staticmethod
    def _build_generic_error_handler(code: HTTPStatus) -> Callable:
        """
        Builds a generic callback that handles a specific error code
        :param code:
        :return:
        """
        def f(error):
            return '{"message":"%s"}' % code.phrase
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
        auth_plugin = (AuthPlugin(), )
        for info in registry.iter_all():
            params = dict(
                path=self.to_bottle_route(info.path),
                method=info.method.value,
                callback=self._callback
            )
            if info.auth:
                params.update(apply=auth_plugin)
            self._endpoint_to_lambda[(params['path'], params['method'])] = info.lambda_name
            custodian_app.route(**params)

        app.mount(self._stage, custodian_app)
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
        handler = self.lambda_name_to_handler.get(ln)
        if not handler:
            resp = ResponseFactory(HTTPStatus.NOT_FOUND).default().build()
            return HTTPResponse(
                status=resp['statusCode'],
                body=resp['body'],
                headers=resp['headers']
            )
        event = {
            'httpMethod': request.method,
            'path': request.path,
            'headers': dict(request.headers),
            'requestContext': {
                'stage': self._stage,
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


def make_app():
    """
    Creates a Bottle application with all routes and handlers.
    :return: Bottle application
    """
    return OnPremApiBuilder('caas').build()
