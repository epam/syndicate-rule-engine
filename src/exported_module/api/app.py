import importlib
from functools import cached_property
from typing import Tuple, Optional

from bottle import Bottle, request, Route, HTTPResponse

from connections.auth_extension.cognito_to_jwt_adapter import \
    UNAUTHORIZED_MESSAGE
from exported_module.api.deployment_resources_parser import \
    DeploymentResourcesParser
from helpers import CustodianException, RequestContext, RESPONSE_UNAUTHORIZED
from helpers.constants import CUSTOM_CUSTOMER_ATTR, CUSTOM_ROLE_ATTR, \
    CUSTOM_TENANTS_ATTR, COGNITO_USERNAME
from helpers.constants import PARAM_HTTP_METHOD, \
    PARAM_RESOURCE_PATH
from helpers.log_helper import get_logger
from services import SERVICE_PROVIDER

_LOG = get_logger(__name__)

RESPONSE_HEADERS = {'Content-Type': 'application/json'}


class DynamicAPI:
    def __init__(self, dr_parser: DeploymentResourcesParser):
        self.app = Bottle(__name__)

        self.dr_parser = dr_parser
        self.api_config = self.dr_parser.generate_api_config()
        self.lambda_module_mapping = self.import_api_lambdas()
        self.generate_api()

    def generate_api(self):
        for request_path, endpoint_meta in self.api_config.items():
            endpoint_methods = endpoint_meta.get('allowed_methods')
            for http_method in endpoint_methods:
                route = Route(app=self.app, rule=request_path,
                              method=http_method,
                              callback=self.api)
                self.app.add_route(route=route)

    @cached_property
    def paths_without_jwt(self) -> set:
        return {
            '/caas/signin'
        }

    @staticmethod
    def get_token_from_header(header: str) -> Optional[str]:
        if not header or not isinstance(header, str):
            return
        parts = header.split()
        if len(parts) == 1:
            return parts[0]
        if len(parts) == 2 and parts[0].lower() == 'bearer':
            return parts[1]

    def authorize(self) -> dict:
        """
        May raise CustodianException.
        Returns a decoded token
        :return: dict
        """
        header = (request.headers.get('Authorization') or
                  request.headers.get('authorization'))
        token = self.get_token_from_header(header)
        if not token:
            raise CustodianException(
                code=RESPONSE_UNAUTHORIZED,
                content=UNAUTHORIZED_MESSAGE
            )
        return SERVICE_PROVIDER.cognito().decode_token(token)

    def api(self, **path_kwargs):
        try:
            if str(request.path) in self.paths_without_jwt:
                token_decoded = {}
            else:
                token_decoded = self.authorize()
        except CustodianException as e:
            response = e.response()
            return HTTPResponse(
                body=response.get('body'),
                status=response.get('statusCode'),
                headers=response.get('headers')
            )

        config_path = str(request.path)
        stage, resource_path = self._split_stage_and_resource(path=config_path)
        if request.url_args:
            # Builds up a proxy-based path.
            for key, value in request.url_args.items():
                # TODO, bug: what if value (which is a value of url args)
                #  is equal, for instance, to api stage???, or to some
                #  part of it. Think about it
                if value in config_path:
                    # Bottle-Routing compatible config path.
                    start = config_path.index(value)
                    prefix = config_path[:start]
                    suffix = config_path[start + len(value):]
                    config_path = prefix + f'<{key}>' + suffix

                    # ApiGateway-2-Lambda compatible request path.
                    start = resource_path.index(value)
                    prefix = resource_path[:start]
                    suffix = resource_path[start + len(value):]
                    resource_path = prefix + '{' + key + '}' + suffix

        endpoint_meta = self.api_config.get(config_path)
        # API-GATEWAY lambda proxy integration event
        event = {
            PARAM_HTTP_METHOD: request.method,
            'headers': dict(request.headers),
            'requestContext': {
                'stage': stage.strip('/'),
                'path': '/' + stage.strip('/') + '/' + resource_path.strip('/'),
                PARAM_RESOURCE_PATH: resource_path,
                'authorizer': {
                    'claims': {
                        COGNITO_USERNAME: token_decoded.get(COGNITO_USERNAME),
                        CUSTOM_CUSTOMER_ATTR: token_decoded.get(
                            CUSTOM_CUSTOMER_ATTR),
                        CUSTOM_ROLE_ATTR: token_decoded.get(CUSTOM_ROLE_ATTR),
                        CUSTOM_TENANTS_ATTR: token_decoded.get(
                            CUSTOM_TENANTS_ATTR) or ''
                    }
                }
            },
            'pathParameters': path_kwargs
        }
        if request.method == 'GET':
            event['queryStringParameters'] = dict(request.query)
        else:
            event['body'] = request.body.read().decode()

        lambda_module = self.lambda_module_mapping.get(
            endpoint_meta.get('lambda_name'))

        response = lambda_module.lambda_handler(event=event,
                                                context=RequestContext())
        return HTTPResponse(
            body=response.get('body'),
            status=response.get('statusCode'),
            headers=response.get('headers')
        )

    @staticmethod
    def import_api_lambdas():
        # TODO add Notification handler lambda?
        # TODO, merge report-generator and report-generator-handler
        _import = importlib.import_module
        return {
            'caas-api-handler':
                _import('lambdas.custodian_api_handler.handler'),
            'caas-configuration-api-handler':
                _import('lambdas.custodian_configuration_api_handler.handler'),
            'caas-report-generator':
                _import('lambdas.custodian_report_generator.handler'),
            'caas-report-generation-handler':
                _import('lambdas.custodian_report_generation_handler.handler')
        }

    @staticmethod
    def _split_stage_and_resource(path: str) -> Tuple[str, str]:
        """/caas/account/region -> ("/caas", "/account/region")"""
        path = path.rstrip('/')
        path = path.lstrip('/')
        first_slash = path.index('/')
        return f'/{path[:first_slash]}', path[first_slash:]
