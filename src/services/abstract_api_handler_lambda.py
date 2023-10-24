import json
from abc import abstractmethod
from http import HTTPStatus
from typing import Optional

from modular_sdk.commons.exception import ModularException

from helpers import build_response, CustodianException, deep_get
from helpers.constants import PARAM_USER_ID, PARAM_USER_ROLE, \
    PARAM_REQUEST_PATH, PARAM_HTTP_METHOD, PARAM_USER_CUSTOMER, \
    PARAM_RESOURCE_PATH, ENV_API_GATEWAY_STAGE, ENV_API_GATEWAY_HOST, \
    HTTPMethod
from helpers.log_helper import get_logger
from services import SERVICE_PROVIDER

_LOG = get_logger(__name__)

# remove pathParams
UNNECESSARY_EVENT_PARAMS = {
    'resource', 'multiValueHeaders', 'multiValueQueryStringParameters',
    'stageVariables', 'requestContext', 'headers', 'isBase64Encoded'
}
REQUEST_CONTEXT = None

NOT_ALLOWED_TO_ACCESS_ENTITY = 'You are not allowed to access this entity'
NOT_ENOUGH_DATA = 'Not enough data to proceed the request'


class AbstractApiHandlerLambda:
    @abstractmethod
    def handle_request(self, event, context):
        """
        Inherited lambda function code
        :param event: lambda event
        :param context: lambda context
        :return:
        """
        pass

    @staticmethod
    def _resolve_stage(resource_path: str, path: str) -> str:
        """
        :param resource_path: path without stage, cannot contain "{}"
        /applications/58da234b-f85b-4a8b-810a-c201f3c55e5f
        :param path: path with stage
        /caas/applications/58da234b-f85b-4a8b-810a-c201f3c55e5f
        :return: str, 'caas' in the example
        """
        resource_path = resource_path.strip('/')
        path = path.strip('/')
        return path[:len(path) - len(resource_path)].strip('/')

    def lambda_handler(self, event, context):
        global REQUEST_CONTEXT
        REQUEST_CONTEXT = context
        _env = SERVICE_PROVIDER.environment_service()
        # These overriden envs used only for POST, PATCH /applications/access
        path_params = event.get('pathParameters') or {}
        _env.override_environment({
            ENV_API_GATEWAY_HOST: deep_get(event, ('headers', 'Host')),
            ENV_API_GATEWAY_STAGE: self._resolve_stage(
                deep_get(event, ('requestContext', 'resourcePath')).format_map(
                    path_params),
                deep_get(event, ('requestContext', 'path')).format_map(
                    path_params)
            )
        })
        try:
            _LOG.debug(f'Request: {json.dumps(event)}, '
                       f'request id: \'{context.aws_request_id}\'')
            _LOG.debug('Checking user permissions')

            # _, request_path = self._split_stage_and_resource(
            #     deep_get(event, ['requestContext', 'path']))

            # todo consider using `resourcePath` convention, instead of
            #  request one.

            request_path = resource_path = deep_get(
                event, ['requestContext', PARAM_RESOURCE_PATH]
            )
            event[PARAM_REQUEST_PATH] = self._floor_request_path(
                request_path=resource_path
            )
            request_method = event.get(PARAM_HTTP_METHOD)
            target_permission = self._get_target_permission(
                request_path, request_method)
            _request_context = event.get('requestContext') or {}

            _LOG.debug('Removing unnecessary attrs from event')
            self._pop_excessive_attributes(event)
            _LOG.debug(f'Event after removing unnecessary attrs: {event}')

            _LOG.debug('Formatting and validating event`s body')
            self._format_event(event)
            _LOG.debug(f'Event after validating and formatting: {event}')

            user_id = deep_get(
                _request_context, ['authorizer', 'claims',
                                   'cognito:username'])
            user_customer = deep_get(
                _request_context, ['authorizer', 'claims',
                                   'custom:customer'])
            user_role = deep_get(
                _request_context, ['authorizer', 'claims', 'custom:role'])
            user_tenants = deep_get(
                _request_context, ['authorizer', 'claims',
                                   'custom:tenants'])
            if user_id and user_customer and user_role:  # endpoint with cognito
                event[PARAM_USER_ID] = user_id
                event[PARAM_USER_ROLE] = user_role
                event[PARAM_USER_CUSTOMER] = user_customer
                if target_permission:
                    _LOG.info('Restricting access by RBAC')
                    if not SERVICE_PROVIDER.access_control_service().is_allowed_to_access(  # noqa
                            customer=user_customer, role_name=user_role,
                            target_permission=target_permission):
                        message = f'User \'{event.get(PARAM_USER_ID)}\' ' \
                                  f'is not allowed to access the resource: ' \
                                  f'\'{target_permission}\''
                        _LOG.info(message)
                        return build_response(
                            code=HTTPStatus.FORBIDDEN.value,
                            content=message
                        )
                _LOG.info(
                    f'Restricting access by entities: '
                    f'user_customer={user_customer}, '
                    f'user_tenants={user_tenants}')
                restriction_service = SERVICE_PROVIDER.restriction_service()
                restriction_service.set_endpoint(request_path, request_method)
                restriction_service.set_user_entities(
                    user_customer,
                    user_tenants=user_tenants.replace(' ', '').split(',')
                    if user_tenants else [])
                restriction_service.update_event(event)
            else:  # for example /signin
                _LOG.debug(f'Authorizer is not provided for '
                           f'endpoint: {request_path}. Allowing...')

            execution_result = self.handle_request(
                event=event,
                context=context
            )
            _LOG.debug(f'Response: {execution_result}')
            return execution_result
        except ModularException as e:
            _LOG.warning(f'Modular exception occurred: {e}')
            return CustodianException(
                code=e.code,
                content=e.content
            ).response()
        except CustodianException as e:
            _LOG.warning(f'Custodian exception occurred: {e}')
            return e.response()
        except Exception:
            _LOG.exception('Unexpected error occurred')
            return CustodianException(
                code=HTTPStatus.INTERNAL_SERVER_ERROR.value,
                content='Internal server error'
            ).response()

    def _get_target_permission(self, request_path: str,
                               http_method: str) -> Optional[str]:
        from services.rbac.endpoint_to_permission_mapping import \
            ENDPOINT_PERMISSION_MAPPING, PERMISSIONS
        return ENDPOINT_PERMISSION_MAPPING.get(
            self._reformat_request_path(request_path), {}
        ).get(http_method, {}).get(PERMISSIONS)

    def _validate_event(self, request_path: str, http_method: str,
                        body: dict) -> dict:
        """
        Validates an input payload using endpoint's validator and in case
        the validation has passed returns validated dict. If the validation
        fails, raises 400
        :param request_path: str
        :param http_method: str
        :param body: dict
        :return: dict
        """
        from services.rbac.endpoint_to_permission_mapping import \
            ENDPOINT_PERMISSION_MAPPING, VALIDATION
        from validators.utils import validate_pydantic
        validation = ENDPOINT_PERMISSION_MAPPING.get(
            self._reformat_request_path(request_path), {}
        ).get(http_method, {}).get(VALIDATION)

        if not validation:
            _LOG.warning(f'Validator is not found for endpoint: '
                         f'{http_method}, {request_path}. Skipping...')
            return body
        return validate_pydantic(
            model=validation,
            value=body
        ).dict(exclude_none=True)

    @staticmethod
    def _split_stage_and_resource(path: str) -> tuple:
        """/caas/account/region -> ("/caas", "/account/region")"""
        path = path.rstrip('/')
        path = path.lstrip('/')
        first_slash = path.index('/')
        return f'/{path[:first_slash]}', path[first_slash:]

    def _format_event(self, event: dict) -> None:
        """
        Unpacks query params and body in the root of the event.
        Event must contain:
        - httpMethod;
        - path;
        ..in order to retrieve a correct validator.
        Raises 400 in case body's json is invalid or validation has not passed
        """
        method, path = (event.get(PARAM_HTTP_METHOD),
                        event.get(PARAM_REQUEST_PATH))
        try:
            body = json.loads(event.pop('body', None) or '{}')
        except json.JSONDecodeError as e:
            return build_response(code=HTTPStatus.BAD_REQUEST.value,
                                  content=f'Invalid request body: \'{e}\'')
        if method == HTTPMethod.GET or method == HTTPMethod.HEAD:
            body.update(event.pop('queryStringParameters', None) or {})
        body.update(event.pop('pathParameters', None) or {})
        event.update(self._validate_event(path, method, body))

    @staticmethod
    def _pop_excessive_attributes(event: dict):
        """
        Removes the attributes we don't need from event
        """
        [event.pop(attr, None) for attr in UNNECESSARY_EVENT_PARAMS]

    @staticmethod
    def _reformat_request_path(request_path: str):
        """
        /hello -> /hello/
        hello/ -> /hello/
        hello -> /hello/
        """
        if not request_path.startswith('/'):
            request_path = '/' + request_path
        if not request_path.endswith('/'):
            request_path += '/'
        return request_path

    @staticmethod
    def _floor_request_path(request_path: str):
        """
        Given an ambiguously deep child-resource - safe to floor out the
        `name`, as such child-resource, does not have any siblings.
        Example: /path/{child+} -> /path/{child}
        :return: str
        """
        if '+' in request_path:
            index = request_path.index('+')
            request_path = request_path[:index] + request_path[index + 1:]
        return request_path
