from abc import ABC, abstractmethod
from http import HTTPStatus
import inspect
import json
import msgspec
from typing import MutableMapping, TypedDict, cast, TYPE_CHECKING
from pynamodb.exceptions import PynamoDBConnectionError

from modular_sdk.commons.exception import ModularException
from modular_sdk.services.customer_service import CustomerService

from helpers import RequestContext, deep_get
from helpers.constants import CAASEnv, CustodianEndpoint, HTTPMethod, Permission
from helpers.lambda_response import (
    CustodianException,
    LambdaOutput,
    MetricsUpdateException,
    ResponseFactory,
)
from helpers.log_helper import get_logger, hide_secret_values
from helpers.system_customer import SYSTEM_CUSTOMER
from services.environment_service import EnvironmentService
from services.job_service import JobService
from services.batch_results_service import BatchResultsService
from services.platform_service import PlatformService
from services.license_service import LicenseService
from services import SP
from services.rbac_service import (
    PolicyService,
    PolicyStruct,
    RoleService,
    TenantAccess,
    TenantsAccessPayload,
)
if TYPE_CHECKING:
    from handlers import Mapping

_LOG = get_logger(__name__)


class AbstractEventProcessor(ABC):
    __slots__ = ()

    @abstractmethod
    def __call__(self, event: dict, context: RequestContext
                 ) -> tuple[dict, RequestContext]:
        """
        Returns somehow changed dict
        """


class ProcessedEvent(TypedDict):
    """
    This is a processed event that contains all kinds of useful information.
    By default, each handler receives only body (query if GET) as a single
    kwargs and all the path params as other kwargs.
    """
    method: HTTPMethod
    resource: CustodianEndpoint | None  # our resource if it can be matched: /jobs/{id}
    path: str  # real path without stage: /jobs/123 or /jobs/123/
    fullpath: str  # full real path with stage /dev/jobs/123
    cognito_username: str | None
    cognito_customer: str | None
    cognito_user_id: str | None
    cognito_user_role: str | None
    permission: Permission | None  # permissions for this endpoint
    is_system: bool
    body: dict  # maybe better str in order not to bind to json
    query: dict
    path_params: dict
    tenant_access_payload: TenantsAccessPayload
    additional_kwargs: dict  # additional kwargs to path to a handler
    headers: dict


class ExpandEnvironmentEventProcessor(AbstractEventProcessor):
    __slots__ = '_env',

    def __init__(self, environment_service: EnvironmentService):
        self._env = environment_service

    @classmethod
    def build(cls) -> 'ExpandEnvironmentEventProcessor':
        return cls(
            environment_service=SP.environment_service
        )

    @staticmethod
    def _resolve_stage(event: dict) -> str | None:
        # nginx reverse proxy gives this header. It also can contain query
        original = event.get('headers', {}).get('X-Original-Uri')
        path = event.get('path')
        if original and path:  # nginx reverse proxy gives this header
            # event['path'] here contains full path without stage
            try:
                return original[:original.index(path)].strip('/')
            except ValueError:
                pass
        # we could've got stage from requestContext.stage, but it always points
        # to api gw stage. That value if wrong for us in case we use a domain
        # name with prefix. So we should resolve stage as difference between
        # requestContext.path and requestContext.resourcePath
        path = deep_get(event, ('requestContext', 'path'))
        resource = deep_get(event, ('requestContext', 'resourcePath'))
        if path and resource:
            return path[:-len(resource)].strip('/')

    def __call__(self, event: dict, context: RequestContext
                 ) -> tuple[dict, RequestContext]:
        """
        Adds some useful data to internal environment variables
        """
        envs = {CAASEnv.INVOCATION_REQUEST_ID.value: context.aws_request_id}
        if host := deep_get(event, ('headers', 'Host')):
            _LOG.debug(f'Resolved host from header: {host}')
            envs[CAASEnv.API_GATEWAY_HOST.value] = host
        stage = self._resolve_stage(event)
        if stage:
            _LOG.debug(f'Resolved stage: {stage}')
            envs[CAASEnv.API_GATEWAY_STAGE.value] = stage

        if context.invoked_function_arn:
            _LOG.debug('Extracting account id from event context')
            envs[CAASEnv.ACCOUNT_ID.value] = RequestContext.extract_account_id(
                context.invoked_function_arn
            )
        self._env.override_environment(envs)
        return event, context


class ApiGatewayEventProcessor(AbstractEventProcessor):
    __slots__ = '_mapping',
    _decoder = msgspec.json.Decoder(type=dict)

    def __init__(self, mapping: dict[tuple[CustodianEndpoint, HTTPMethod], Permission | None]):
        """
        :param mapping: permissions mapping. Currently, we have one inside
        validators.registry
        """
        self._mapping = mapping

    @staticmethod
    def _is_from_step_function(event: dict) -> bool:
        """
        Standard api gateway event does not contain `execution_job_id`.
        Historically we can have a situation when the lambda is invoked by
        step function with an event which mocks api gateway's event but still
        is a bit different.
        :param event:
        :return:
        """
        return 'execution_job_id' in event

    @staticmethod
    def _prepare_step_function_event(event: dict):
        """
        Such an event has `execution_job_id` and `attempt` in his root. Also,
        its body is already an object, not a string. It does not have `path`
        :param event:
        :return:
        """
        assert isinstance(event.get('body'), dict), 'A bug found'
        event['body']['execution_job_id'] = event.pop('execution_job_id', None)
        event['body']['attempt'] = event.pop('attempt', 0)
        p = event['requestContext']['path']
        # removes stage
        event['path'] = '/' + p.strip('/').split('/', maxsplit=1)[-1]

    def __call__(self, event: dict, context: RequestContext
                 ) -> tuple[ProcessedEvent, RequestContext]:
        """
        Accepts API GW lambda proxy integration event
        """
        if self._is_from_step_function(event):
            _LOG.debug('Event came from step function. Preprocessing')
            self._prepare_step_function_event(event)

        body = event.get('body') or '{}'
        if isinstance(body, str):
            try:
                body = self._decoder.decode(body)
            except msgspec.ValidationError as e:
                _LOG.warning('Invalid body type came. Returning 400')
                raise ResponseFactory(HTTPStatus.BAD_REQUEST).message(
                    str(e)
                ).exc()
            except msgspec.DecodeError as e:
                _LOG.warning('Invalid incoming json. Returning 400')
                raise ResponseFactory(HTTPStatus.BAD_REQUEST).message(
                    str(e)
                ).exc()
        rc = event.get('requestContext') or {}
        return {
            'method': (method := HTTPMethod(event['httpMethod'])),
            'resource': (res := CustodianEndpoint.match(rc['resourcePath'])),
            'path': event['path'],  # todo may be wrong if we use custom domain
            'fullpath': rc['path'],
            'cognito_username': deep_get(rc, ('authorizer', 'claims',
                                              'cognito:username')),
            'cognito_customer': (cst := deep_get(rc, ('authorizer', 'claims',
                                                      'custom:customer'))),
            'cognito_user_id': deep_get(rc, ('authorizer', 'claims', 'sub')),
            'cognito_user_role': deep_get(rc, ('authorizer', 'claims',
                                               'custom:role')),
            'permission': self._mapping.get((res, method)),
            'is_system': cst == SYSTEM_CUSTOMER,
            'body': body,
            'query': dict(event.get('queryStringParameters') or {}),
            'path_params': dict(event.get('pathParameters') or {}),
            'tenant_access_payload': TenantsAccessPayload.build_denying_all(),
            'additional_kwargs': dict(),
            'headers': event['headers']
        }, context


class RestrictCustomerEventProcessor(AbstractEventProcessor):
    """
    Each user has its own customer but a system user should be able to
    perform actions on behalf of any customer. Every request model has
    customer_id attribute that is used by handlers to manage entities of
    that customer. This processor inserts user's customer to each event body.
    Allows to provide customer_id only for system users
    """
    __slots__ = '_cs',

    # TODO organize this collection somehow else
    can_work_without_customer_id = {
        (CustodianEndpoint.CUSTOMERS, HTTPMethod.GET),

        (CustodianEndpoint.METRICS_UPDATE, HTTPMethod.POST),
        (CustodianEndpoint.METRICS_STATUS, HTTPMethod.GET),

        (CustodianEndpoint.ED_RULESETS, HTTPMethod.GET),
        (CustodianEndpoint.ED_RULESETS, HTTPMethod.POST),
        (CustodianEndpoint.ED_RULESETS, HTTPMethod.DELETE),
        (CustodianEndpoint.RULESETS, HTTPMethod.GET),
        (CustodianEndpoint.RULESETS, HTTPMethod.POST),
        (CustodianEndpoint.RULESETS, HTTPMethod.PATCH),
        (CustodianEndpoint.RULESETS, HTTPMethod.DELETE),
        (CustodianEndpoint.RULESETS_RELEASE, HTTPMethod.POST),

        (CustodianEndpoint.RULE_SOURCES_ID, HTTPMethod.GET),
        (CustodianEndpoint.RULE_SOURCES, HTTPMethod.GET),
        (CustodianEndpoint.RULE_SOURCES, HTTPMethod.POST),
        (CustodianEndpoint.RULE_SOURCES_ID, HTTPMethod.DELETE),
        (CustodianEndpoint.RULE_SOURCES_ID, HTTPMethod.PATCH),
        (CustodianEndpoint.RULE_SOURCES_ID_SYNC, HTTPMethod.POST),

        (CustodianEndpoint.RULES, HTTPMethod.GET),
        (CustodianEndpoint.RULES, HTTPMethod.DELETE),
        (CustodianEndpoint.RULE_META_UPDATER, HTTPMethod.POST),

        (CustodianEndpoint.LICENSES_LICENSE_KEY_SYNC, HTTPMethod.POST),

        (CustodianEndpoint.SETTINGS_MAIL, HTTPMethod.GET),
        (CustodianEndpoint.SETTINGS_MAIL, HTTPMethod.POST),
        (CustodianEndpoint.SETTINGS_MAIL, HTTPMethod.DELETE),
        (CustodianEndpoint.SETTINGS_SEND_REPORTS, HTTPMethod.POST),
        (CustodianEndpoint.SETTINGS_LICENSE_MANAGER_CLIENT, HTTPMethod.POST),
        (CustodianEndpoint.SETTINGS_LICENSE_MANAGER_CLIENT, HTTPMethod.GET),
        (CustodianEndpoint.SETTINGS_LICENSE_MANAGER_CLIENT, HTTPMethod.DELETE),
        (CustodianEndpoint.SETTINGS_LICENSE_MANAGER_CONFIG, HTTPMethod.POST),
        (CustodianEndpoint.SETTINGS_LICENSE_MANAGER_CONFIG, HTTPMethod.GET),
        (CustodianEndpoint.SETTINGS_LICENSE_MANAGER_CONFIG, HTTPMethod.DELETE),

        (CustodianEndpoint.EVENT, HTTPMethod.POST),

        (CustodianEndpoint.USERS_WHOAMI, HTTPMethod.GET),
        (CustodianEndpoint.USERS_RESET_PASSWORD, HTTPMethod.POST),
        (CustodianEndpoint.USERS, HTTPMethod.GET),
        (CustodianEndpoint.USERS_USERNAME, HTTPMethod.PATCH),
        (CustodianEndpoint.USERS_USERNAME, HTTPMethod.DELETE),
        (CustodianEndpoint.USERS_USERNAME, HTTPMethod.GET),

        (CustodianEndpoint.SCHEDULED_JOB, HTTPMethod.GET),
        (CustodianEndpoint.SCHEDULED_JOB_NAME, HTTPMethod.GET)
    }

    def __init__(self, customer_service: CustomerService):
        self._cs = customer_service

    @classmethod
    def build(cls) -> 'RestrictCustomerEventProcessor':
        return cls(
            customer_service=SP.modular_client.customer_service()
        )

    @staticmethod
    def _get_cid(event: ProcessedEvent) -> str | None:
        one = event['query'].get('customer_id')
        two = event['query'].get('customer')
        three = event['body'].get('customer_id')
        four = event['body'].get('customer')
        return one or two or three or four

    def __call__(self, event: ProcessedEvent, context: RequestContext
                 ) -> tuple[ProcessedEvent, RequestContext]:
        if not event['cognito_username']:
            _LOG.info('Event does not contain username. No auth')
            # endpoint without auth
            return event, context
        if event['is_system']:
            # if system user is making a request he should provide customer_id
            # as a parameter to make a request on that customer's behalf.
            cid = self._get_cid(event)
            if cid and cid != SYSTEM_CUSTOMER and not self._cs.get(cid):
                raise ResponseFactory(HTTPStatus.BAD_REQUEST).message(
                    f'Customer {cid} does not exist. You cannot make a request'
                    f' on his behalf'
                ).exc()
            event['tenant_access_payload'] = TenantsAccessPayload.build_allowing_all()

            if (event['resource'], event['method']) in self.can_work_without_customer_id:  # noqa
                _LOG.info(f'System is making request that can be done without '
                          f'customer_id')
                return event, context
            if not cid:
                raise ResponseFactory(HTTPStatus.BAD_REQUEST).message(
                    'Please, provide customer_id param to make a request on '
                    'his behalf'
                ).exc()
            _LOG.info(f'System is making request on behalf of {cid}')
            return event, context
        # override customer attribute for standard users with their customer
        cust = event['cognito_customer']
        match event['method']:
            case HTTPMethod.GET:
                event['query']['customer_id'] = cust
            case _:
                event['body']['customer_id'] = cust
        return event, context


class CheckPermissionEventProcessor(AbstractEventProcessor):
    """
    Processor that restricts rbac permission
    """
    __slots__ = '_rs', '_ps', '_env'

    def __init__(self, role_service: RoleService,
                 policy_service: PolicyService,
                 environment_service: EnvironmentService):
        self._rs = role_service
        self._ps = policy_service
        self._env = environment_service

    @classmethod
    def build(cls) -> 'CheckPermissionEventProcessor':
        return cls(
            role_service=SP.role_service,
            policy_service=SP.policy_service,
            environment_service=SP.environment_service
        )

    @staticmethod
    def _not_allowed_message(permission: Permission) -> str:
        if permission.depends_on_tenant:
            return (f'Permission \'{permission.value}\' is not allowed '
                    f'for any tenant by your user role')
        else:
            return (f'Permission \'{permission.value}\' is not allowed '
                    f'by your user role')

    def _check_permission(self, customer: str, role_name: str,
                          permission: Permission) -> TenantsAccessPayload:
        """
        Checks users role in order to make immediate 403 in case the permission
        is not allowed. This method raises 403 in case the permission is not
        allowed.
        :param customer:
        :param role_name:
        :param permission:
        :return: TenantAccessPayload
        """
        _LOG.info(f'Checking permission: {permission}')
        factory = ResponseFactory(HTTPStatus.FORBIDDEN).message
        # todo cache role and policies?
        role = self._rs.get_nullable(customer, role_name)
        if not role:
            raise factory('Your user role was removed').exc()
        if role.is_expired():
            raise factory('Your user role has expired').exc()

        it = map(PolicyStruct.from_model, self._ps.iter_role_policies(role))
        ta = TenantAccess()
        is_allowed = False
        for policy in it:
            _LOG.info(f'Checking permission for policy: {policy.name}')
            if policy.forbids(permission):
                _LOG.debug('Policy explicitly forbids')
                raise factory(self._not_allowed_message(permission)).exc()
            is_allowed |= policy.allows(permission)
            ta.add(policy)
        if not is_allowed:
            raise factory(self._not_allowed_message(permission)).exc()
        return ta.resolve_payload(permission)

    def __call__(self, event: ProcessedEvent, context: RequestContext
                 ) -> tuple[ProcessedEvent, RequestContext]:
        if event['is_system']:  # do not check any permissions for system
            return event, context
        username = event['cognito_username']
        if not username:
            # no username -> means no auth on endpoint. No permission to check
            return event, context
        if not event['resource']:
            # will be aborted with 404
            _LOG.warning('A request for not known resource')
            return event, context
        permission = event['permission']
        if not permission:
            _LOG.info('No permission exist for endpoint, allowing')
            return event, context
        if not self._env.allow_disabled_permissions() and permission.is_disabled:
            raise ResponseFactory(HTTPStatus.FORBIDDEN).message(
                'Action is allowed only for system user'
            ).exc()

        # if cognito_username exists, cognito_customer & cognito_user_role
        # exist as well
        event['tenant_access_payload'] = self._check_permission(
            customer=cast(str, event['cognito_customer']),
            role_name=cast(str, event['cognito_user_role']),
            permission=permission
        )
        _LOG.debug(f'Resolved tenant access payload: '
                   f'{event["tenant_access_payload"]}')
        return event, context


class RestrictTenantEventProcessor(AbstractEventProcessor):
    """
    Restricting endpoints based on tenant permissions is kind of a
    difficult task. In general, it would mean that we should adjust each
    single endpoint so that it could do its logic taking allowed tenants
    into consideration. We can do that but that is a lot of work and repeating.
    This processor will intercept some common cases like endpoints with
    {tenant_name} or {job_id} in path and will check tenant permissions
    immediately. Additionally, it will insert some data into
    event['additional_kwargs']. I know it all seems quite confusing.
    Behold example:
    imagine we have and an endpoint `GET /jobs/{job_id}`. It describes a
    job by the given job_id. Each job belongs to a specific tenant so if I
    make say this request: `GET /jobs/1` and the job with id `1` belongs to
    the tenant I have no rights to access - I should receive 403. More or less
    the same logic can be applied to a lot of other endpoints ({tenant_name},
    {platform_id}, etc.).
    So, this method will catch such endpoints, will query the request item
    (job for the example above) if necessary and perform restriction here.
    Then it will add this item to
    additional_kwargs so that it could be used in the actual handler
    without querying again.
    Although it will cover the majority of cases we still need to implement
    custom restriction for some specific endpoints. For example `GET /jobs`
    should query jobs that belong to tenants a user have access to
    """
    __slots__ = '_js', '_brs', '_ps', '_ls'

    def __init__(self, job_service: JobService,
                 batch_results_service: BatchResultsService,
                 platform_service: PlatformService,
                 license_service: LicenseService):
        self._js = job_service
        self._brs = batch_results_service
        self._ps = platform_service
        self._ls = license_service

    @classmethod
    def build(cls) -> 'RestrictTenantEventProcessor':
        return cls(
            job_service=SP.job_service,
            batch_results_service=SP.batch_results_service,
            platform_service=SP.platform_service,
            license_service=SP.license_service
        )

    def _restrict_tenant_name(self, event: ProcessedEvent,
                              context: RequestContext
                              ) -> tuple[ProcessedEvent, RequestContext]:
        tenant_name = cast(str, event['path_params'].get('tenant_name'))
        if not event['tenant_access_payload'].is_allowed_for(tenant_name):
            raise ResponseFactory(HTTPStatus.FORBIDDEN).message(
                f'The request tenant \'{tenant_name}\' is not found'
            ).exc()
        return event, context

    def _restrict_job_id(self, event: ProcessedEvent, context: RequestContext
                         ) -> tuple[ProcessedEvent, RequestContext]:
        job_id = cast(str, event['path_params'].get('job_id'))
        job = self._js.get_nullable(hash_key=job_id)
        if not job or not event['tenant_access_payload'].is_allowed_for(job.tenant_name):
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                self._js.not_found_message(job_id)
            ).exc()
        event['additional_kwargs']['job_obj'] = job
        return event, context

    def _restrict_batch_results(self, event: ProcessedEvent,
                                context: RequestContext
                                ) -> tuple[ProcessedEvent, RequestContext]:
        br_id = cast(str, event['path_params'].get('batch_results_id'))
        job = self._brs.get_nullable(hash_key=br_id)
        if not job or not event['tenant_access_payload'].is_allowed_for(job.tenant_name):
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                self._brs.not_found_message(br_id)
            ).exc()
        event['additional_kwargs']['br_obj'] = job
        return event, context

    def _restrict_platform_id(self, event: ProcessedEvent,
                              context: RequestContext
                              ) -> tuple[ProcessedEvent, RequestContext]:
        platform_id = cast(str, event['path_params'].get('platform_id'))
        platform = self._ps.get_nullable(hash_key=platform_id)
        if not platform or not event['tenant_access_payload'].is_allowed_for(platform.tenant_name):
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                self._ps.not_found_message(platform_id)
            ).exc()
        event['additional_kwargs']['platform_obj'] = platform
        return event, context

    def _restrict_license_key(self, event: ProcessedEvent,
                              context: RequestContext
                              ) -> tuple[ProcessedEvent, RequestContext]:
        # todo check if license is applicable at least for one allowed tenant
        return event, context

    def __call__(self, event: ProcessedEvent, context: RequestContext
                 ) -> tuple[ProcessedEvent, RequestContext]:
        res = event['resource']
        perm = event['permission']
        if not res or not perm or not perm.depends_on_tenant:
            return event, context

        if '{tenant_name}' in res:
            return self._restrict_tenant_name(event, context)
        if '{job_id}' in res:
            return self._restrict_job_id(event, context)
        if '{batch_results_id}' in res:
            return self._restrict_batch_results(event, context)
        if '{platform_id}' in res:
            return self._restrict_platform_id(event, context)
        if '{license_key}' in res:
            return self._restrict_license_key(event, context)
        return event, context


class AbstractLambdaHandler(ABC):
    @abstractmethod
    def handle_request(self, event: MutableMapping, context: RequestContext
                       ) -> LambdaOutput:
        """
        Should be implemented. May raise TelegramBotException or any
        other kind of exception
        """

    @abstractmethod
    def lambda_handler(self, event: dict, context: RequestContext
                       ) -> LambdaOutput:
        """
        Main lambda's method that is executed
        """


class EventProcessorLambdaHandler(AbstractLambdaHandler):
    processors: tuple[AbstractEventProcessor, ...] = ()

    def _process_event(self, event: dict, context: RequestContext
                       ) -> tuple[dict, RequestContext]:
        for processor in self.processors:
            _LOG.debug(f'Processing event: {processor.__class__.__name__}')
            event, context = processor(event, context)
        return event, context

    @abstractmethod
    def handle_request(self, event: MutableMapping,
                       context: RequestContext) -> LambdaOutput:
        ...

    def lambda_handler(self, event: dict, context: RequestContext
                       ) -> LambdaOutput:
        _LOG.info(f'Starting request: {context.aws_request_id}')
        # This is the only place where we print the event. Do not print it
        # somewhere else
        _LOG.debug('Incoming event')
        _LOG.debug(json.dumps(hide_secret_values(event)))
        try:
            processed, context = self._process_event(event, context)
            return self.handle_request(event=processed, context=context)
        except MetricsUpdateException as e:
            # todo 5.0.0 refactor metrics-updater lambda to remove this except
            #  from here
            _LOG.warning('Metrics update exception occurred', exc_info=True)
            raise e  # needed for ModularJobs to track failed jobs
        except CustodianException as e:
            _LOG.warning(f'Application exception occurred: {e}')
            return e.build()
        except ModularException as e:
            _LOG.warning('Modular exception occurred', exc_info=True)
            return ResponseFactory(int(e.code)).message(e.content).build()
        except PynamoDBConnectionError as e:
            if e.cause_response_code not in (
                    'ProvisionedThroughputExceededException',
                    'ThrottlingException'):
                _LOG.exception('Unexpected pynamodb exception occurred')
                return ResponseFactory(
                    HTTPStatus.INTERNAL_SERVER_ERROR
                ).default().build()
            _LOG.exception(f'{e.cause_response_code} occurred. Returning 429')
            return ResponseFactory(
                HTTPStatus.TOO_MANY_REQUESTS
            ).default().build()
        except Exception:  # noqa
            _LOG.exception('Unexpected exception occurred')
            return ResponseFactory(
                HTTPStatus.INTERNAL_SERVER_ERROR
            ).default().build()


class ApiEventProcessorLambdaHandler(EventProcessorLambdaHandler):
    mapping: 'Mapping'

    def handle_request(self, event: ProcessedEvent,
                       context: RequestContext) -> LambdaOutput:
        """
        It resolves handler from the mapping using method and resource. It
        always passes query or body inside `event` kwargs (depending on
        method). Also, if a handler has some specific reserved kwargs this
        method will add additional values. Also, all `additional_kwargs` from
        event will be passes to handlers if those have that kwargs
        :param event:
        :param context:
        :return:
        """
        handler = self.mapping.get(event['resource'], {}).get(event['method'])
        if not handler:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).default().exc()
        match event['method']:
            case HTTPMethod.GET:
                body = event['query']
            case _:
                body = event['body']
        params = dict(event=body, **event['path_params'])
        parameters = inspect.signature(handler).parameters
        if '_pe' in parameters:
            # pe - Processed Event: in case we need to access some raw data
            # inside a handler.
            _LOG.debug('Expanding handler payload with raw event')
            params['_pe'] = event
        if '_tap' in parameters:
            # _tap - in case we need to know what tenants are allowed
            # inside a specific handler
            _LOG.debug('Expanding handler payload with tenant access data')
            params['_tap'] = event['tenant_access_payload']
        additional = event['additional_kwargs']
        for p in parameters:
            if p in additional:
                _LOG.debug(f'Expanding handler payload with {p}')
                params[p] = additional[p]
        return handler(**params)
