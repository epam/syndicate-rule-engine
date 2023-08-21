from functools import cached_property
from typing import List, Type

from modular_sdk.commons.error_helper import RESPONSE_SERVICE_UNAVAILABLE_CODE

from helpers import (
    raise_error_response, RESPONSE_BAD_REQUEST_CODE, validate_params,
    build_response, PARAM_USER_ID, RESPONSE_OK_CODE, RESPONSE_CREATED,
    RESPONSE_INTERNAL_SERVER_ERROR, RESPONSE_RESOURCE_NOT_FOUND_CODE,
    CustodianException,
    batches, KeepValueGenerator)
from helpers.__version__ import __version__
from helpers.constants import PARAM_HTTP_METHOD, PARAM_REQUEST_PATH, \
    USER_CUSTOMER_ATTR, \
    DELETE_METHOD, POST_METHOD
from helpers.log_helper import get_logger
from helpers.system_customer import SYSTEM_CUSTOMER
from lambdas.custodian_api_handler.handlers import AbstractHandler, Mapping
from lambdas.custodian_api_handler.handlers.batch_result_handler import \
    BatchResultsHandler
from lambdas.custodian_api_handler.handlers.health_check_handler import \
    HealthCheckHandler
from lambdas.custodian_api_handler.handlers.job_handler import JobHandler
from lambdas.custodian_api_handler.handlers.metrics_status_handler import \
    MetricsStatusHandler
from services import SERVICE_PROVIDER
from services.abstract_api_handler_lambda import AbstractApiHandlerLambda
from services.abstract_lambda import (PARAM_ROLE)
from services.clients.batch import BatchClient
from services.clients.ecr import ECRClient
from services.environment_service import EnvironmentService
from services.event_processor_service import EventProcessorService
from services.event_service import EventService
from services.modular_service import ModularService
from services.rbac.iam_cache_service import CachedIamService
from services.ruleset_service import RulesetService
from services.scheduler_service import SchedulerService
from services.setting_service import SettingsService, \
    KEY_CURRENT_CUSTODIAN_CUSTOM_CORE_VERSION
from services.user_service import CognitoUserService

_LOG = get_logger('custodian-api-handler')

ACTION_PARAM_ERROR = 'There is no handler for the \'{endpoint}\' endpoint'

PARAM_USERNAME = 'username'
PARAM_PASSWORD = 'password'
PARAM_TENANTS = 'tenants'
PARAM_CUSTOMER = 'customer'
PARAM_VENDOR = 'vendor'
PARAM_EVENTS = 'events'

HTTP_METHOD_ERROR = 'The server does not support the HTTP method {method} ' \
                    'for the resource {resource}'

UNABLE_TO_START_SCAN_ERROR_MESSAGE = \
    'Cannot start a scan: custodian custom core is outdated and cannot be' \
    ' updated to version to the latest version \'{0}\'.'


class ApiHandler(AbstractApiHandlerLambda):

    def validate_request(self, event) -> dict:
        """No request validation needed."""
        pass

    def __init__(self, batch_client, modular_service,
                 environment_service, user_service,
                 cached_iam_service, settings_service,
                 ecr_client, event_service,
                 event_processor_service,
                 ruleset_service, scheduler_service):
        self.batch_client: BatchClient = batch_client
        self.modular_service: ModularService = modular_service
        self.environment_service: EnvironmentService = environment_service
        self.user_service: CognitoUserService = user_service
        self.cached_iam_service: CachedIamService = cached_iam_service
        self.settings_service: SettingsService = settings_service
        self.ecr_client: ECRClient = ecr_client
        self.event_service: EventService = event_service
        self.event_processor_service: EventProcessorService = \
            event_processor_service
        self.ruleset_service: RulesetService = ruleset_service
        self.scheduler_service: SchedulerService = scheduler_service

    @cached_property
    def additional_handlers(self) -> List[Type[AbstractHandler]]:
        return [
            BatchResultsHandler,
            JobHandler,
            HealthCheckHandler,
            MetricsStatusHandler
        ]

    @cached_property
    def mapping(self) -> Mapping:
        # todo use only additional handlers
        res = {
            '/users': {
                DELETE_METHOD: self.user_delete
            },
            '/users/password-reset': {
                POST_METHOD: self.user_reset_password
            },
            '/signup': {
                POST_METHOD: self.signup_action
            },
            '/signin': {
                POST_METHOD: self.signin_action
            },
            '/event': {
                POST_METHOD: self.event_action
            }
        }
        for handler in self.additional_handlers:
            res.update(
                handler.build().mapping
            )
        return res

    def handle_request(self, event, context):
        request_path = event[PARAM_REQUEST_PATH]
        method_name = event[PARAM_HTTP_METHOD]
        handler_functions = self.mapping.get(request_path)
        if not handler_functions:
            raise_error_response(
                RESPONSE_BAD_REQUEST_CODE,
                ACTION_PARAM_ERROR.format(endpoint=request_path))
        handler_func = handler_functions.get(method_name)
        if not handler_func:
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE,
                content=HTTP_METHOD_ERROR.format(method=method_name,
                                                 resource=request_path))
        return handler_func(event=event)

    def signup_action(self, event):
        _LOG.debug('Going to signup user')
        username = event.get(PARAM_USERNAME)
        password = event.get(PARAM_PASSWORD)
        customer = event.get(PARAM_CUSTOMER)
        role = event.get(PARAM_ROLE)
        if not all([username, password, customer, role]):
            raise CustodianException(
                code=RESPONSE_BAD_REQUEST_CODE,
                content='You must specify all required parameters: username, '
                        'password, customer, role.')
        tenants = event.get(PARAM_TENANTS)
        if not self.modular_service.get_customer(customer):
            raise CustodianException(
                code=RESPONSE_BAD_REQUEST_CODE,
                content=f'Invalid customer: {customer}')
        _LOG.debug(f'Customer \'{customer}\' exists')
        if not self.cached_iam_service.get_role(customer, role):
            raise CustodianException(
                code=RESPONSE_BAD_REQUEST_CODE,
                content=f'Invalid role name: {role}')
        _LOG.debug(f'Role \'{role}\' exists')
        _LOG.info(f'Saving user: {username}:{customer}')
        self.user_service.save(username=username, password=password,
                               customer=customer, role=role, tenants=tenants)
        return build_response(
            code=RESPONSE_CREATED,
            content={PARAM_USERNAME: username, PARAM_CUSTOMER: customer,
                     PARAM_ROLE: role, PARAM_TENANTS: tenants})

    def signin_action(self, event):
        username = event.get(PARAM_USERNAME)
        password = event.get(PARAM_PASSWORD)
        if not username or not password:
            raise CustodianException(
                code=RESPONSE_BAD_REQUEST_CODE,
                content='You must specify both username and password')

        _LOG.info('Going to initiate the authentication flow')
        auth_result = self.user_service.initiate_auth(
            username=username,
            password=password)
        if not auth_result:
            raise CustodianException(
                code=RESPONSE_BAD_REQUEST_CODE,
                content='Incorrect username and/or password')

        _state = "contains" if auth_result.get(
            "ChallengeName") else "does not contain"
        _LOG.debug(f'Authentication initiation response '
                   f'{_state} the challenge')

        if auth_result.get('ChallengeName'):
            _LOG.debug(f'Responding to an authentication challenge '
                       f'{auth_result.get("ChallengeName")} ')
            auth_result = self.user_service.respond_to_auth_challenge(
                challenge_name=auth_result['ChallengeName'])
        refresh_token = auth_result['AuthenticationResult']['RefreshToken']
        id_token = auth_result['AuthenticationResult']['IdToken']

        return build_response(
            code=RESPONSE_OK_CODE,
            content={'id_token': id_token, 'refresh_token': refresh_token,
                     'api_version': __version__})

    def user_delete(self, event: dict) -> dict:
        _LOG.info('Delete user event')
        user_id = event[PARAM_USER_ID]
        if event[USER_CUSTOMER_ATTR] == SYSTEM_CUSTOMER:
            validate_params(event, [PARAM_USERNAME])
            _target_user_id = event[PARAM_USERNAME]
            if _target_user_id == user_id:
                return build_response('SYSTEM user cannot remove himself.')
            user_id = _target_user_id
        if not self.user_service.is_user_exists(user_id):
            return build_response(f'User \'{user_id}\' does not exist',
                                  code=RESPONSE_RESOURCE_NOT_FOUND_CODE)
        self.user_service.admin_delete_user(user_id)
        return build_response(content=f'User \'{user_id}\' has been deleted')

    def user_reset_password(self, event: dict) -> dict:
        _LOG.info('Reset user password user')
        validate_params(event, [PARAM_PASSWORD])
        user_id = event[PARAM_USER_ID]
        if (event[USER_CUSTOMER_ATTR] == SYSTEM_CUSTOMER and
                event.get(PARAM_USERNAME)):
            user_id = event.get(PARAM_USERNAME)
        if not self.user_service.is_user_exists(user_id):
            return build_response(f'User \'{user_id}\' does not exist',
                                  code=RESPONSE_RESOURCE_NOT_FOUND_CODE)
        self.user_service.set_password(user_id, event[PARAM_PASSWORD])
        return build_response(content=f'Password was reset for '
                                      f'user \'{user_id}\'')

    def event_action(self, event):
        # _LOG.debug(f'Event: {event}')  # it's too huge
        vendor = event[PARAM_VENDOR]
        events_in_item = self.environment_service. \
            number_of_native_events_in_event_item()

        _LOG.info('Initializing event processors')
        processor = self.event_processor_service.get_processor(vendor)
        processor.events = event[PARAM_EVENTS]
        n_received = processor.number_of_received()
        gen = KeepValueGenerator(
            processor.without_duplicates(processor.prepared_events())
        )
        entities = (
            self.event_service.create(events=batch, vendor=vendor)
            for batch in batches(gen, events_in_item)
        )
        self.event_service.batch_save(entities)

        return build_response(
            code=RESPONSE_CREATED, content={
                'received': n_received,
                'saved': gen.value
            }
        )

    def _validate_custom_core_version(self, required_ruleset_names: list,
                                      customer: str, current_version: str,
                                      cloud: str):
        # TODO rewrite
        rules = set()
        rules_with_newer_version = {}
        min_version = None
        rulesets = list(self.ruleset_service.list_customer_cloud_rulesets(
            customer=customer, cloud=cloud))
        for ruleset in rulesets:
            if ruleset.name in required_ruleset_names:
                rules.update(ruleset.rules)
        _LOG.debug(f'Retrieved following rulesets: {rulesets}')
        for rule in rules:
            rule_version = self.rules_service.get_rule_version(
                customer=customer, rule_id=rule)
            if current_version < rule_version:
                rules_with_newer_version.update({rule: rule_version})
            if not min_version or rule_version > min_version:
                min_version = rule_version
        if min_version == '-1' and not rules_with_newer_version:
            min_version = current_version
        _LOG.debug(f'Found {len(rules_with_newer_version)} rule(s) with newer '
                   f'version of custodian custom core: '
                   f'{rules_with_newer_version}')
        return rules_with_newer_version, min_version

    def _update_job_def(self, ccc_version):
        job_def_name = self.environment_service.get_batch_job_def()
        last_job_def = self.batch_client.get_job_definition_by_name(
            job_def_name=job_def_name)
        if not last_job_def:
            _LOG.error('Invalid configuration. Last job definition is empty, '
                       'cannot update job definition.')
            return build_response(
                code=RESPONSE_INTERNAL_SERVER_ERROR,
                content=UNABLE_TO_START_SCAN_ERROR_MESSAGE.format(ccc_version))
        last_job_def = last_job_def[0]
        image_url = self._get_image_url(version=ccc_version)
        self.batch_client.create_job_definition_from_existing_one(
            job_def=last_job_def, image_url=image_url)

        _LOG.debug('Overwriting current custodian custom core version setting')
        setting = self.settings_service.create(
            name=KEY_CURRENT_CUSTODIAN_CUSTOM_CORE_VERSION,
            value=ccc_version)
        self.settings_service.save(setting)

    def _get_image_url(self, version):
        image_folder_url = self.environment_service.get_image_folder_url()
        if not image_folder_url:
            _LOG.error('Missing property \'image_folder_url\'')
            return build_response(
                code=RESPONSE_INTERNAL_SERVER_ERROR,
                content=UNABLE_TO_START_SCAN_ERROR_MESSAGE.format(version))

        image_url = f'{image_folder_url}:{version}'
        _LOG.debug(f'Image URL: {image_url}. Checking image existence')
        is_image_exists = self.ecr_client.is_image_with_tag_exists(
            tag=version,
            repository_name=image_folder_url.split('/')[-1])
        if not is_image_exists:
            _LOG.error(f'Image with URL {image_url} does not exist. Unable '
                       f'to update job definition.')
            return build_response(
                code=RESPONSE_INTERNAL_SERVER_ERROR,
                content=UNABLE_TO_START_SCAN_ERROR_MESSAGE.format(version))
        return image_url

    def _update_ccc_version(self, target_rulesets, customer, cloud):
        current_ccc_version = self.settings_service.get_current_ccc_version()
        min_version = '0'
        if self.environment_service.get_feature_update_ccc_version():
            if not current_ccc_version:
                _LOG.error('Missing setting CURRENT_CCC_VERSION (current '
                           'custodian custom core version) in the CaaSSettings'
                           ' table')
                return build_response(
                    code=RESPONSE_SERVICE_UNAVAILABLE_CODE,
                    content='Application Service is not configured properly: '
                            'missing setting.')
            _LOG.debug(f'Current custom core version is '
                       f'\'{current_ccc_version}\'')
            rules_with_newer_ver, min_version = \
                self._validate_custom_core_version(
                    required_ruleset_names=target_rulesets,
                    customer=customer, current_version=current_ccc_version,
                    cloud=cloud)
            if rules_with_newer_ver:
                max_version = max(rules_with_newer_ver.values())
                _LOG.warning(f'Custom core is outdated. Need to update it to '
                             f'the version \'{max_version}\'. This rules '
                             f'require newer version: '
                             f'{", ".join(rules_with_newer_ver)}')
                self._update_job_def(max_version)
        min_version = '0' if not min_version else min_version
        return current_ccc_version, min_version


API_HANDLER = ApiHandler(
    batch_client=SERVICE_PROVIDER.batch(),
    modular_service=SERVICE_PROVIDER.modular_service(),
    environment_service=SERVICE_PROVIDER.environment_service(),
    user_service=SERVICE_PROVIDER.user_service(),
    cached_iam_service=SERVICE_PROVIDER.iam_cache_service(),
    settings_service=SERVICE_PROVIDER.settings_service(),
    ecr_client=SERVICE_PROVIDER.ecr(),
    event_service=SERVICE_PROVIDER.event_service(),
    event_processor_service=SERVICE_PROVIDER.event_processor_service(),
    ruleset_service=SERVICE_PROVIDER.ruleset_service(),
    scheduler_service=SERVICE_PROVIDER.scheduler_service()
)


def lambda_handler(event, context):
    return API_HANDLER.lambda_handler(event=event, context=context)


def skip_auth(event, context):
    return API_HANDLER.skip_auth(event=event, context=context)
