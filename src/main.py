"""
On-prem entering point. All the imports are inside functions to make the
helps fast and be safe from importing not existing packages
"""
import argparse
import json
import logging
import logging.config
import multiprocessing
import os
import secrets
import string
from abc import ABC, abstractmethod
from datetime import timedelta, datetime
from functools import cached_property
from pathlib import Path
from typing import Callable, Optional, Dict, Set, Union, Tuple, List, TypedDict

import boto3
from bottle import Bottle
from dateutil.relativedelta import relativedelta, SU
from dotenv import load_dotenv
from modular_sdk.models.customer import Customer

from exported_module.scripts.parse_rule_source import \
    main as parse_rule_source, \
    init_parser as init_parser_rule_source_cli_parser
from exported_module.scripts.rules_table_generator import \
    main as generate_rules_table, \
    init_parser as init_rules_table_generator_cli_parser
from helpers.time_helper import utc_iso, utc_datetime
from services import SERVICE_PROVIDER
from services.clients.xlsx_standard_parser import \
    main as parse_xlsx_standard, init_parser as init_xlsx_cli_parser

SRC = Path(__file__).parent.resolve()
ROOT = SRC.parent.resolve()

DEPLOYMENT_RESOURCES_FILENAME = 'deployment_resources.json'

ACTION_DEST = 'action'
ENV_ACTION_DEST = 'env_action'
ALL_NESTING: Tuple[str, ...] = (ACTION_DEST, ENV_ACTION_DEST)  # important

RUN_ACTION = 'run'
CREATE_INDEXES_ACTION = 'create_indexes'
CREATE_BUCKETS_ACTION = 'create_buckets'
INIT_VAULT_ACTION = 'init_vault'
UPDATE_API_GATEWAY_MODELS_ACTION = 'update_api_models'
PARSE_XLSX_STANDARD_ACTION = 'parse_standards'
PARSE_RULE_SOURCE_ACTION = 'parse_rule_source'
GENERATE_RULES_TABLE_ACTION = 'generate_rules_table'
ENV_ACTION = 'env'

UPDATE_SETTINGS_ENV_ACTION = 'update_settings'
CREATE_SYSTEM_USER_ENV_ACTION = 'create_system_user'
CREATE_CUSTOMER_ACTION = 'create_customer'
CREATE_TENANT_ACTION = 'create_tenant'
CREATE_USER_ACTION = 'create_user'

DEFAULT_HOST = '0.0.0.0'
DEFAULT_PORT = 8000
DEFAULT_SCHEDULE_HOURS = 3
DEFAULT_NUMBER_OF_WORKERS = (multiprocessing.cpu_count() * 2) + 1
DEFAULT_ON_PREM_API_LINK = f'http://{DEFAULT_HOST}:{str(DEFAULT_PORT)}/caas'
DEFAULT_API_GATEWAY_NAME = 'custodian-as-a-service-api'

API_GATEWAY_LINK = 'https://{id}.execute-api.{region}.amazonaws.com/{stage}'

SYSTEM_ROLE, ADMIN_ROLE, USER_ROLE = 'system_role', 'admin_role', 'user_role'
SYSTEM_POLICY, ADMIN_POLICY, USER_POLICY = 'system_policy', 'admin_policy', \
    'user_policy'

C7N_CONFIGURE_COMMAND = 'c7n configure --api_link {api_link}'
C7N_LOGIN_COMMAND = 'c7n login --username {username} --password ' \
                    '\'{password}\''


def gen_password(digits: int = 20) -> str:
    allowed_punctuation = ''.join(set(string.punctuation) - {'"', "'", "!"})
    chars = string.ascii_letters + string.digits + allowed_punctuation
    while True:
        password = ''.join(secrets.choice(chars) for _ in range(digits)) + '='
        if (any(c.islower() for c in password)
                and any(c.isupper() for c in password)
                and sum(c.isdigit() for c in password) >= 3):
            break
    return password


def get_logger():
    config = {
        'version': 1,
        'disable_existing_loggers': True
    }
    logging.config.dictConfig(config)
    logger = logging.getLogger()
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(levelname)s - %(message)s'))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


_LOG = get_logger()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Custodian configuration cli entering point'
    )
    # -- top level sub-parser
    sub_parsers = parser.add_subparsers(dest=ACTION_DEST, required=True,
                                        help='Available actions')
    _ = sub_parsers.add_parser(
        CREATE_INDEXES_ACTION,
        help='Re-create MongoDB indexes'
    )
    _ = sub_parsers.add_parser(
        INIT_VAULT_ACTION,
        help='Enables secret engine and crated a necessary token in Vault'
    )
    _ = sub_parsers.add_parser(
        CREATE_BUCKETS_ACTION,
        help='Creates necessary buckets in Minio'
    )
    _ = sub_parsers.add_parser(
        UPDATE_API_GATEWAY_MODELS_ACTION,
        help='Regenerates API Gateway models from existing pydantic validators'
    )
    init_xlsx_cli_parser(sub_parsers.add_parser(
        PARSE_XLSX_STANDARD_ACTION,
        help='Parses Custom Core\'s xlsx with standards'
    ))
    init_parser_rule_source_cli_parser(sub_parsers.add_parser(
        PARSE_RULE_SOURCE_ACTION,
        help='Parses Rule source and extracts some data'
    ))
    init_rules_table_generator_cli_parser(sub_parsers.add_parser(
        GENERATE_RULES_TABLE_ACTION,
        help='Generates xlsx table with rules data from local dir with rules'
    ))
    parser_run = sub_parsers.add_parser(RUN_ACTION, help='Run on-prem server')
    parser_run.add_argument(
        '-g', '--gunicorn', action='store_true', default=False,
        help='Specify the flag is you want to run the server via Gunicorn')
    parser_run.add_argument(
        '-nw', '--workers', type=int, required=False,
        help='Number of gunicorn workers. Must be specified only '
             'if --gunicorn flag is set'
    )
    parser_run.add_argument('--host', default=DEFAULT_HOST, type=str,
                            help='IP address where to run the server')
    parser_run.add_argument('--port', default=DEFAULT_PORT, type=int,
                            help='IP Port to run the server on')
    parser_run.add_argument(
        '-hours', '--schedule_hours', default=DEFAULT_SCHEDULE_HOURS, type=int,
        help='License synchronization schedule (in hours). Default value: '
             '3 hours. To disable synchronization, set this value to 0.')
    # -------

    # env sub-action
    env_parser = sub_parsers.add_parser(
        ENV_ACTION, help='Sub-group to configure an existing env'
    )
    env_sub_parsers = env_parser.add_subparsers(
        dest=ENV_ACTION_DEST, required=True, help='Configure an existing env'
    )
    env_parser_update_settings = env_sub_parsers.add_parser(
        UPDATE_SETTINGS_ENV_ACTION, help='Actualizes existing settings'
    )
    env_parser_update_settings.add_argument(
        '--rulesets_bucket', type=str, required=False,
        help='Reports bucket name to put some settings\' data in. '
             'If not specified, value from env will be used'
    )
    env_parser_update_settings.add_argument(
        '--lm_api_link', type=str, required=False,
        help='Api link to Custodian license manager'
    )
    env_parser_create_system = env_sub_parsers.add_parser(
        CREATE_SYSTEM_USER_ENV_ACTION,
        help='Creates system role, policy and user in case they do not exist'
    )
    env_parser_create_system.add_argument(
        '--username', type=str, required=True,
        help='Username name of root admin'
    )
    env_parser_create_system.add_argument(
        '--api_link', type=str, required=False,
        help='Link to api of the server. If not specified, '
             'it will be resolved automatically'
    )
    env_parser_create_customer = env_sub_parsers.add_parser(
        CREATE_CUSTOMER_ACTION,
        help='Creates a standard customer, and its role and policy'
    )
    env_parser_create_customer.add_argument(
        '--customer_name', type=str, required=True, help='Customer\'s name')
    env_parser_create_customer.add_argument(
        '--admins', type=str, nargs='+', help='Customer\'s owner(s) emails')

    env_parser_create_user = env_sub_parsers.add_parser(
        CREATE_USER_ACTION,
        help='Creates user with for given entities'
    )
    env_parser_create_user.add_argument(
        '--username', type=str, required=True, help='The name of the user')
    env_parser_create_user.add_argument(
        '--customer_name', type=str, required=True,
        help='Customer name to create the user in')
    env_parser_create_user.add_argument(
        '--tenant_names', type=str, nargs='+',
        help='Tenants names to create the user for'
    )
    env_parser_create_user.add_argument(
        '--role_name', type=str, required=False, default=ADMIN_ROLE,
        help='Role name within a customer to give '
             'to the user (default: %(default)s)')
    return parser


class ActionHandler(ABC):

    @staticmethod
    def is_docker() -> bool:
        # such a kludge due to different envs that points to on-prem env in
        # LM and Modular
        lm_docker = SERVICE_PROVIDER.environment_service().is_docker()
        modular_docker = SERVICE_PROVIDER.modular_client().environment_service(). \
            is_docker()
        return lm_docker or modular_docker

    @abstractmethod
    def __call__(self, **kwargs):
        ...


class InitSubService(ActionHandler):
    """
    Dynamic. Just add a method which starts from prefix
    """

    def __init__(self, services: Set[str]):
        assert services.issubset(self.available_services)
        self._services = services

    @cached_property
    def prefix(self) -> str:
        return 'init_'

    @cached_property
    def available_services(self) -> Set[str]:
        return {
            attr[len(self.prefix):] for attr in dir(self)
            if attr.startswith(self.prefix) and callable(getattr(self, attr))
        }

    def get_method(self, name: str) -> Callable:
        """
        For scripting purposes this is exactly what I need
        """
        return getattr(self, self.prefix + name, lambda **kwargs: None)

    def init_vault(self, **kwargs):
        from exported_module.scripts.init_vault import init_vault as \
            _init_vault
        _init_vault()

    def init_minio(self, **kwargs):
        from exported_module.scripts.init_minio import init_minio as \
            _init_minio
        _init_minio()

    def init_mongo(self, **kwargs):
        from exported_module.scripts.init_mongo import init_mongo as \
            _init_mongo
        _init_mongo()

    def __call__(self, **kwargs):
        for service in self._services:
            method = self.get_method(service)
            _LOG.info(f'Initializing {service}')
            method(**kwargs)


class Run(ActionHandler):
    @staticmethod
    def make_app() -> Bottle:
        """For gunicorn"""
        from exported_module.api.deployment_resources_parser import \
            DeploymentResourcesParser
        from exported_module.api.app import DynamicAPI
        api = DynamicAPI(dr_parser=DeploymentResourcesParser(
            SRC / DEPLOYMENT_RESOURCES_FILENAME
        ))
        return api.app

    def __call__(self, host: str = DEFAULT_HOST, port: str = DEFAULT_PORT,
                 schedule_hours: int = DEFAULT_SCHEDULE_HOURS,
                 gunicorn: bool = False, workers: Optional[int] = None):
        if not gunicorn and workers:
            print(
                '--workers is ignored because it you are not running Gunicorn')

        from exported_module.api.license_sync import ensure_license_sync_job
        from helpers.constants import ENV_SERVICE_MODE, DOCKER_SERVICE_MODE
        if os.getenv(ENV_SERVICE_MODE) != DOCKER_SERVICE_MODE:
            print(f'Env \'{ENV_SERVICE_MODE}\' is not equal to '
                  f'\'{DOCKER_SERVICE_MODE}\' but You are executing the '
                  f'on-prem server. Setting '
                  f'{ENV_SERVICE_MODE}={DOCKER_SERVICE_MODE} forcefully')
            os.environ[ENV_SERVICE_MODE] = DOCKER_SERVICE_MODE

        app = self.make_app()
        SERVICE_PROVIDER.ap_job_scheduler().start()
        ensure_license_sync_job(schedule_hours)
        if gunicorn:
            workers = workers or DEFAULT_NUMBER_OF_WORKERS
            from exported_module.api.app_gunicorn import \
                CustodianGunicornApplication
            options = {
                'bind': f'{host}:{port}',
                'workers': workers,
            }
            CustodianGunicornApplication(app, options).run()
        else:
            app.run(host=host, port=port)


class UpdateApiGatewayModels(ActionHandler):

    @property
    def validators_module(self) -> str:
        return 'validators'

    @property
    def deployment_resources_file(self) -> Path:
        return SRC / self.validators_module / DEPLOYMENT_RESOURCES_FILENAME

    @property
    def custodian_api_gateway_name(self) -> str:
        return "custodian-as-a-service-api"

    @property
    def custodian_api_definition(self) -> dict:
        return {
            self.custodian_api_gateway_name: {
                "resource_type": "api_gateway",
                "dependencies": [],
                "resources": {},
                "models": {}
            }
        }

    def __call__(self, **kwargs):
        from validators.request_validation import ALL_MODELS_WITHOUT_GET
        from validators.response_validation import ALL_MODELS
        api_def = self.custodian_api_definition
        for model in set(ALL_MODELS_WITHOUT_GET) | set(ALL_MODELS):
            api_def[self.custodian_api_gateway_name]['models'].update({
                model.__name__: {
                    "content_type": "application/json",
                    "schema": model.schema()
                }
            })

        with open(self.deployment_resources_file, 'w') as file:
            json.dump(api_def, file, indent=2)
        _LOG.info(f'{self.deployment_resources_file} has been updated')


class UpdateSettings(ActionHandler):

    @cached_property
    def temp_dir(self) -> Path:
        return Path.cwd() / '.tmp'

    def read_temp_json_file(self, filename: str) -> Optional[dict]:
        """
        By default, such scripts ad parse_xlsx_standard, parse_rule_source
        generate their meta to ./.tmp. This method reads from there
        """
        file = self.temp_dir / filename
        if not file.exists():
            return
        with open(file, 'r') as fp:
            try:
                return json.load(fp)
            except json.JSONDecodeError:
                return

    @cached_property
    def access_data_lm(self) -> dict:
        from services.setting_service import KEY_ACCESS_DATA_LM
        return {
            "name": KEY_ACCESS_DATA_LM,
            "value": {
                "host": None,
                "port": None,
                "version": "1"
            }
        }

    @cached_property
    def current_ccc_version(self) -> dict:
        from services.setting_service import \
            KEY_CURRENT_CUSTODIAN_CUSTOM_CORE_VERSION
        return {
            "name": KEY_CURRENT_CUSTODIAN_CUSTOM_CORE_VERSION,
            "value": "0.9.8.20211013_030000"
        }

    @cached_property
    def system_customer_name(self) -> dict:
        from services.setting_service import KEY_SYSTEM_CUSTOMER
        return {
            "name": KEY_SYSTEM_CUSTOMER,
            "value": "CUSTODIAN_SYSTEM"
        }

    @cached_property
    def report_date_marker(self) -> dict:
        from services.setting_service import KEY_REPORT_DATE_MARKER
        return {
            "name": KEY_REPORT_DATE_MARKER,
            "value": {
                "last_week_date": (datetime.today() + relativedelta(
                    weekday=SU(-1))).date().isoformat(),
                "current_week_date": (datetime.today() + relativedelta(
                    weekday=SU(0))).date().isoformat()
            }
        }

    def set_access_data_lm_setting(self, lm_api_link, lm_api_port=443):
        from models.setting import Setting
        from modular_sdk.services.impl.maestro_credentials_service import \
            AccessMeta
        setting = self.access_data_lm
        model = AccessMeta.from_dict({})
        model.update_host(host=lm_api_link)
        setting['value'] = model.dict()
        Setting(**setting).save()

    def set_current_ccc_version(self):
        from models.setting import Setting
        Setting(**self.current_ccc_version).save()

    def set_system_customer_name_setting(self):
        from models.setting import Setting
        Setting(**self.system_customer_name).save()

    def set_report_date_marker_setting(self):
        from models.setting import Setting
        Setting(**self.report_date_marker).save()

    def __call__(self, rulesets_bucket: Optional[str] = None,
                 lm_api_link: Optional[str] = None):
        if lm_api_link:
            _LOG.info('LM API link was given. Setting lm access data')
            self.set_access_data_lm_setting(lm_api_link)
        _LOG.info('Setting current Custodian custom core version')
        self.set_current_ccc_version()
        _LOG.info('Setting CloudTrail resources mapping')
        _LOG.info('Setting system customer name')
        self.set_system_customer_name_setting()
        _LOG.info('Setting report date marker')
        self.set_report_date_marker_setting()


class EntitiesRelatedActions:
    class RoleItem(TypedDict):
        customer: Optional[str]
        expiration: Optional[str]
        name: Optional[str]
        policies: List[str]
        # resource: List[str]  # not used

    class PolicyItem(TypedDict):
        customer: Optional[str]
        name: Optional[str]
        permissions: List

    @property
    def blank_role(self) -> RoleItem:
        return {
            "customer": None,
            "expiration": None,
            "name": None,
            "policies": [],
            # "resource": ["*"]
        }

    @property
    def blank_policy(self) -> PolicyItem:
        return {
            "customer": None,
            "name": None,
            "permissions": []
        }

    @staticmethod
    def create_customer(customer_name: str,
                        admins: Optional[List[str]] = None):
        """
        Creates a customer with given params. Is the customer already exists,
        the creation will be skipped, no attributes will be changed.
        """
        _service = SERVICE_PROVIDER.modular_client().customer_service()
        if _service.get(customer_name):
            _LOG.warning(f"\'{customer_name}'\' customer already "
                         f"exists. His attributes won`t be changed")
            return
        customer = Customer(
            name=customer_name,
            display_name=customer_name.title().replace('_', ' '),
            admins=admins or []
        )
        customer.save()
        _LOG.info(f'Customer "{customer_name}" created..')

    def create_policy(self, customer_name: str, policy_name: str,
                      permissions: list):
        from models.policy import Policy
        policy = self.blank_policy
        policy['customer'] = customer_name
        policy['name'] = policy_name
        policy['permissions'].extend(permissions)
        Policy(**policy).save()
        _LOG.info(f'Policy "{policy_name}" was created')

    def create_role(self, customer_name: str, role_name: str,
                    policy_names: list):
        from models.role import Role
        role = self.blank_role

        role['customer'] = customer_name
        role['name'] = role_name
        role['policies'].extend(policy_names)
        role['expiration'] = utc_iso(utc_datetime() + timedelta(days=6 * 30))
        Role(**role).save()
        _LOG.info(f'Role "{role_name}" was created')

    @staticmethod
    def create_user(username: str, customer_name: str, role_name: str,
                    tenants: Optional[List[str]] = None
                    ) -> Tuple[str, Optional[str]]:
        user_service = SERVICE_PROVIDER.user_service()

        if user_service.is_user_exists(username):
            _LOG.warning(f'User with username {username} already exists. '
                         f'Skipping...')
            return username, None

        password = gen_password()

        user_service.save(username=username, password=password,
                          customer=customer_name, role=role_name,
                          tenants=tenants)
        _LOG.info(f'User \'{username}\' with customer \'{customer_name}\' '
                  f'was created')
        return username, password


class CreateSystemUser(EntitiesRelatedActions, ActionHandler):

    def create_system_user(self, username: str, role_name: str
                           ) -> Tuple[Optional[str], Optional[str]]:
        from helpers.system_customer import SYSTEM_CUSTOMER
        user_service = SERVICE_PROVIDER.user_service()
        if user_service.is_system_user_exists():
            system_username = user_service.get_system_user()
            _LOG.warning(f'System user already exists. '
                         f'It\'s name: \'{system_username}\'. Skipping...')
            return None, None
        else:
            return self.create_user(
                username=username,
                customer_name=SYSTEM_CUSTOMER,
                role_name=role_name
            )

    def resolve_api_link(self,
                         api_name: Optional[str] = DEFAULT_API_GATEWAY_NAME,
                         ) -> Optional[str]:
        if self.is_docker():
            _LOG.warning('Not going to try to resolve api link on on-prem. '
                         'Default will do')
            return DEFAULT_ON_PREM_API_LINK
        client = boto3.client(service_name='apigateway')
        rest_apis = client.get_rest_apis()
        api = next(
            (api for api in rest_apis.get('items') if api['name'] == api_name),
            None
        )
        if not api:
            _LOG.warning('Could not resolve api link')
            return
        rest_api_id = api['id']
        stages = client.get_stages(restApiId=rest_api_id).get('item')
        stage = ''
        if len(stages) == 0:
            _LOG.warning('Api gateway has no stages')
        else:
            stage = stages[0]['stageName']
        return API_GATEWAY_LINK.format(
            id=rest_api_id,
            region=client.meta.region_name,
            stage=stage
        )

    def __call__(self, username: str, api_link: Optional[str] = None):
        from helpers.system_customer import SYSTEM_CUSTOMER
        access_control_service = SERVICE_PROVIDER.access_control_service()
        _LOG.info(f'Creating \'{SYSTEM_CUSTOMER}\' customer...')
        # self.create_customer(SYSTEM_CUSTOMER)

        _LOG.info('Creating policy...')
        self.create_policy(
            customer_name=SYSTEM_CUSTOMER,
            policy_name=SYSTEM_POLICY,
            permissions=list(access_control_service.all_permissions)
        )

        _LOG.info('Creating role...')
        self.create_role(
            customer_name=SYSTEM_CUSTOMER, role_name=SYSTEM_ROLE,
            policy_names=[SYSTEM_POLICY, ]
        )

        _LOG.info('Creating user...')

        username, password = self.create_system_user(
            username=username, role_name=SYSTEM_ROLE)
        if not api_link:
            _LOG.info('Api link was not specified. Resolving the api link')
            api_link = self.resolve_api_link()
        _LOG.info('Environment was successfully configured! Use commands to '
                  'configure CLI:')
        print(C7N_CONFIGURE_COMMAND.format(api_link=api_link or '[api_link]'))
        print(C7N_LOGIN_COMMAND.format(username=username,
                                       password=password or '[password]'))


class CreateCustomer(EntitiesRelatedActions, ActionHandler):
    """
    Creates an admin customer and his admin role and policy and additionally
    creates so-called user policy and user role within the customer which
    contains a more restricted set of permissions
    """

    def __call__(self, customer_name: str, admins: List[str]):
        acs = SERVICE_PROVIDER.access_control_service()
        _LOG.info(f'Creating admin customer "{customer_name}"')
        self.create_customer(customer_name, admins)

        _LOG.info('Creating admin policy')
        self.create_policy(
            customer_name=customer_name, policy_name=ADMIN_POLICY,
            permissions=list(acs.admin_permissions)
        )

        _LOG.info('Creating admin role')
        self.create_role(
            customer_name=customer_name,
            role_name=ADMIN_ROLE,
            policy_names=[ADMIN_POLICY, ]
        )

        _LOG.info('Creating user policy')
        self.create_policy(
            customer_name=customer_name,
            policy_name=USER_POLICY,
            permissions=list(acs.user_permissions)
        )
        _LOG.info('Create user role')
        self.create_role(
            customer_name=customer_name,
            role_name=USER_ROLE,
            policy_names=[USER_POLICY]
        )


class CreateTenant(ActionHandler):

    def __call__(self, **kwargs):
        pass


class CreateUser(EntitiesRelatedActions, ActionHandler):
    def __call__(self, username: str, customer_name: str,
                 tenant_names: List[str], role_name: str = USER_ROLE):
        username, password = self.create_user(
            username=username,
            customer_name=customer_name,
            role_name=role_name,
            tenants=tenant_names
        )
        print(C7N_LOGIN_COMMAND.format(username=username,
                                       password=password or '[password]'))


def main(args: Optional[List[str]] = None):
    parser = build_parser()
    arguments = parser.parse_args(args)
    key = tuple(
        getattr(arguments, dest) for dest in ALL_NESTING
        if hasattr(arguments, dest)
    )
    mapping: Dict[Tuple[str, ...], Union[Callable, ActionHandler]] = {
        (INIT_VAULT_ACTION,): InitSubService({'vault'}),
        (CREATE_INDEXES_ACTION,): InitSubService({'mongo'}),
        (CREATE_BUCKETS_ACTION,): InitSubService({'minio'}),
        (RUN_ACTION,): Run(),
        (PARSE_XLSX_STANDARD_ACTION,): parse_xlsx_standard,
        (PARSE_RULE_SOURCE_ACTION,): parse_rule_source,
        (GENERATE_RULES_TABLE_ACTION,): generate_rules_table,

        (UPDATE_API_GATEWAY_MODELS_ACTION,): UpdateApiGatewayModels(),
        (ENV_ACTION, UPDATE_SETTINGS_ENV_ACTION): UpdateSettings(),
        (ENV_ACTION, CREATE_SYSTEM_USER_ENV_ACTION): CreateSystemUser(),
        (ENV_ACTION, CREATE_CUSTOMER_ACTION): CreateCustomer(),
        (ENV_ACTION, CREATE_USER_ACTION): CreateUser()

    }
    func: Callable = mapping.get(key) or (lambda **kwargs: _LOG.error('Hello'))
    for dest in ALL_NESTING:
        if hasattr(arguments, dest):
            delattr(arguments, dest)
    load_dotenv(verbose=True)
    func(**vars(arguments))


if __name__ == '__main__':
    main()
