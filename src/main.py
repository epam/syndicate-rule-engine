#!/usr/local/bin/python
import argparse
import base64
import json
import logging.config
import secrets
import string
import sys
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Callable, Literal

from modular_sdk.models.pynamongo.indexes_creator import IndexesCreator

from helpers import dereference_json
from helpers.log_helper import setup_logging
from helpers.__version__ import __version__
from helpers.constants import (
    DEFAULT_RULES_METADATA_REPO_ACCESS_SSM_NAME,
    DOCKER_SERVICE_MODE,
    PRIVATE_KEY_SECRET_NAME,
    Env,
    HTTPMethod,
    Permission,
    SettingKey,
    DEFAULT_SYSTEM_CUSTOMER,
)
from onprem.api.app import OnPremApiBuilder
from services import SP
from services.openapi_spec_generator import OpenApiGenerator


SRC = Path(__file__).parent.resolve()

DEPLOYMENT_RESOURCES_FILENAME = 'deployment_resources.json'

ACTION_DEST = 'action'
ENV_ACTION_DEST = 'env_action'
ALL_NESTING: tuple[str, ...] = (ACTION_DEST, ENV_ACTION_DEST)  # important

RUN_ACTION = 'run'
CREATE_INDEXES_ACTION = 'create_indexes'
CREATE_BUCKETS_ACTION = 'create_buckets'
GENERATE_OPENAPI_ACTION = 'generate_openapi'
INIT_VAULT_ACTION = 'init_vault'
SET_META_REPOS_ACTION = 'set_meta_repos'
UPDATE_API_GATEWAY_MODELS_ACTION = 'update_api_models'
SHOW_PERMISSIONS_ACTION = 'show_permissions'
INIT_ACTION = 'init'

DEFAULT_HOST = '0.0.0.0'
DEFAULT_PORT = 8000

SYSTEM_USER = 'system_user'


def gen_password(digits: int = 20) -> str:
    chars = string.ascii_letters + string.digits
    while True:
        password = ''.join(secrets.choice(chars) for _ in range(digits))
        if (
            any(c.islower() for c in password)
            and any(c.isupper() for c in password)
            and sum(c.isdigit() for c in password) >= 3
        ):
            break
    return password


logging.config.dictConfig(
    {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'console_formatter': {'format': '%(levelname)s - %(message)s'}
        },
        'handlers': {
            'console_handler': {
                'class': 'logging.StreamHandler',
                'formatter': 'console_formatter',
            }
        },
        'loggers': {
            '__main__': {'level': 'DEBUG', 'handlers': ['console_handler']},
            'modular_sdk': {'level': 'INFO', 'handlers': ['console_handler']},
        },
    }
)
_LOG = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Syndicate Rule Engine configuration cli entering point'
    )
    # -- top level sub-parser
    sub_parsers = parser.add_subparsers(
        dest=ACTION_DEST, required=True, help='Available actions'
    )
    _ = sub_parsers.add_parser(
        CREATE_INDEXES_ACTION, help='Re-create MongoDB indexes'
    )
    _ = sub_parsers.add_parser(
        INIT_VAULT_ACTION,
        help='Enables secret engine and crated a necessary token in Vault',
    )
    set_meta_parser = sub_parsers.add_parser(
        SET_META_REPOS_ACTION,
        help='Sets rules metadata gitlab repositories to vault',
    )

    class MetaAccessType:
        def __call__(self, item: str) -> tuple[str, str]:
            res = item.strip().split(':', maxsplit=1)
            if len(res) != 2:
                raise ValueError('Invalid value. Must be <project>:<secret>')
            return res[0], res[1]

    set_meta_parser.add_argument(
        '--repositories',
        nargs='+',
        required=True,
        type=MetaAccessType(),
        help='List of repositories to set for meta: '
        '--repositories <project1>:<secret> <project2>:<secret>',
    )
    _ = sub_parsers.add_parser(
        CREATE_BUCKETS_ACTION, help='Creates necessary buckets in Minio'
    )
    _ = sub_parsers.add_parser(
        UPDATE_API_GATEWAY_MODELS_ACTION,
        help='Regenerates API Gateway models from existing pydantic validators',
    )
    _ = sub_parsers.add_parser(
        SHOW_PERMISSIONS_ACTION,
        help='Dumps existing permissions to stdout. '
        'By default, dumps only user permission. '
        'Use flags to dump admin permissions as well ',
    )
    _ = sub_parsers.add_parser(
        INIT_ACTION, help='Creates system user and sets up some base settings'
    )
    _ = sub_parsers.add_parser(
        GENERATE_OPENAPI_ACTION,
        help='Generates Open API spec for Rule Engine API',
    )
    parser_run = sub_parsers.add_parser(RUN_ACTION, help='Run on-prem server')
    parser_run.add_argument(
        '--host',
        default=DEFAULT_HOST,
        type=str,
        help='IP address where to run the server',
    )
    parser_run.add_argument(
        '--port',
        default=DEFAULT_PORT,
        type=int,
        help='IP Port to run the server on',
    )
    return parser


class ActionHandler(ABC):
    @abstractmethod
    def __call__(self, **kwargs): ...


class InitVault(ActionHandler):
    @staticmethod
    def generate_private_key(
        kty: Literal['EC', 'RSA'] = 'EC', crv='P-521', size: int = 4096
    ) -> str:
        """
        Generates a private key and exports PEM to str encoding it to base64
        :param kty:
        :param crv:
        :param size:
        :return:
        """
        from jwcrypto import jwk

        match kty:
            case 'EC':
                key = jwk.JWK.generate(kty=kty, crv=crv)
            case _:  # RSA
                key = jwk.JWK.generate(kty=kty, size=size)
        return base64.b64encode(
            key.export_to_pem(private_key=True, password=None)
        ).decode()

    def __call__(self):
        ssm = SP.ssm
        if not ssm.is_secrets_engine_enabled():
            _LOG.info('Enabling vault secrets engine')
            ssm.enable_secrets_engine()
        else:
            _LOG.info('Secrets engine is already enabled in vault')
        if ssm.get_secret_value(PRIVATE_KEY_SECRET_NAME):
            _LOG.info('Token inside Vault already exists. Skipping...')
            return
        ssm.create_secret(
            secret_name=PRIVATE_KEY_SECRET_NAME,
            secret_value=self.generate_private_key(),
        )
        _LOG.info('Private token was generated and set to vault')


class InitMinio(ActionHandler):
    @staticmethod
    def buckets() -> tuple[str, ...]:
        environment = SP.environment_service
        return (
            environment.get_statistics_bucket_name(),
            environment.get_rulesets_bucket_name(),
            environment.default_reports_bucket_name(),
        )

    @staticmethod
    def create_bucket(name: str) -> None:
        client = SP.s3
        if client.bucket_exists(bucket=name):
            _LOG.info(f'Bucket {name} already exists')
            return
        client.create_bucket(
            bucket=name, region=SP.environment_service.aws_region()
        )
        _LOG.info(f'Bucket {name} was created')

    def __call__(self):
        from services.reports_bucket import (
            ReportMetaBucketsKeys,
            ReportsBucketKeysBuilder,
        )

        for name in self.buckets():
            self.create_bucket(name)

        _LOG.info(f'Setting lifecycle rules for reports bucket')
        SP.s3.put_bucket_lifecycle_rules(
            bucket=Env.REPORTS_BUCKET_NAME.as_str(),
            rules=[
                SP.s3.build_lifecycle_rule(
                    days=7, prefix=ReportsBucketKeysBuilder.on_demand
                ),
                SP.s3.build_lifecycle_rule(
                    days=7, prefix=ReportMetaBucketsKeys.prefix
                ),
                SP.s3.build_lifecycle_rule(
                    days=Env.REPORTS_SNAPSHOTS_LIFETIME_DAYS.as_int(),
                    tag=('Type', 'DataSnapshot')
                ),
            ],
        )


class InitMongo(ActionHandler):
    @staticmethod
    def models() -> tuple:
        from models.batch_results import BatchResults
        from models.event import Event
        from models.job import Job
        from models.metrics import ReportMetrics
        from models.policy import Policy
        from models.report_statistics import ReportStatistics
        from models.retries import Retries
        from models.role import Role
        from models.rule import Rule
        from models.rule_source import RuleSource
        from models.ruleset import Ruleset
        from models.scheduled_job import ScheduledJob
        from models.setting import Setting
        from models.user import User
        from models.resource import Resource

        return (
            BatchResults,
            Event,
            Job,
            Policy,
            ReportStatistics,
            Retries,
            Role,
            Rule,
            RuleSource,
            Ruleset,
            ScheduledJob,
            Setting,
            User,
            ReportMetrics,
            Resource
        )

    def __call__(self):
        _LOG.debug('Going to sync indexes with code')
        from models import PynamoDBToPymongoAdapterSingleton, BaseModel
        from models.resource import create_resources_indexes
        from models.resource_exception import create_resource_exceptions_indexes

        if not BaseModel.is_mongo_model():
            _LOG.warning(f'Cannot create indexes for {Env.get_db_type()}')
            return
        creator = IndexesCreator(
            db=PynamoDBToPymongoAdapterSingleton.get_instance().mongo_database
        )

        _LOG.info('Creating custom indexes for SREResources if not exist')
        resources_indexes = create_resources_indexes(
            PynamoDBToPymongoAdapterSingleton.get_instance().mongo_database
        )
        _LOG.info('Creating custom indexes for SREResourceExceptions if not exist')
        resource_exceptions_indexes = create_resource_exceptions_indexes(
            PynamoDBToPymongoAdapterSingleton.get_instance().mongo_database
        )

        always_keep = (
            '_id_', 
            'next_run_time_1',
            *resources_indexes,
            *resource_exceptions_indexes,
        )
        for model in self.models():
            _LOG.info(f'Syncing indexes for {model.Meta.table_name}')

            creator.sync(model, always_keep=always_keep)

class Run(ActionHandler):
    def __call__(
        self,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
    ):
        # needed here to override logging config from this file
        setup_logging()

        self._host = host
        self._port = port

        if not Env.is_docker():
            Env.SERVICE_MODE.set(DOCKER_SERVICE_MODE)

        app = OnPremApiBuilder('caas').build()
        app.run(host=host, port=port)


class UpdateApiGatewayModels(ActionHandler):
    """
    Updates ./validators/deployment_resources.json and
    ./deployment_resources.json
    """

    @property
    def validators_module(self) -> str:
        return 'validators'

    @property
    def models_deployment_resources(self) -> Path:
        return SRC / self.validators_module / DEPLOYMENT_RESOURCES_FILENAME

    @property
    def mail_deployment_resources(self) -> Path:
        return SRC / DEPLOYMENT_RESOURCES_FILENAME

    @property
    def sre_api_gateway_name(self) -> str:
        return 'custodian-as-a-service-api'

    @property
    def sre_api_definition(self) -> dict:
        return {
            self.sre_api_gateway_name: {
                'resource_type': 'api_gateway',
                'dependencies': [],
                'resources': {},
                'models': {},
            }
        }

    def __call__(self, **kwargs):
        from validators import registry

        api_def = self.sre_api_definition
        for model in registry.iter_models(without_get=True):
            schema = model.model_json_schema()
            dereference_json(schema)
            schema.pop('$defs', None)
            api_def[self.sre_api_gateway_name]['models'].update(
                {
                    model.__name__: {
                        'content_type': 'application/json',
                        'schema': schema,
                    }
                }
            )
        path = self.models_deployment_resources
        _LOG.info(f'Updating {path}')
        with open(path, 'w') as file:
            json.dump(api_def, file, indent=2, sort_keys=True)
        _LOG.info(f'{path} has been updated')

        # here we update api gateway inside main deployment resources.
        # We don't remove existing endpoints, only add new in case they are
        # defined in RequestModelRegistry and are absent inside deployment
        # resources. Also, we update request and response models. Default
        # lambda is configuration-api-handler. Change it if it's wrong
        path = self.mail_deployment_resources
        _LOG.info(f'Updating {path}')
        with open(path, 'r') as file:
            deployment_resources = json.load(file)
        api = deployment_resources.get(self.sre_api_gateway_name)
        if not api:
            _LOG.warning('Api gateway not found in deployment_resources')
            return
        resources = api.setdefault('resources', {})
        for item in registry.iter_all():
            # if endpoint & method are defined, just update models.
            # otherwise add configuration
            data = resources.setdefault(
                item.path,
                {'policy_statement_singleton': True, 'enable_cors': True},
            ).setdefault(
                item.method.value,
                {
                    'integration_type': 'lambda',
                    'enable_proxy': True,
                    'lambda_alias': '${lambdas_alias_name}',
                    'authorization_type': 'authorizer'
                    if item.auth
                    else 'NONE',
                    'lambda_name': 'caas-configuration-api-handler',
                },
            )
            data.pop('method_request_models', None)
            data.pop('responses', None)
            data.pop('method_request_parameters', None)
            if model := item.request_model:
                match item.method:
                    case HTTPMethod.GET:
                        params = {}
                        for name, info in model.model_fields.items():
                            params[f'method.request.querystring.{name}'] = (
                                info.is_required()
                            )
                        data['method_request_parameters'] = params
                    case _:
                        data['method_request_models'] = {
                            'application/json': model.__name__
                        }
            responses = []
            for st, m, description in item.responses:
                resp = {'status_code': str(st.value)}
                if m:
                    resp['response_models'] = {'application/json': m.__name__}
                responses.append(resp)
            data['responses'] = responses

        with open(path, 'w') as file:
            json.dump(deployment_resources, file, indent=2)
        _LOG.info(f'{path} has been updated')


class InitAction(ActionHandler):
    def __call__(self):
        from models.setting import Setting

        if not Setting.get_nullable(SettingKey.SYSTEM_CUSTOMER):
            _LOG.info('Setting system customer name')
            Setting(
                name=SettingKey.SYSTEM_CUSTOMER.value,
                value=DEFAULT_SYSTEM_CUSTOMER,
            ).save()
        Setting(name=SettingKey.SEND_REPORTS, value=True).save()
        users_client = SP.users_client
        if not users_client.get_user_by_username(SYSTEM_USER):
            _LOG.info('Creating a system user')
            password = Env.SYSTEM_USER_PASSWORD.get(None)
            from_env = bool(password)
            if not from_env:
                password = gen_password()

            users_client.signup_user(
                username=SYSTEM_USER,
                password=password,
                customer=DEFAULT_SYSTEM_CUSTOMER,
            )
            if not from_env:
                print(f'System ({SYSTEM_USER}) password: {password}')
            else:
                print(f'System ({SYSTEM_USER}) was created')
        else:
            _LOG.info('System user already exists')
        _LOG.info('Done')


class GenerateOpenApi(ActionHandler):
    def __call__(self):
        from validators import registry
        generator = OpenApiGenerator(
            title='Rule Engine - OpenAPI 3.0',
            description='Rule engine rest api',
            url=f'http://{DEFAULT_HOST}:{DEFAULT_PORT}',
            stages='caas',
            version=__version__,
            endpoints=registry.iter_all(),
        )
        json.dump(generator.generate(), sys.stdout, separators=(',', ':'))


class ShowPermissions(ActionHandler):
    def __call__(self):
        json.dump(sorted(Permission.iter_enabled()), sys.stdout, indent=4)


class SetMetaRepos(ActionHandler):
    def __call__(self, repositories: list[tuple[str, str]]):
        ssm = SP.ssm
        ssm.create_secret(
            secret_name=DEFAULT_RULES_METADATA_REPO_ACCESS_SSM_NAME,
            secret_value=[
                {
                    'project': i[0],
                    'ref': 'main',
                    'secret': i[1],
                    'url': 'https://git.epam.com',
                }
                for i in repositories
            ],
        )
        _LOG.info('Repositories were set')


def main(args: list[str] | None = None):
    parser = build_parser()
    arguments = parser.parse_args(args)
    key = tuple(
        getattr(arguments, dest)
        for dest in ALL_NESTING
        if hasattr(arguments, dest)
    )
    mapping: dict[tuple[str, ...], Callable | ActionHandler] = {
        (INIT_VAULT_ACTION,): InitVault(),
        (SET_META_REPOS_ACTION,): SetMetaRepos(),
        (CREATE_INDEXES_ACTION,): InitMongo(),
        (CREATE_BUCKETS_ACTION,): InitMinio(),
        (GENERATE_OPENAPI_ACTION,): GenerateOpenApi(),
        (RUN_ACTION,): Run(),
        (UPDATE_API_GATEWAY_MODELS_ACTION,): UpdateApiGatewayModels(),
        (SHOW_PERMISSIONS_ACTION,): ShowPermissions(),
        (INIT_ACTION,): InitAction(),
    }
    func: Callable = mapping.get(key) or (lambda **kwargs: _LOG.error('Hello'))
    for dest in ALL_NESTING:
        if hasattr(arguments, dest):
            delattr(arguments, dest)
    try:
        func(**vars(arguments))
    except Exception as e:
        _LOG.error(f'Unexpected exception occurred: {e}')
        exit(1)  # some connection errors for entrypoint.sh


if __name__ == '__main__':
    main()
