from modular_sdk.commons.error_helper import RESPONSE_BAD_REQUEST_CODE, \
    RESPONSE_OK_CODE, RESPONSE_SERVICE_UNAVAILABLE_CODE

from helpers import build_response, REPO_SETTINGS_PATH, REPO_S3_ROOT, \
    LINE_SEP, CustodianException
from helpers.log_helper import get_logger
from models.licenses import License
from models.policy import Policy
from models.role import Role
from services import SERVICE_PROVIDER
from services.abstract_lambda import AbstractLambda
from services.clients.kms import KMSClient
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService
from services.gitlab_service import GitlabService
from services.license_service import LicenseService
from services.rbac.iam_cache_service import CachedIamService
from services.setting_service import SettingsService
from services.ssm_service import SSMService

_LOG = get_logger('custodian-configuration-updater')

PARAM_BUILD_NAME = 'build_name'
ACCOUNTS = 'Accounts'
POLICIES = 'Policies'
ROLES = 'Roles'
LICENSES = 'Licenses'
SIEM = 'SIEMManager'


class ConfigurationUpdater(AbstractLambda):

    def __init__(self, environment_service: EnvironmentService,
                 ssm_service: SSMService, kms_client: KMSClient,
                 s3_client: S3Client, settings_service: SettingsService,
                 cached_iam_service: CachedIamService,
                 license_service: LicenseService):
        self.environment_service = environment_service
        self.ssm_service = ssm_service
        self.s3_client = s3_client
        self.kms_client = kms_client
        self.settings_service = settings_service
        self.cached_iam_service = cached_iam_service
        self.license_service = license_service

    def configuration_git(self):
        git_access_data = self.settings_service.get_backup_repo_settings()
        if not git_access_data:
            build_response(
                content='Please check Backup repo configuration',
                code=RESPONSE_BAD_REQUEST_CODE
            )

        secret_name = git_access_data.get('git_access_secret')
        git_access_secret = self.ssm_service.get_secret_value(
            secret_name=secret_name)
        git_access_data['git_access_secret'] = git_access_secret

        self.gitlab_service: GitlabService = SERVICE_PROVIDER.gitlab_service(
            git_access_data=git_access_data)

    def validate_request(self, event):
        bucket = self.environment_service.get_ssm_backup_bucket()
        kms_key_id = self.environment_service.get_ssm_backup_kms_key_id()

        if self.error_with_validation_env_vars(kms_key_id, bucket):
            _LOG.error('Missing one of the following env variables: '
                       '\'caas_ssm_backup_kms_key_id\', '
                       '\'caas_ssm_backup_bucket\'')
            raise CustodianException(
                code=502,
                content="The service is not configured correctly. Please "
                        "contact Custodian Service support."
            )

    def handle_request(self, event, context):
        self.configuration_git()
        _LOG.debug(f'Event: {event}')

        _LOG.debug('Resolving build folder')
        build_folder = self.create_build_folder_or_error(event)

        _LOG.info(f'Processing build: {build_folder}')

        entities = self.gitlab_service.pull_structure_of_ddb_tables(
            folder_path=f"{build_folder}/dynamodb")

        _LOG.info(f'Creating {entities} configurations.')

        configuration_objects = self.create_configuration(
            build_folder=build_folder, entities=entities)
        _LOG.debug(f'Configuration objects: {configuration_objects}')

        secrets = self.create_secrets(build_folder=build_folder)

        _LOG.info('Creating Settings')
        settings = self.create_settings(build_folder=build_folder)
        _LOG.debug(f'Settings objects: {settings}')

        _LOG.info('Uploading files to s3')
        files = self.upload_git_files_to_s3(build_folder=build_folder)
        _LOG.debug(f'Files: {files}')

        for table_name, configurations in configuration_objects.items():
            for index, conf_object in enumerate(configurations):
                configurations[index] = conf_object.get_json()

        file_paths = [file.get('path') for file in files]
        return build_response(
            code=RESPONSE_OK_CODE,
            content={
                'configuration_objects': configuration_objects,
                'settings': settings,
                'secrets': secrets,
                'ruleset_files': file_paths
            }
        )

    def create_configuration(self, build_folder, entities):
        """
        1. Pull content from Git
        2. Creating configuration for each entity
        3. Save models to DDB
        """

        data_entities = self.get_data_from_git(_entities=entities,
                                               _build_folder=build_folder)

        _LOG.debug(f'Data from Git: {data_entities}')

        objects = self.populate_configuration(
            accounts_data=data_entities.get("Accounts") if data_entities.get(
                "Accounts") else [],
            policies_data=data_entities.get("Policies") if data_entities.get(
                "Policies") else [],
            roles_data=data_entities.get("Roles") if data_entities.get(
                "Roles") else [],
            licenses_data=data_entities.get("Licenses") if data_entities.get(
                "Licenses") else [],
            siem_data=data_entities.get("SIEMManager") if data_entities.get(
                "SIEMManager") else []
        )

        self.batch_save_configuration(
            policies_data=objects.get('Policies'),
            roles_data=objects.get('Roles'),
            licenses_data=objects.get('Licenses'),
            siem_data=objects.get('SIEMManager')
        )
        _LOG.debug(f'Configuration objects: {objects}')
        return objects

    def create_secrets(self, build_folder):
        bucket = self.environment_service.get_ssm_backup_bucket()
        kms_key_id = self.environment_service.get_ssm_backup_kms_key_id()

        _LOG.debug(f'SSM backup bucket: {bucket}')
        encrypted_objects = list(self.s3_client.list_objects(
            bucket_name=bucket, prefix=build_folder
        ))
        object_keys = [obj.get('Key') for obj in encrypted_objects]
        _LOG.debug(f'List of objects to import as SSM parameters: '
                   f'{object_keys}')
        if not encrypted_objects:
            return []

        created_secrets = []
        for object_key in object_keys:
            _LOG.debug(f'Processing {object_key} parameter')
            secret_name = object_key.split('/')[-1]
            _LOG.info('Downloading encrypted content')
            content = self.s3_client.get_file_content(
                bucket_name=bucket,
                full_file_name=object_key)
            decrypted_value = self.kms_client.decrypt(value=content,
                                                      key_id=kms_key_id)
            _LOG.debug('Secret value has been decrypted')
            self.ssm_service.create_secret_value(secret_name=secret_name,
                                                 secret_value=decrypted_value)
            created_secrets.append(secret_name)
            _LOG.info(f'SSM Parameter with name \'{secret_name}\''
                      f' has been created.')
        return created_secrets

    def create_settings(self, build_folder):
        full_settings_path = LINE_SEP.join((build_folder, REPO_SETTINGS_PATH))
        settings_data = self.gitlab_service.pull_folder_files_content(
            folder_path=full_settings_path)
        _LOG.debug(f'Settings data: {settings_data}')

        settings = {}
        for setting_data in settings_data:
            for name, value in setting_data.items():
                _LOG.debug(f'Creating Setting: {name}:{value}')
                setting = self.settings_service.create(name=name, value=value)
                self.settings_service.save(setting)
                settings[name] = value
        return settings

    def upload_git_files_to_s3(self, build_folder):
        """
        1. Get bucket name from env vars
        2. Validate if bucket exists
        3. Build path in s3 (repo_s3_root)
        4. Get files from Git
        5. Upload files into S3
        6. return files from Git
        """
        bucket_name = self.environment_service.get_rulesets_bucket_name()
        _LOG.debug(f'Rulesets bucket name: {bucket_name}')

        if not self.s3_client.is_bucket_exists(bucket_name=bucket_name):
            _LOG.error(f'Specified rulesets bucket \'{bucket_name}\' '
                       f'does not exist')
            build_response(
                code=RESPONSE_SERVICE_UNAVAILABLE_CODE,
                content=f'Specified rulesets bucket \'{bucket_name}\' '
                        f'does not exist')

        repo_s3_root = LINE_SEP.join((build_folder, REPO_S3_ROOT, bucket_name))

        files_data = self.gitlab_service.pull_recursive_folder_files_content(
            folder_path=repo_s3_root)
        _LOG.debug(f'Files data: {files_data}')

        for file_data in files_data:
            _LOG.debug(f'Uploading file: {file_data.get("path")}')
            self.s3_client.put_object(
                bucket_name=bucket_name,
                object_name=file_data.get('path'),
                body=file_data.get('content'))

        return files_data

    def resolve_build_folder(self, build_folder: str) -> str:
        if build_folder and self.gitlab_service.build_exists(
                build_name=build_folder):
            _LOG.info(f'Build specified in the request '
                      f'will be used: {build_folder}')
        else:
            _LOG.info('Last available build will be used')
            build_folder = self.gitlab_service.get_last_build_folder_name()
        return build_folder

    @staticmethod
    def error_with_validation_env_vars(kms_key_id, bucket):
        if not kms_key_id or not bucket:
            return False

    def create_build_folder_or_error(self, _event):
        build_folder = self.resolve_build_folder(
            build_folder=_event.get(PARAM_BUILD_NAME))

        if not build_folder:
            _LOG.warning(f'No builds available in target repository: '
                         f'{self.gitlab_service.git_url} , '
                         f'ref: {self.gitlab_service.git_ref}')
            return build_response(
                code=RESPONSE_SERVICE_UNAVAILABLE_CODE,
                content=f'No builds available in target repository: '
                        f'{self.gitlab_service.git_url} , '
                        f'ref: {self.gitlab_service.git_ref}')
        return build_folder

    def get_data_from_git(self, _entities, _build_folder):
        """
        Get files from Git repo (<build_folder>/dynamodb/<entity>)
        entity could be from list of _entities
        """
        _data_entities = dict().fromkeys(_entities)

        for entity in _entities:  # ['Accounts', 'Policies',
            # 'Roles', 'Rules', 'Settings']
            path_for_entity = LINE_SEP.join(
                (_build_folder, 'dynamodb', entity))
            entity_data = self.gitlab_service.pull_folder_files_content(
                folder_path=path_for_entity)
            _LOG.debug(f'{entity} data: {entity_data}')
            _data_entities[entity] = entity_data
        return _data_entities

    @staticmethod
    def modify_name(entity_data):
        for file in entity_data:
            if file.get('display_name'):
                file['display_name'] += '_restored'
            if file.get('name'):
                file['name'] += '_restored'
        return entity_data

    def populate_configuration(self, accounts_data: list, policies_data: list,
                               roles_data: list, licenses_data: list,
                               siem_data: list) -> dict:
        configs = {i: [] for i in
                   (ACCOUNTS, POLICIES, ROLES, LICENSES, SIEM)}

        for policy_data in policies_data:
            _LOG.debug(f'Creating policy configuration: {policy_data}')
            config = self.cached_iam_service.create_policy(policy_data)
            if config:
                configs[POLICIES].append(config)

        for role_data in roles_data:
            _LOG.debug(f'Creating role configuration: {role_data}')
            config = self.cached_iam_service.create_role(role_data)
            if config:
                configs[ROLES].append(config)

        for license_data in licenses_data:
            _LOG.debug(f'Creating license configuration: {license_data}')
            config = self.license_service.create(license_data)
            if config:
                configs[LICENSES].append(config)

        for siem in siem_data:
            _LOG.debug(f'Creating SIEM configuration: {siem}')
            config = self.siem_manager_service.create(siem)
            if config:
                configs[SIEM].append(config)

        return configs

    @staticmethod
    def batch_save_configuration(policies_data: list = None,
                                 roles_data: list = None,
                                 licenses_data: list = None,
                                 siem_data: list = None):
        if roles_data:
            with Role.batch_write() as batch:
                for role in roles_data:
                    batch.save(role)
        if policies_data:
            with Policy.batch_write() as batch:
                for policy in policies_data:
                    batch.save(policy)
        if licenses_data:
            with License.batch_write() as batch:
                for _license in licenses_data:
                    batch.save(_license)


HANDLER = ConfigurationUpdater(
    environment_service=SERVICE_PROVIDER.environment_service(),
    ssm_service=SERVICE_PROVIDER.ssm_service(),
    s3_client=SERVICE_PROVIDER.s3(),
    kms_client=SERVICE_PROVIDER.kms(),
    settings_service=SERVICE_PROVIDER.settings_service(),
    cached_iam_service=SERVICE_PROVIDER.iam_cache_service(),
    license_service=SERVICE_PROVIDER.license_service()
)


def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
