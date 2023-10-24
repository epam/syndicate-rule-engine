import json
from datetime import datetime
from http import HTTPStatus

from services.gitlab_service import GitlabService

from helpers import build_response, \
    LINE_SEP, REPO_S3_ROOT, REPO_DYNAMODB_ROOT, REPO_SETTINGS_FOLDER, \
    REPO_POLICIES_FOLDER, REPO_ROLES_FOLDER, \
    CustodianException, REPO_LICENSES_FOLDER, REPO_SIEM_FOLDER
from helpers.log_helper import get_logger
from services import SERVICE_PROVIDER
from services.abstract_lambda import AbstractLambda
from services.clients.kms import KMSClient
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService
from services.license_service import LicenseService
from services.modular_service import ModularService
from services.rbac.iam_cache_service import CachedIamService
from services.rule_source_service import RuleSourceService
from services.ruleset_service import RulesetService
from services.setting_service import SettingsService
from services.ssm_service import SSMService

_LOG = get_logger('custodian-configuration-backupper')

COMMIT_MESSAGE_TEMPLATE = 'Backup from \'{build_name}\''
STATUS_READY_TO_SCAN = 'READY_TO_SCAN'


def error_with_validation_env_vars(kms_key_id, ssm_backup_bucket):
    """
    Must be set upped
    AWS_REGION=eu-central-1;
    caas_ssm_backup_kms_key_id=KMS_KEY_ID;
    caas_ssm_backup_bucket=BUCKET
    """
    message = ''

    if not kms_key_id or not ssm_backup_bucket:
        message = "Service is improperly configured. Please contact " \
                  "Custodian Service support team."
    return message


class ConfigurationBackupper(AbstractLambda):

    def __init__(self, environment_service: EnvironmentService,
                 ssm_service: SSMService, s3_client: S3Client,
                 kms_client: KMSClient, settings_service: SettingsService,
                 modular_service: ModularService, ruleset_service: RulesetService,
                 cached_iam_service: CachedIamService,
                 rulesource_service: RuleSourceService,
                 license_service: LicenseService):

        self.environment_service = environment_service
        self.ssm_service = ssm_service
        self.s3_client = s3_client
        self.kms_client = kms_client
        self.settings_service = settings_service
        self.modular_service = modular_service
        self.cached_iam_service = cached_iam_service
        self.ruleset_service = ruleset_service
        self.rulesource_service = rulesource_service
        self.license_service = license_service
        self.accounts = []
        self.policies = []
        self.roles = []
        self.settings = []
        self.rulesets = []
        self.rulesources = []
        self.licenses = []
        self.siem = []

    def configuration_git(self):
        self.git_access_data = SERVICE_PROVIDER.settings_service() \
            .get_backup_repo_settings()
        self.validating_git_access_data(self.git_access_data)
        secret_name = self.git_access_data.get('git_access_secret')
        self.validate_secret_name(secret_name)
        git_access_secret = self.ssm_service.get_secret_value(
            secret_name=secret_name)
        self.git_access_data['git_access_secret'] = git_access_secret
        self.gitlab_service: GitlabService = SERVICE_PROVIDER.gitlab_service(
            git_access_data=self.git_access_data)

    def create_list_data_for_backup(self):
        self.policies = self.cached_iam_service.list_policies()
        self.roles = self.cached_iam_service.list_roles()
        self.settings = list(self.settings_service.get_all_settings())
        self.get_all_rulesets()
        self.get_all_rulesources(self.accounts)
        self.licenses = list(self.license_service.scan())
        self.siem = list(self.siem_manager_service.list())

    def handle_request(self, event, context):
        self.configuration_git()

        _LOG.debug(f'Event: {event}')

        _LOG.debug('Extracting data from dynamodb tables.')
        self.create_list_data_for_backup()

        dynamodb_config = self.generate_ddb_config()

        build_name = self.generate_build_name()
        _LOG.debug(f'Build name generated: {build_name}')
        commit_message = self.generate_commit_message(build_name=build_name)
        _LOG.debug(f'Commit message generated: {commit_message}')

        commit_data = {
            'branch': self.gitlab_service.git_ref,
            'commit_message': commit_message,
            'actions': []
        }

        table_meta_actions = self.backup_table_meta(dynamodb_config,
                                                    build_name)
        commit_data["actions"].extend(table_meta_actions)

        _LOG.info('Backupping settings table meta')
        settings_table_actions = self.backup_settings_table(build_name)
        commit_data["actions"].extend(settings_table_actions)

        _LOG.info('Backupping compiled ruleset files')
        s3_actions = self.get_s3_actions(build_name=build_name)

        commit_data['actions'].extend(s3_actions)

        _LOG.info('Backupping SSM Parameters')
        ssm_params_backed_up = self.backup_ssm(
            build_name=build_name,
            conf_objects=self.rulesources)

        # creating commit
        _LOG.info(f'Creating a commit in branch:'
                  f' \'{self.gitlab_service.git_ref}\'')
        commit = self.gitlab_service.create_commit(
            commit_data=commit_data)

        files_created = [item.get('file_path') for item in
                         commit_data.get('actions')]

        _LOG.info('Commit has been created successfully')

        response_content = {
            'title': commit.title,
            'commit_url': commit.web_url,
            'stats': commit.stats,
            'git_files_created': files_created,
            'ssm_params_created': ssm_params_backed_up
        }
        return build_response(code=HTTPStatus.OK, content=response_content)

    def validate_request(self, event):

        kms_key_id = self.environment_service.get_ssm_backup_kms_key_id()
        ssm_backup_bucket = self.environment_service.get_ssm_backup_bucket()

        if error_with_validation_env_vars(kms_key_id, ssm_backup_bucket):
            _LOG.warning("Please check setting up environment variables")
            raise CustodianException(
                code=502,
                content="The service is not configured correctly. Please "
                        "contact Custodian Service support."
            )

    def backup_ssm(self, build_name: str, conf_objects: list) -> list:
        if not conf_objects:
            return []
        ssm_param_names = []

        for conf_object in conf_objects:
            conf_json = conf_object.get_json()

            rules_repo_secret = conf_json.get('git_access_secret')
            if rules_repo_secret:
                ssm_param_names.append(rules_repo_secret)

        self.validation_ssm_param_names(ssm_param_names)

        param_values = self.ssm_service.get_secret_values(
            secret_names=ssm_param_names)
        kms_key_id = self.environment_service.get_ssm_backup_kms_key_id()
        ssm_backup_bucket = self.environment_service.get_ssm_backup_bucket()

        params_backed_up = []
        for cred_key, value in param_values.items():
            value_encrypted = self.kms_client.encrypt(key_id=kms_key_id,
                                                      value=value)
            obj_key = LINE_SEP.join((build_name, cred_key))

            self.s3_client.put_object(bucket_name=ssm_backup_bucket,
                                      object_name=obj_key,
                                      body=value_encrypted)
            params_backed_up.append(
                LINE_SEP.join((ssm_backup_bucket, obj_key)))
        return params_backed_up

    def get_all_rulesets(self):
        rulesets = self.ruleset_service.list_rulesets(
            licensed=False)
        self.rulesets.extend(self.add_validated_rulesets(rulesets))

    def get_all_rulesources(self, configuration_objects: list):
        for conf_object in configuration_objects:
            self.rulesources.extend(
                self.rulesource_service.list_rule_sources(customer=conf_object.customer_display_name))

    def get_s3_actions(self, build_name: str) -> list:
        """
        Append rulesets from S3 to action (list of files to add in Git).

        configuration_objects: list of Accounts from DDB to backup.
        Criteria to backup rulesets:
        1. Status - READY_TO_SCAN
        2. Has S3 path
        3. Has content
        """
        actions = []
        for ruleset in self.rulesets:
            s3_path = ruleset.attribute_values.get('path')
            bucket = ruleset.attribute_values.get('bucket_name')
            if s3_path and bucket:
                content = self.s3_client.get_file_content(
                    bucket_name=bucket, full_file_name=s3_path)
                if content:
                    repo_path = LINE_SEP.join(
                        (build_name, REPO_S3_ROOT, bucket, s3_path))
                    actions.append({'action': 'create',
                                    'file_path': repo_path,
                                    'content': content})
        return actions

    @staticmethod
    def get_settings_actions(build_name: str, settings: list) -> list:
        build_folder_path = LINE_SEP.join((build_name, REPO_DYNAMODB_ROOT,
                                           REPO_SETTINGS_FOLDER))
        filename = 'settings.json'
        content = {}
        for setting in settings:
            content[setting.name] = setting.value

        return [{'action': 'create',
                 'file_path': LINE_SEP.join((build_folder_path, filename)),
                 'content': json.dumps(content, indent=4)}]

    @staticmethod
    def get_dynamodb_objects_actions(configuration_objects: list,
                                     build_folder_path: str,
                                     identifier_attribute_name='id') -> list:
        """
        Creates action with all fields from DDB and file path. Append it to
        actions list
        """
        actions = []
        for obj in configuration_objects:
            content = obj.get_json()
            if hasattr(obj, identifier_attribute_name):
                file_name = f'{getattr(obj, identifier_attribute_name)}.json'
            elif obj._hash_keyname and obj._range_keyname:
                _hash_key = getattr(obj, obj._hash_keyname)
                _range_ket = getattr(obj, obj._range_keyname)
                file_name = f'{_hash_key}-{_range_ket}.json'
            else:
                file_name = f'{getattr(obj, obj._hash_keyname)}.json'

            file_path = LINE_SEP.join((build_folder_path, file_name))
            actions.append({
                'action': 'create',
                'file_path': file_path,
                'content': json.dumps(content, indent=4)})
        return actions

    @staticmethod
    def generate_commit_message(build_name: str):
        return COMMIT_MESSAGE_TEMPLATE.format(build_name=build_name)

    @staticmethod
    def generate_build_name():
        return datetime.now().strftime('%Y-%m-%d-%H-%M')

    def generate_ddb_config(self):
        dynamodb_config = {
            'Policies': {
                'objects': self.policies,
                'folder_name': REPO_POLICIES_FOLDER
            },
            'Roles': {
                'objects': self.roles,
                'folder_name': REPO_ROLES_FOLDER
            },
            'Licenses': {
                'objects': self.licenses,
                'folder_name': REPO_LICENSES_FOLDER
            },
            'SIEMManager': {
                'objects': self.siem,
                'folder_name': REPO_SIEM_FOLDER
            }
        }
        return dynamodb_config

    def backup_table_meta(self, _dynamodb_config, _build_name):
        """
        For-loop where take table name, data from this table and create
        actions (list with files to create)
        """
        _actions = []
        for table_name, config in _dynamodb_config.items():
            _LOG.info(f'Backupping \'{table_name}\' table meta')
            objects = config.get('objects')
            build_folder_path = LINE_SEP.join((_build_name, REPO_DYNAMODB_ROOT,
                                               config.get('folder_name')))
            actions = self.get_dynamodb_objects_actions(
                configuration_objects=objects,
                build_folder_path=build_folder_path)

            _LOG.debug(f'{table_name} actions: {actions}')
            _actions.extend(actions)
        return _actions

    def backup_settings_table(self, build_name):
        _actions = []
        settings_actions = self.get_settings_actions(build_name=build_name,
                                                     settings=self.settings)
        _LOG.debug(f'Settings actions: {settings_actions}')
        _actions.extend(settings_actions)
        return _actions

    @staticmethod
    def validation_ssm_param_names(ssm_param_names):
        if not ssm_param_names:
            build_response(
                content={"message": "Cannot backup SSM parameters because no "
                                    "model has the 'git_access_secret' and "
                                    "'credentials' fields"},
                code=HTTPStatus.BAD_REQUEST
            )

    @staticmethod
    def validate_secret_name(secret_name):
        error_message = "Please check that Git Access Data contains Git " \
                        "access secret"

        if not secret_name:
            build_response(
                content=error_message,
                code=HTTPStatus.BAD_REQUEST
            )

    @staticmethod
    def validating_git_access_data(git_access_data):

        _required_fields = ["git_ref", "git_access_type", "git_url",
                            "git_project_id", "git_access_secret"]

        if not isinstance(git_access_data, dict):
            error_message = "Please check git access data, it should " \
                            "provide keys and values"
            build_response(
                content=error_message,
                code=HTTPStatus.BAD_REQUEST
            )

        for key in ["git_ref", "git_access_type", "git_url",
                    "git_project_id", "git_access_secret"]:
            if not git_access_data.get(key):
                error_message = "Please, provide all required fields " \
                                f"first - {_required_fields}"
                build_response(
                    content=error_message,
                    code=HTTPStatus.BAD_REQUEST
                )

    @staticmethod
    def add_validated_rulesets(_rulesets):
        _rulesets_to_backup = []
        for ruleset in _rulesets:
            status = ruleset.attribute_values.get('status')
            if status:
                status_code = status.attribute_values.get('code')
                if status_code == STATUS_READY_TO_SCAN:
                    s3_path = ruleset.attribute_values.get('s3_path')
                    if s3_path:
                        _rulesets_to_backup.append(s3_path)
        return _rulesets_to_backup


HANDLER = ConfigurationBackupper(
    environment_service=SERVICE_PROVIDER.environment_service(),
    ssm_service=SERVICE_PROVIDER.ssm_service(),
    s3_client=SERVICE_PROVIDER.s3(),
    kms_client=SERVICE_PROVIDER.kms(),
    settings_service=SERVICE_PROVIDER.settings_service(),
    modular_service=SERVICE_PROVIDER.modular_service(),
    cached_iam_service=SERVICE_PROVIDER.iam_cache_service(),
    ruleset_service=SERVICE_PROVIDER.ruleset_service(),
    rulesource_service=SERVICE_PROVIDER.rule_source_service(),
    license_service=SERVICE_PROVIDER.license_service(),
)


def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
