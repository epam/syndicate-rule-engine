import json
from abc import ABC, abstractmethod
from functools import cached_property
from typing import Dict, Optional, Type, Tuple, List

import requests
from botocore.exceptions import ClientError
from modular_sdk.commons.constants import RABBITMQ_TYPE, SIEM_DEFECT_DOJO_TYPE
from modular_sdk.services.impl.maestro_credentials_service import \
    DefectDojoApplicationMeta, DefectDojoApplicationSecret, AccessMeta
from pydantic import BaseModel, Field, ValidationError

from helpers.constants import AWS_CLOUD_ATTR, AZURE_CLOUD_ATTR, \
    GCP_CLOUD_ATTR, DEFAULT_SYSTEM_CUSTOMER, HOST_ATTR, CUSTOMER_ATTR, \
    ED_AWS_RULESET_NAME, \
    ED_AZURE_RULESET_NAME, ED_GOOGLE_RULESET_NAME, \
    KEY_RULES_TO_SERVICE_SECTION, KEY_RULES_TO_SEVERITY, \
    KEY_RULES_TO_STANDARDS, KEY_RULES_TO_MITRE, KEY_CLOUD_TO_RULES, \
    KEY_HUMAN_DATA, KEY_AWS_STANDARDS_COVERAGE, KEY_AZURE_STANDARDS_COVERAGE, \
    KEY_GOOGLE_STANDARDS_COVERAGE, KEY_AWS_EVENTS, KEY_AZURE_EVENTS, \
    KEY_GOOGLE_EVENTS, HealthCheckStatus
from helpers.log_helper import get_logger
from helpers.system_customer import SYSTEM_CUSTOMER
from integrations.defect_dojo_adapter import DefectDojoAdapter
from models.modular.application import Application
from services import SERVICE_PROVIDER
from services.clients.s3 import S3Client
from services.clients.ssm import VaultSSMClient
from services.environment_service import EnvironmentService
from services.license_manager_service import LicenseManagerService
from services.modular_service import ModularService
from services.ruleset_service import RulesetService
from services.setting_service import SettingsService, KEY_SYSTEM_CUSTOMER, \
    KEY_REPORT_DATE_MARKER
from services.ssm_service import SSMService

_LOG = get_logger(__name__)


class CheckResult(BaseModel):
    id: str
    status: HealthCheckStatus = HealthCheckStatus.OK
    details: Optional[Dict] = Field(default_factory=dict)
    remediation: Optional[str]
    impact: Optional[str]

    class Config:
        use_enum_values = True

    def is_ok(self) -> bool:
        return self.status == HealthCheckStatus.OK


class AbstractHealthCheck(ABC):
    @classmethod
    def build(cls) -> 'AbstractHealthCheck':
        """
        Builds the instance of the class
        """
        return cls()

    @classmethod
    @abstractmethod
    def identifier(cls) -> str:
        """
        Returns the identifier of a certain check
        :return: str
        """

    @classmethod
    def remediation(cls) -> Optional[str]:
        """
        Actions in case the check is failed
        :return:
        """
        return

    @classmethod
    def impact(cls) -> Optional[str]:
        """
        Harm in case the check is failed
        :return:
        """
        return

    def ok_result(self, details: Optional[dict] = None) -> CheckResult:
        return CheckResult(
            id=self.identifier(),
            status=HealthCheckStatus.OK,
            details=details,
        )

    def not_ok_result(self, details: Optional[dict] = None) -> CheckResult:
        return CheckResult(
            id=self.identifier(),
            status=HealthCheckStatus.NOT_OK,
            details=details,
            remediation=self.remediation(),
            impact=self.impact()
        )

    def unknown_result(self, details: Optional[dict] = None) -> CheckResult:
        return CheckResult(
            id=self.identifier(),
            status=HealthCheckStatus.UNKNOWN,
            details=details
        )

    @abstractmethod
    def check(self, **kwargs) -> CheckResult:
        """
        Must check a certain aspect of the service
        :return: CheckResult
        """


# class CoveragesSetToS3SettingsCheck(AbstractHealthCheck):
#     @classmethod
#     def remediation(cls) -> Optional[str]:
#         return 'Parse coverages mappings from excell and set them ' \
#                'using `main.py env update_settings`'
#
#     @classmethod
#     def impact(cls) -> Optional[str]:
#         return 'You will not be able to generate compliance report for ' \
#                'the cloud for which coverages mapping is missing'
#
#     @classmethod
#     def identifier(cls) -> str:
#         return 'coverages_setting'
#
#     def __init__(self, s3_settings_service: S3SettingsService):
#         self._s3_settings_service = s3_settings_service
#
#     @classmethod
#     def build(cls) -> 'CoveragesSetToS3SettingsCheck':
#         return cls(
#             s3_settings_service=SERVICE_PROVIDER.s3_settings_service()
#         )
#
#     def check(self, **kwargs) -> CheckResult:
#         details = {
#             AWS_CLOUD_ATTR: True,
#             AZURE_CLOUD_ATTR: True,
#             GCP_CLOUD_ATTR: True
#         }
#         for cloud in details:
#             key = S3_KEY_SECURITY_STANDARDS_COVERAGE.format(cloud=cloud)
#             coverage = self._s3_settings_service.get(key)
#             if not coverage:
#                 details[cloud] = False
#
#         if all(details.values()):
#             return self.ok_result(details=details)
#         return self.not_ok_result(
#             details=details
#         )


class SystemCustomerSettingCheck(AbstractHealthCheck):
    def __init__(self, settings_service: SettingsService):
        self._settings_service = settings_service

    @classmethod
    def build(cls) -> 'SystemCustomerSettingCheck':
        return cls(
            settings_service=SERVICE_PROVIDER.settings_service()
        )

    @classmethod
    def identifier(cls) -> str:
        return 'system_customer_setting'

    def check(self, **kwargs) -> CheckResult:
        name = (self._settings_service.get(KEY_SYSTEM_CUSTOMER) or
                DEFAULT_SYSTEM_CUSTOMER)
        return self.ok_result(details={
            'name': name
        })


class LicenseManagerIntegrationCheck(AbstractHealthCheck):
    def __init__(self, settings_service: SettingsService):
        self._settings_service = settings_service

    @classmethod
    def build(cls) -> 'LicenseManagerIntegrationCheck':
        return cls(
            settings_service=SERVICE_PROVIDER.settings_service()
        )

    @classmethod
    def remediation(cls) -> Optional[str]:
        return 'Make sure License Manager is running. Execute ' \
               '`c7n setting lm config add --host` to set license manager link'

    @classmethod
    def impact(cls) -> Optional[str]:
        return 'The installation of service does not have ' \
               'access to License Manager API'

    @classmethod
    def identifier(cls) -> str:
        return 'license_manager_integration'

    def check(self, **kwargs) -> CheckResult:
        setting = self._settings_service.get_license_manager_access_data()
        if not isinstance(setting, dict) or not setting.get(HOST_ATTR):
            return self.not_ok_result(details={'host': None})
        url = AccessMeta.from_dict(setting).url
        try:
            requests.get(url, timeout=5)
        except requests.exceptions.RequestException as e:
            _LOG.warning(f'Exception occurred trying to connect to lm: {e}')
            return self.not_ok_result(details={
                'host': url,
                'error': 'Failed to establish connection'
            })
        return self.ok_result(details={'host': url})


class LicenseManagerClientKeyCheck(AbstractHealthCheck):
    def __init__(self, settings_service: SettingsService,
                 license_manager_service: LicenseManagerService,
                 ssm_service: SSMService):
        self._settings_service = settings_service
        self._license_manager_service = license_manager_service
        self._ssm_service = ssm_service

    @classmethod
    def build(cls) -> 'LicenseManagerClientKeyCheck':
        return cls(
            settings_service=SERVICE_PROVIDER.settings_service(),
            license_manager_service=SERVICE_PROVIDER.license_manager_service(),
            ssm_service=SERVICE_PROVIDER.ssm_service()
        )

    @classmethod
    def remediation(cls) -> Optional[str]:
        return 'Create client on LM side and rotate keys. Then execute ' \
               '`c7n setting lm client add` to import the rotated key'

    @classmethod
    def impact(cls) -> Optional[str]:
        return 'License manager does not know about this custodian ' \
               'installation. It will no allow to sync licenses'

    @classmethod
    def identifier(cls) -> str:
        return 'license_manager_client_key'

    def check(self, **kwargs) -> CheckResult:
        setting = self._settings_service.get_license_manager_client_key_data(
            value=True)
        if not isinstance(setting, dict) or 'kid' not in setting:
            return self.not_ok_result(details={
                'kid': None,
                'secret': False
            })
        kid = setting['kid']
        ssm_name = self._license_manager_service.derive_client_private_key_id(
            kid=kid)
        if not self._ssm_service.get_secret_value(ssm_name):
            return self.not_ok_result(details={
                'kid': kid,
                'secret': False
            })
        return self.ok_result(details={
            'kid': kid,
            'secret': True
        })


class ReportDateMarkerSettingCheck(AbstractHealthCheck):
    def __init__(self, settings_service: SettingsService):
        self._settings_service = settings_service

    @classmethod
    def build(cls) -> 'ReportDateMarkerSettingCheck':
        return cls(
            settings_service=SERVICE_PROVIDER.settings_service()
        )

    @classmethod
    def identifier(cls) -> str:
        return 'report_date_marker_setting'

    @classmethod
    def impact(cls) -> Optional[str]:
        return 'Metric collecting will not work properly'

    @classmethod
    def remediation(cls) -> Optional[str]:
        return 'Execute `main.py env update_settings`'

    @cached_property
    def model(self) -> Type[BaseModel]:
        class Setting(BaseModel):
            last_week_date: str
            current_week_date: str

        return Setting

    def check(self, **kwargs) -> CheckResult:
        setting = self._settings_service.get_report_date_marker()
        if not setting:
            return self.not_ok_result({
                'error': f'setting \'{KEY_REPORT_DATE_MARKER}\' is not set'
            })
        try:
            self.model(**setting)
        except ValidationError as e:
            return self.not_ok_result({
                'error': f'setting \'{KEY_REPORT_DATE_MARKER}\' is invalid'
            })
        return self.ok_result()


class RabbitMQConnectionCheck(AbstractHealthCheck):
    def __init__(self, settings_service: SettingsService,
                 ssm_service: SSMService,
                 modular_service: ModularService):
        self._settings_service = settings_service
        self._ssm_service = ssm_service
        self._modular_service = modular_service

    @classmethod
    def build(cls) -> 'RabbitMQConnectionCheck':
        return cls(
            settings_service=SERVICE_PROVIDER.settings_service(),
            ssm_service=SERVICE_PROVIDER.ssm_service(),
            modular_service=SERVICE_PROVIDER.modular_service()
        )

    @classmethod
    def identifier(cls) -> str:
        return 'rabbitmq_connection'

    @classmethod
    def impact(cls) -> Optional[str]:
        return 'Customer won`t be able to send messages to Maestro'

    @classmethod
    def remediation(cls) -> Optional[str]:
        return 'Setup RabbitMQ configuration for your customer'

    def _check_customer(self, customer: str) -> Tuple[dict, bool]:
        """
        Returns details for one customer and boolean whether the check
        is successful
        :param customer:
        :return:
        """
        app = next(self._modular_service.get_applications(
            customer=customer,
            _type=RABBITMQ_TYPE,
            limit=1,
            deleted=False
        ), None)
        if not app:
            return {
                customer: f'Application with type {RABBITMQ_TYPE} not found'
            }, False
        creds = self._modular_service.modular_client.maestro_credentials_service(). \
            get_by_application(app)
        if not creds:
            return {
                customer: 'Could not resolve rabbitmq creds from application'
            }, False
        return {customer: 'OK'}, True

    def check(self, **kwargs) -> CheckResult:
        if kwargs.get(CUSTOMER_ATTR):
            customers = iter([kwargs[CUSTOMER_ATTR]])
        else:
            customers = (c.name for c in
                         self._modular_service.i_get_customers())
        details = {}
        ok = True
        for customer in customers:
            data, success = self._check_customer(customer)
            details.update(data)
            if not success:
                ok = False
        if ok:
            return self.ok_result(details)
        return self.not_ok_result(details)


class EventDrivenRulesetsExist(AbstractHealthCheck):
    def __init__(self, ruleset_service: RulesetService):
        self._ruleset_service = ruleset_service

    @classmethod
    def build(cls) -> 'AbstractHealthCheck':
        return cls(
            ruleset_service=SERVICE_PROVIDER.ruleset_service()
        )

    @classmethod
    def identifier(cls) -> str:
        return 'event_driven_rulesets'

    def check(self, **kwargs) -> CheckResult:
        details = {
            AWS_CLOUD_ATTR: False,
            AZURE_CLOUD_ATTR: False,
            GCP_CLOUD_ATTR: False
        }
        aws = next(self._ruleset_service.iter_standard(
            customer=SYSTEM_CUSTOMER,
            name=ED_AWS_RULESET_NAME,
            cloud=AWS_CLOUD_ATTR,
            event_driven=True,
            limit=1
        ), None)
        azure = next(self._ruleset_service.iter_standard(
            customer=SYSTEM_CUSTOMER,
            name=ED_AZURE_RULESET_NAME,
            cloud=AZURE_CLOUD_ATTR,
            event_driven=True,
            limit=1
        ), None)
        gcp = next(self._ruleset_service.iter_standard(
            customer=SYSTEM_CUSTOMER,
            name=ED_GOOGLE_RULESET_NAME,
            cloud=GCP_CLOUD_ATTR,
            event_driven=True,
            limit=1
        ), None)
        if aws:
            details[AWS_CLOUD_ATTR] = True
        if azure:
            details[AZURE_CLOUD_ATTR] = True
        if gcp:
            details[GCP_CLOUD_ATTR] = True
        if all(details.values()):
            return self.ok_result(details=details)
        return self.not_ok_result(
            details=details
        )

    @classmethod
    def remediation(cls) -> Optional[str]:
        """
        Actions in case the check is failed
        :return:
        """
        return 'Login as a system user and execute ' \
               '"c7n ruleset eventdriven add" for all three clouds'

    @classmethod
    def impact(cls) -> Optional[str]:
        """
        Harm in case the check is failed
        :return:
        """
        return 'Event-driven scans won`t work'


class DefectDojoCheck(AbstractHealthCheck):
    def __init__(self, modular_service: ModularService):
        self._modular_service = modular_service

    @classmethod
    def build(cls) -> 'DefectDojoCheck':
        return cls(
            modular_service=SERVICE_PROVIDER.modular_service()
        )

    @classmethod
    def identifier(cls) -> str:
        return 'defect_dojo_connection'

    def _check_application(self, application: Application) -> Tuple[str, bool]:
        raw_secret = self._modular_service.modular_client.assume_role_ssm_service().get_parameter(
            application.secret)
        if not raw_secret or not isinstance(raw_secret, dict):
            _LOG.debug(f'SSM Secret by name {application.secret} not found')
            return 'Application secret not found', False
        meta = DefectDojoApplicationMeta.from_dict(application.meta.as_dict())
        secret = DefectDojoApplicationSecret.from_dict(raw_secret)
        try:
            _LOG.info('Initializing dojo client')
            DefectDojoAdapter(
                host=meta.url,
                api_key=secret.api_key,
                # todo get other configuration from parent meta
            )
            return 'OK', True
        except requests.RequestException as e:
            return str(e), False

    def _check_customer(self, customer: str) -> Tuple[dict, bool]:
        parents = self._modular_service.get_customer_bound_parents(
            customer=customer,
            parent_type=SIEM_DEFECT_DOJO_TYPE,
            is_deleted=False
        )
        result = {}
        ok = True
        for aid in (parent.application_id for parent in parents):
            app = self._modular_service.get_application(aid)
            if not app or app.is_deleted:
                result[aid], ok = 'Application not found', False

            else:
                result[aid], _ok = self._check_application(app)
                ok &= _ok
        return {customer: result}, ok

    def check(self, **kwargs) -> CheckResult:
        if kwargs.get(CUSTOMER_ATTR):
            customers = iter([kwargs[CUSTOMER_ATTR]])
        else:
            customers = (c.name for c in
                         self._modular_service.i_get_customers())
        details = {}
        ok = True
        for customer in customers:
            data, success = self._check_customer(customer)
            details.update(data)
            if not success:
                ok = False
        if ok:
            return self.ok_result(details)
        return self.not_ok_result(details)


class RulesMetaAccessDataCheck(AbstractHealthCheck):
    def __init__(self, settings_service: SettingsService,
                 ssm_service: SSMService):
        self._settings_service = settings_service
        self._ssm_service = ssm_service

    @classmethod
    def build(cls) -> 'RulesMetaAccessDataCheck':
        return cls(
            settings_service=SERVICE_PROVIDER.settings_service(),
            ssm_service=SERVICE_PROVIDER.ssm_service()
        )

    @classmethod
    def identifier(cls) -> str:
        return 'rules_meta_access_data'

    @classmethod
    def remediation(cls) -> Optional[str]:
        return 'Admin should set secret with access data to ' \
               'git repo containing rules meta'

    @classmethod
    def impact(cls) -> Optional[str]:
        return 'Custodian will not pull rules meta. ' \
               'Some features will be unavailable'

    def check(self, **kwargs) -> CheckResult:
        secret_name = self._settings_service.rules_metadata_repo_access_data()
        secret_value = self._ssm_service.get_secret_value(secret_name)
        if not secret_value:
            return self.not_ok_result({
                'error': f'No secret with rules metadata access data found. '
                         f'Name: {secret_name} was looked up'
            })
        if not isinstance(secret_value, (list, dict)):
            return self.not_ok_result({
                'error': f'Invalid secret value: {type(secret_value)}'
            })
        if isinstance(secret_value, dict):
            secret_value = [secret_value]
        return self.ok_result()


class RulesMetaCheck(AbstractHealthCheck):
    def __init__(self, s3_settings_service):
        self._s3_settings_service = s3_settings_service

    @classmethod
    def build(cls) -> 'RulesMetaCheck':
        return cls(
            s3_settings_service=SERVICE_PROVIDER.s3_settings_service()
        )

    @classmethod
    def remediation(cls) -> Optional[str]:
        return 'Configure rule-meta access data and invoke rule-meta-updater ' \
               'with an empty event to wait for cron invoke'

    @classmethod
    def impact(cls) -> Optional[str]:
        return 'Some features will not be available'

    @classmethod
    def identifier(cls) -> str:
        return 'rule_meta'

    def check(self, **kwargs) -> CheckResult:
        _all = set(self._s3_settings_service.ls())
        _required = {
            KEY_RULES_TO_SERVICE_SECTION, KEY_RULES_TO_SEVERITY,
            KEY_RULES_TO_STANDARDS, KEY_RULES_TO_MITRE, KEY_CLOUD_TO_RULES,
            KEY_HUMAN_DATA, KEY_AWS_STANDARDS_COVERAGE,
            KEY_AZURE_STANDARDS_COVERAGE, KEY_GOOGLE_STANDARDS_COVERAGE,
            KEY_AWS_EVENTS, KEY_AZURE_EVENTS, KEY_GOOGLE_EVENTS
        }
        missing = _required - _all
        present = _required & _all
        if not missing:
            return self.ok_result({key: True for key in present})
        res = {key: True for key in present}
        res.update({key: False for key in missing})
        return self.not_ok_result(res)


# on-prem specific checks -----
class VaultConnectionCheck(AbstractHealthCheck):
    def __init__(self, ssm_service: SSMService):
        self._ssm_service = ssm_service

    @classmethod
    def build(cls) -> 'VaultConnectionCheck':
        return cls(
            ssm_service=SERVICE_PROVIDER.ssm_service()
        )

    @classmethod
    def remediation(cls) -> Optional[str]:
        return 'Make sure, vault required envs are set to .env file. ' \
               'In case k8s installation check whether Vault k8s service is up'

    @classmethod
    def impact(cls) -> Optional[str]:
        return 'On-prem will not work properly'

    @classmethod
    def identifier(cls) -> str:
        return 'vault_connection'

    def check(self, **kwargs) -> CheckResult:
        client = self._ssm_service.client
        assert isinstance(client, VaultSSMClient), \
            'on-prem installation must use VaultSSMClient'
        client = client.client  # hvac
        try:
            authenticated = client.is_authenticated()
        except requests.exceptions.RequestException as e:
            _LOG.warning(f'Exception occurred trying to connect to vault: {e}')
            return self.not_ok_result({
                'error': 'Cannot connect to vault'
            })
        if not authenticated:
            return self.not_ok_result({
                'error': 'Cannot authenticate to vault'
            })
        return self.ok_result()


class MinioConnectionCheck(AbstractHealthCheck):
    def __init__(self, s3_client: S3Client):
        self._s3_client = s3_client

    @classmethod
    def build(cls) -> 'AbstractHealthCheck':
        return cls(
            s3_client=SERVICE_PROVIDER.s3()
        )

    @classmethod
    def identifier(cls) -> str:
        return 'minio_connection'

    def check(self, **kwargs) -> CheckResult:
        # TODO check host and port
        try:
            self._s3_client.list_buckets()
        except ClientError as e:
            if e.response['Error']['Code'] in ('SignatureDoesNotMatch',
                                               'InvalidAccessKeyId'):
                return self.not_ok_result({
                    'error': 'Cannot authenticate to minio'
                })
        return self.ok_result()


class MongoConnectionCheck(AbstractHealthCheck):

    @classmethod
    def identifier(cls) -> str:
        return 'mongodb_connection'

    def check(self, **kwargs) -> CheckResult:
        from models.job import Job
        client = Job.mongodb_handler().mongodb.client
        # TODO think how to check considering that for on-prem we make
        #  queries when the server starts
        return self.ok_result()


class AllS3BucketsExist(AbstractHealthCheck):
    def __init__(self, s3_client: S3Client,
                 environment_service: EnvironmentService):
        self._s3_client = s3_client
        self._environment_service = environment_service

    @classmethod
    def build(cls) -> 'AllS3BucketsExist':
        return cls(
            s3_client=SERVICE_PROVIDER.s3(),
            environment_service=SERVICE_PROVIDER.environment_service()
        )

    @classmethod
    def impact(cls) -> Optional[str]:
        return 'Depending on missing buckets some features may not work'

    @classmethod
    def remediation(cls) -> Optional[str]:
        return 'Set bucket names to .env and execute `main.py ' \
               'create_buckets`. For saas deploy the buckets'

    @classmethod
    def identifier(cls) -> str:
        return 'buckets_exist'

    @cached_property
    def bucket_names(self) -> List[str]:
        return [name for name in (
            self._environment_service.get_statistics_bucket_name(),
            self._environment_service.get_ssm_backup_bucket(),
            self._environment_service.get_rulesets_bucket_name(),
            self._environment_service.default_reports_bucket_name(),
            self._environment_service.get_templates_bucket_name(),
            self._environment_service.get_metrics_bucket_name()
        ) if name]

    def check(self, **kwargs) -> CheckResult:
        availability = {}
        for name in self.bucket_names:
            availability[name] = self._s3_client.is_bucket_exists(name)
        if all(availability.values()):
            return self.ok_result()
        return self.not_ok_result(availability)


class VaultAuthTokenIsSetCheck(AbstractHealthCheck):
    def __init__(self, ssm_service: SSMService):
        self._ssm_service = ssm_service

    @classmethod
    def build(cls) -> 'VaultAuthTokenIsSetCheck':
        return cls(
            ssm_service=SERVICE_PROVIDER.ssm_service()
        )

    @classmethod
    def remediation(cls) -> Optional[str]:
        return 'Execute `main.py init_vault`'

    @classmethod
    def impact(cls) -> Optional[str]:
        return 'On-prem authentication will not work'

    @classmethod
    def identifier(cls) -> str:
        return 'vault_auth_token'

    def check(self, **kwargs) -> CheckResult:
        from connections.auth_extension.cognito_to_jwt_adapter import \
            AUTH_TOKEN_NAME
        # lambda package does not include `exported_module`
        if not self._ssm_service.is_secrets_engine_enabled():
            return self.not_ok_result(details={
                'token': False,
                'secrets_engine': False
            })
        token = self._ssm_service.get_secret_value(AUTH_TOKEN_NAME)
        if isinstance(token, str):
            try:
                payload = json.loads(token)
            except json.JSONDecodeError as e:
                return self.not_ok_result(details={
                    'token': True,
                    'secrets_engine': True,
                    'error': 'Invalid token JSON'
                })
        else:  # isinstance(token, dict)
            payload = token
        phrase = payload.get('phrase')
        if not phrase:
            return self.not_ok_result(details={
                'token': True,
                'secrets_engine': True,
                'error': '`phrase` key is missing'
            })
        return self.ok_result(details={'token': True, 'secrets_engine': True})
