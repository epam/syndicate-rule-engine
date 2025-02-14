from abc import ABC, abstractmethod
from functools import cached_property
from typing import Type

import requests
from botocore.exceptions import ClientError
from modular_sdk.commons.constants import ApplicationType
from modular_sdk.modular import Modular
from modular_sdk.services.impl.maestro_credentials_service import AccessMeta
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from helpers.constants import (
    CUSTOMER_ATTR,
    DEFAULT_SYSTEM_CUSTOMER,
    ED_AWS_RULESET_NAME,
    ED_AZURE_RULESET_NAME,
    ED_GOOGLE_RULESET_NAME,
    HOST_ATTR,
    PRIVATE_KEY_SECRET_NAME,
    HealthCheckStatus,
    RuleDomain,
    S3SettingKey,
    SettingKey,
)
from helpers.log_helper import get_logger
from helpers.system_customer import SYSTEM_CUSTOMER
from services import SERVICE_PROVIDER
from services.clients.lm_client import LmTokenProducer
from services.clients.s3 import S3Client
from services.clients.ssm import AbstractSSMClient, VaultSSMClient
from services.environment_service import EnvironmentService
from services.license_manager_service import LicenseManagerService
from services.ruleset_service import RulesetService
from services.setting_service import SettingsService

_LOG = get_logger(__name__)


class CheckResult(BaseModel):
    id: str
    status: HealthCheckStatus = HealthCheckStatus.OK
    details: dict = Field(default_factory=dict)
    remediation: str | None = None
    impact: str | None = None

    model_config = ConfigDict(use_enum_values=True)

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
    def remediation(cls) -> str | None:
        """
        Actions in case the check is failed
        :return:
        """
        return

    @classmethod
    def impact(cls) -> str | None:
        """
        Harm in case the check is failed
        :return:
        """
        return

    def ok_result(self, details: dict | None = None) -> CheckResult:
        return CheckResult(
            id=self.identifier(),
            status=HealthCheckStatus.OK,
            details=details or {},
        )

    def not_ok_result(self, details: dict | None = None) -> CheckResult:
        return CheckResult(
            id=self.identifier(),
            status=HealthCheckStatus.NOT_OK,
            details=details or {},
            remediation=self.remediation(),
            impact=self.impact(),
        )

    def unknown_result(self, details: dict | None = None) -> CheckResult:
        return CheckResult(
            id=self.identifier(),
            status=HealthCheckStatus.UNKNOWN,
            details=details or {},
        )

    @abstractmethod
    def check(self, **kwargs) -> CheckResult:
        """
        Must check a certain aspect of the service
        :return: CheckResult
        """


class SystemCustomerSettingCheck(AbstractHealthCheck):
    def __init__(self, settings_service: SettingsService):
        self._settings_service = settings_service

    @classmethod
    def build(cls) -> 'SystemCustomerSettingCheck':
        return cls(settings_service=SERVICE_PROVIDER.settings_service)

    @classmethod
    def identifier(cls) -> str:
        return 'system_customer_setting'

    def check(self, **kwargs) -> CheckResult:
        name = (
            self._settings_service.get(SettingKey.SYSTEM_CUSTOMER)
            or DEFAULT_SYSTEM_CUSTOMER
        )
        return self.ok_result(details={'name': name})


class LicenseManagerIntegrationCheck(AbstractHealthCheck):
    def __init__(self, settings_service: SettingsService):
        self._settings_service = settings_service

    @classmethod
    def build(cls) -> 'LicenseManagerIntegrationCheck':
        return cls(settings_service=SERVICE_PROVIDER.settings_service)

    @classmethod
    def remediation(cls) -> str | None:
        return (
            'Make sure License Manager is running. Execute '
            '`c7n setting lm config add --host` to set license manager link'
        )

    @classmethod
    def impact(cls) -> str | None:
        return (
            'The installation of service does not have '
            'access to License Manager API'
        )

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
            return self.not_ok_result(
                details={
                    'host': url,
                    'error': 'Failed to establish connection',
                }
            )
        return self.ok_result(details={'host': url})


class LicenseManagerClientKeyCheck(AbstractHealthCheck):
    def __init__(
        self,
        settings_service: SettingsService,
        license_manager_service: LicenseManagerService,
        ssm: AbstractSSMClient,
    ):
        self._settings_service = settings_service
        self._license_manager_service = license_manager_service
        self._ssm = ssm

    @classmethod
    def build(cls) -> 'LicenseManagerClientKeyCheck':
        return cls(
            settings_service=SERVICE_PROVIDER.settings_service,
            license_manager_service=SERVICE_PROVIDER.license_manager_service,
            ssm=SERVICE_PROVIDER.ssm,
        )

    @classmethod
    def remediation(cls) -> str | None:
        return (
            'Create client on LM side and rotate keys. Then execute '
            '`c7n setting lm client add` to import the rotated key'
        )

    @classmethod
    def impact(cls) -> str | None:
        return (
            'License manager does not know about this custodian '
            'installation. It will no allow to sync licenses'
        )

    @classmethod
    def identifier(cls) -> str:
        return 'license_manager_client_key'

    def check(self, **kwargs) -> CheckResult:
        setting = self._settings_service.get_license_manager_client_key_data(
            value=True
        )
        if not isinstance(setting, dict) or 'kid' not in setting:
            return self.not_ok_result(details={'kid': None, 'secret': False})
        kid = setting['kid']
        ssm_name = LmTokenProducer.derive_client_private_key_id(kid=kid)
        if not self._ssm.get_secret_value(ssm_name):
            return self.not_ok_result(details={'kid': kid, 'secret': False})
        return self.ok_result(details={'kid': kid, 'secret': True})


class ReportDateMarkerSettingCheck(AbstractHealthCheck):
    def __init__(self, settings_service: SettingsService):
        self._settings_service = settings_service

    @classmethod
    def build(cls) -> 'ReportDateMarkerSettingCheck':
        return cls(settings_service=SERVICE_PROVIDER.settings_service)

    @classmethod
    def identifier(cls) -> str:
        return 'report_date_marker_setting'

    @classmethod
    def impact(cls) -> str | None:
        return 'Metric collecting will not work properly'

    @classmethod
    def remediation(cls) -> str | None:
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
            return self.not_ok_result(
                {
                    'error': f"setting '{SettingKey.REPORT_DATE_MARKER}' "
                    f'is not set'
                }
            )
        try:
            self.model(**setting)
        except ValidationError as e:
            return self.not_ok_result(
                {
                    'error': f"setting '{SettingKey.REPORT_DATE_MARKER}' "
                    f'is invalid'
                }
            )
        return self.ok_result()


class RabbitMQConnectionCheck(AbstractHealthCheck):
    def __init__(
        self, settings_service: SettingsService, modular_client: Modular
    ):
        self._settings_service = settings_service
        self._modular_client = modular_client

    @classmethod
    def build(cls) -> 'RabbitMQConnectionCheck':
        return cls(
            settings_service=SERVICE_PROVIDER.settings_service,
            modular_client=SERVICE_PROVIDER.modular_client,
        )

    @classmethod
    def identifier(cls) -> str:
        return 'rabbitmq_connection'

    @classmethod
    def impact(cls) -> str | None:
        return 'Customer won`t be able to send messages to Maestro'

    @classmethod
    def remediation(cls) -> str | None:
        return 'Setup RabbitMQ configuration for your customer'

    def _check_customer(self, customer: str) -> tuple[dict, bool]:
        """
        Returns details for one customer and boolean whether the check
        is successful
        :param customer:
        :return:
        """
        app = next(
            self._modular_client.application_service().list(
                customer=customer,
                _type=ApplicationType.RABBITMQ.value,
                limit=1,
                deleted=False,
            ),
            None,
        )
        if not app:
            return {
                customer: f'Application with type '
                f'{ApplicationType.RABBITMQ} not found'
            }, False
        creds = self._modular_client.maestro_credentials_service().get_by_application(
            app
        )
        if not creds:
            return {
                customer: 'Could not resolve RabbitMQ creds from application'
            }, False
        return {customer: 'OK'}, True

    def check(self, **kwargs) -> CheckResult:
        if kwargs.get(CUSTOMER_ATTR):
            customers = iter([kwargs[CUSTOMER_ATTR]])
        else:
            cs = self._modular_client.customer_service()
            customers = (c.name for c in cs.i_get_customer())
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
        return cls(ruleset_service=SERVICE_PROVIDER.ruleset_service)

    @classmethod
    def identifier(cls) -> str:
        return 'event_driven_rulesets'

    def check(self, **kwargs) -> CheckResult:
        details = {
            RuleDomain.AWS.value: False,
            RuleDomain.AZURE.value: False,
            RuleDomain.GCP.value: False,
        }
        aws = next(
            self._ruleset_service.iter_standard(
                customer=SYSTEM_CUSTOMER,
                name=ED_AWS_RULESET_NAME,
                cloud=RuleDomain.AWS.value,
                event_driven=True,
                limit=1,
            ),
            None,
        )
        azure = next(
            self._ruleset_service.iter_standard(
                customer=SYSTEM_CUSTOMER,
                name=ED_AZURE_RULESET_NAME,
                cloud=RuleDomain.AZURE.value,
                event_driven=True,
                limit=1,
            ),
            None,
        )
        gcp = next(
            self._ruleset_service.iter_standard(
                customer=SYSTEM_CUSTOMER,
                name=ED_GOOGLE_RULESET_NAME,
                cloud=RuleDomain.GCP.value,
                event_driven=True,
                limit=1,
            ),
            None,
        )
        if aws:
            details[RuleDomain.AWS.value] = True
        if azure:
            details[RuleDomain.AZURE.value] = True
        if gcp:
            details[RuleDomain.GCP.value] = True
        if all(details.values()):
            return self.ok_result(details=details)
        return self.not_ok_result(details=details)

    @classmethod
    def remediation(cls) -> str | None:
        """
        Actions in case the check is failed
        :return:
        """
        return (
            'Login as a system user and execute '
            '"c7n ruleset eventdriven add" for all three clouds'
        )

    @classmethod
    def impact(cls) -> str | None:
        """
        Harm in case the check is failed
        :return:
        """
        return 'Event-driven scans won`t work'


class RulesMetaAccessDataCheck(AbstractHealthCheck):
    def __init__(
        self, settings_service: SettingsService, ssm: AbstractSSMClient
    ):
        self._settings_service = settings_service
        self._ssm = ssm

    @classmethod
    def build(cls) -> 'RulesMetaAccessDataCheck':
        return cls(
            settings_service=SERVICE_PROVIDER.settings_service,
            ssm=SERVICE_PROVIDER.ssm,
        )

    @classmethod
    def identifier(cls) -> str:
        return 'rules_meta_access_data'

    @classmethod
    def remediation(cls) -> str | None:
        return (
            'Admin should set secret with access data to '
            'git repo containing rules meta'
        )

    @classmethod
    def impact(cls) -> str | None:
        return (
            'Custodian will not pull rules meta. '
            'Some features will be unavailable'
        )

    def check(self, **kwargs) -> CheckResult:
        secret_name = self._settings_service.rules_metadata_repo_access_data()
        secret_value = self._ssm.get_secret_value(secret_name)
        if not secret_value:
            return self.not_ok_result(
                {
                    'error': f'No secret with rules metadata access data found. '
                    f'Name: {secret_name} was looked up'
                }
            )
        if not isinstance(secret_value, (list, dict)):
            return self.not_ok_result(
                {'error': f'Invalid secret value: {type(secret_value)}'}
            )
        if isinstance(secret_value, dict):
            secret_value = [secret_value]
        return self.ok_result()


class RulesMetaCheck(AbstractHealthCheck):
    def __init__(self, s3_settings_service):
        self._s3_settings_service = s3_settings_service

    @classmethod
    def build(cls) -> 'RulesMetaCheck':
        return cls(s3_settings_service=SERVICE_PROVIDER.s3_settings_service)

    @classmethod
    def remediation(cls) -> str | None:
        return (
            'Configure rule-meta access data and invoke rule-meta-updater '
            'with an empty event to wait for cron invoke'
        )

    @classmethod
    def impact(cls) -> str | None:
        return 'Some features will not be available'

    @classmethod
    def identifier(cls) -> str:
        return 'rule_meta'

    def check(self, **kwargs) -> CheckResult:
        _all = set(self._s3_settings_service.ls())
        _required = {
            S3SettingKey.RULES_TO_SERVICE_SECTION.value,
            S3SettingKey.RULES_TO_SEVERITY.value,
            S3SettingKey.RULES_TO_STANDARDS.value,
            S3SettingKey.RULES_TO_MITRE.value,
            S3SettingKey.CLOUD_TO_RULES.value,
            S3SettingKey.HUMAN_DATA.value,
            S3SettingKey.AWS_STANDARDS_COVERAGE.value,
            S3SettingKey.AZURE_STANDARDS_COVERAGE.value,
            S3SettingKey.GOOGLE_STANDARDS_COVERAGE.value,
            S3SettingKey.AWS_EVENTS.value,
            S3SettingKey.AZURE_EVENTS.value,
            S3SettingKey.GOOGLE_EVENTS.value,
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
    def __init__(self, ssm: VaultSSMClient):
        self._ssm = ssm

    @classmethod
    def build(cls) -> 'VaultConnectionCheck':
        return cls(ssm=SERVICE_PROVIDER.ssm)

    @classmethod
    def remediation(cls) -> str | None:
        return (
            'Make sure, vault required envs are set to .env file. '
            'In case k8s installation check whether Vault k8s service is up'
        )

    @classmethod
    def impact(cls) -> str | None:
        return 'On-prem will not work properly'

    @classmethod
    def identifier(cls) -> str:
        return 'vault_connection'

    def check(self, **kwargs) -> CheckResult:
        client = self._ssm
        from hvac import Client

        assert isinstance(
            client.client, Client
        ), 'on-prem installation must use VaultSSMClient'
        client = client.client  # hvac
        try:
            authenticated = client.is_authenticated()
        except requests.exceptions.RequestException as e:
            _LOG.warning(f'Exception occurred trying to connect to vault: {e}')
            return self.not_ok_result({'error': 'Cannot connect to vault'})
        if not authenticated:
            return self.not_ok_result(
                {'error': 'Cannot authenticate to vault'}
            )
        return self.ok_result()


class MinioConnectionCheck(AbstractHealthCheck):
    def __init__(self, s3_client: S3Client):
        self._s3_client = s3_client

    @classmethod
    def build(cls) -> 'AbstractHealthCheck':
        return cls(s3_client=SERVICE_PROVIDER.s3)

    @classmethod
    def identifier(cls) -> str:
        return 'minio_connection'

    def check(self, **kwargs) -> CheckResult:
        # TODO check host and port
        try:
            next(self._s3_client.list_buckets())
        except ClientError as e:
            if e.response['Error']['Code'] in (
                'SignatureDoesNotMatch',
                'InvalidAccessKeyId',
            ):
                return self.not_ok_result(
                    {'error': 'Cannot authenticate to minio'}
                )
        return self.ok_result()


class MongoConnectionCheck(AbstractHealthCheck):
    @classmethod
    def identifier(cls) -> str:
        return 'mongodb_connection'

    def check(self, **kwargs) -> CheckResult:
        from models import BaseModel

        adapter = BaseModel.mongo_adapter()
        if (
            adapter
            and adapter.mongo_database is not None
            and adapter.mongo_database.client
        ):
            return self.ok_result()
        # TODO think how to check considering that for on-prem we make
        #  queries when the server starts
        return self.not_ok_result()


class AllS3BucketsExist(AbstractHealthCheck):
    def __init__(
        self, s3_client: S3Client, environment_service: EnvironmentService
    ):
        self._s3_client = s3_client
        self._environment_service = environment_service

    @classmethod
    def build(cls) -> 'AllS3BucketsExist':
        return cls(
            s3_client=SERVICE_PROVIDER.s3,
            environment_service=SERVICE_PROVIDER.environment_service,
        )

    @classmethod
    def impact(cls) -> str | None:
        return 'Depending on missing buckets some features may not work'

    @classmethod
    def remediation(cls) -> str | None:
        return (
            'Set bucket names to .env and execute `main.py '
            'create_buckets`. For saas deploy the buckets'
        )

    @classmethod
    def identifier(cls) -> str:
        return 'buckets_exist'

    @cached_property
    def bucket_names(self) -> list[str]:
        return [
            name
            for name in (
                self._environment_service.get_statistics_bucket_name(),
                self._environment_service.get_rulesets_bucket_name(),
                self._environment_service.default_reports_bucket_name(),
                self._environment_service.get_metrics_bucket_name(),
            )
            if name
        ]

    def check(self, **kwargs) -> CheckResult:
        availability = {}
        for name in self.bucket_names:
            availability[name] = self._s3_client.bucket_exists(name)
        if all(availability.values()):
            return self.ok_result()
        return self.not_ok_result(availability)


class VaultAuthTokenIsSetCheck(AbstractHealthCheck):
    def __init__(self, ssm: VaultSSMClient):
        self._ssm = ssm

    @classmethod
    def build(cls) -> 'VaultAuthTokenIsSetCheck':
        return cls(ssm=SERVICE_PROVIDER.ssm)

    @classmethod
    def remediation(cls) -> str | None:
        return 'Execute `main.py init_vault`'

    @classmethod
    def impact(cls) -> str | None:
        return 'On-prem authentication will not work'

    @classmethod
    def identifier(cls) -> str:
        return 'vault_auth_token'

    def check(self, **kwargs) -> CheckResult:
        # lambda package does not include `onprem`
        if not self._ssm.is_secrets_engine_enabled():
            return self.not_ok_result(
                details={'private_key': False, 'secrets_engine': False}
            )
        token = self._ssm.get_secret_value(PRIVATE_KEY_SECRET_NAME)
        if not token or not isinstance(token, str):
            return self.not_ok_result(
                details={
                    'private_key': False,
                    'secrets_engine': True,
                    'error': 'Private key does not exist or invalid',
                }
            )
        return self.ok_result(details={'token': True, 'secrets_engine': True})


class LiveCheck(AbstractHealthCheck):
    @classmethod
    def build(cls) -> 'LiveCheck':
        return cls()

    @classmethod
    def remediation(cls) -> str | None:
        # this message will probably never be shown
        return 'Restart the container'

    @classmethod
    def impact(cls) -> str | None:
        return 'Nothing works'

    @classmethod
    def identifier(cls) -> str:
        return 'live'

    def check(self, **kwargs) -> CheckResult:
        # just a formality
        return self.ok_result()
