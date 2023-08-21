from helpers import SingletonMeta
from services.batch_service import BatchService
from services.clients.event_bridge import EventBridgeClient
from services.clients.iam import IAMClient
from services.clients.license_manager import LicenseManagerClient
from services.clients.modular import ModularClient
from services.clients.s3 import S3Client
from services.clients.ssm import SSMClient, VaultSSMClient, AbstractSSMClient
from services.clients.sts import StsClient
from services.credentials_service import CredentialsService
from services.environment_service import EnvironmentService
from services.integration_service import IntegrationService
from services.job_updater_service import JobUpdaterService
from services.license_manager_service import LicenseManagerService
from services.modular_service import ModularService, TenantService
from services.notification_service import NotificationService
from services.os_service import OSService
from services.policy_service import PolicyService
from services.report_service import ReportService
from services.ruleset_service import RulesetService
from services.s3_service import S3Service
from services.s3_settings_service import CachedS3SettingsService
from services.scheduler_service import SchedulerService
from services.setting_service import SettingService
from services.ssm_service import SSMService
from services.statistics_service import StatisticsService
from services.token_service import TokenService


class ServiceProvider(metaclass=SingletonMeta):
    __ssm_conn = None
    __s3_conn = None
    __sts_conn = None
    __modular_conn = None
    __iam_conn = None

    __license_manager_client = None
    __event_bridge_client = None
    __ap_job_scheduler = None
    __event_bridge_scheduler = None
    __standalone_key_management = None

    __environment_service = None
    __configuration_service = None
    __modular_service = None
    __ssm_service = None
    __credentials_service = None
    __os_service = None
    __s3_service = None
    __ruleset_service = None
    __policy_service = None
    __batch_service = None
    __report_service = None
    __statistics_service = None
    __job_updater_service = None
    __license_manager_service = None
    __tenant_service = None
    __scheduler_service = None
    __token_service = None
    __setting_service = None
    __s3_setting_service = None
    __notification_service = None
    __integration_service = None

    def __str__(self):
        return id(self)

    def iam(self) -> IAMClient:
        if not self.__iam_conn:
            self.__iam_conn = IAMClient(sts_client=self.sts_client())
        return self.__iam_conn

    def ssm(self) -> AbstractSSMClient:
        if not self.__ssm_conn:
            _env = self.environment_service()
            if _env.is_docker():
                self.__ssm_conn = VaultSSMClient(environment_service=_env)
            else:
                self.__ssm_conn = SSMClient(environment_service=_env)
        return self.__ssm_conn

    def s3(self) -> S3Client:
        if not self.__s3_conn:
            self.__s3_conn = S3Client(
                environment_service=self.environment_service())
        return self.__s3_conn

    def sts_client(self) -> StsClient:
        if not self.__sts_conn:
            self.__sts_conn = StsClient(
                environment_service=self.environment_service())
        return self.__sts_conn

    def modular_client(self) -> ModularClient:
        if not self.__modular_conn:
            self.__modular_conn = ModularClient()
        return self.__modular_conn

    def license_manager_client(self) -> LicenseManagerClient:
        if not self.__license_manager_client:
            self.__license_manager_client = LicenseManagerClient(
                environment_service=self.environment_service(),
                setting_service=self.setting_service()
            )
        return self.__license_manager_client

    def event_bridge_client(self) -> EventBridgeClient:
        if not self.__event_bridge_client:
            self.__event_bridge_client = EventBridgeClient(
                environment_service=self.environment_service(),
                sts_client=self.sts_client()
            )
        return self.__event_bridge_client

    def ap_job_scheduler(self):
        if not self.__ap_job_scheduler:
            from services.clients.scheduler import APJobScheduler
            self.__ap_job_scheduler = APJobScheduler()
        return self.__ap_job_scheduler

    def event_bridge_scheduler(self):
        if not self.__event_bridge_scheduler:
            from services.clients.scheduler import EventBridgeJobScheduler
            self.__event_bridge_scheduler = EventBridgeJobScheduler(
                client=self.event_bridge_client()
            )
        return self.__event_bridge_scheduler

    def standalone_key_management(self):
        if not self.__standalone_key_management:
            from services.clients.standalone_key_management import \
                StandaloneKeyManagementClient
            self.__standalone_key_management = \
                StandaloneKeyManagementClient(ssm_client=self.ssm())
        return self.__standalone_key_management

    def environment_service(self) -> EnvironmentService:
        if not self.__environment_service:
            self.__environment_service = EnvironmentService()
        return self.__environment_service

    def modular_service(self) -> ModularService:
        if not self.__modular_service:
            self.__modular_service = ModularService(
                client=self.modular_client())
        return self.__modular_service

    def ssm_service(self) -> SSMService:
        if not self.__ssm_service:
            self.__ssm_service = SSMService(
                client=self.ssm(),
                environment_service=self.environment_service())
        return self.__ssm_service

    def credentials_service(self) -> CredentialsService:
        if not self.__credentials_service:
            self.__credentials_service = CredentialsService(
                ssm_service=self.ssm_service(),
                environment_service=self.environment_service(),
                sts_client=self.sts_client())
        return self.__credentials_service

    def os_service(self) -> OSService:
        if not self.__os_service:
            self.__os_service = OSService()
        return self.__os_service

    def s3_service(self) -> S3Service:
        if not self.__s3_service:
            self.__s3_service = S3Service(client=self.s3())
        return self.__s3_service

    def ruleset_service(self) -> RulesetService:
        if not self.__ruleset_service:
            self.__ruleset_service = RulesetService(
                environment_service=self.environment_service()
            )
        return self.__ruleset_service

    def policy_service(self) -> PolicyService:
        if not self.__policy_service:
            self.__policy_service = PolicyService(
                environment_service=self.environment_service(),
                s3_service=self.s3_service(),
                ruleset_service=self.ruleset_service()
            )
        return self.__policy_service

    def batch_service(self) -> BatchService:
        if not self.__batch_service:
            self.__batch_service = BatchService(
                environment_service=self.environment_service())
        return self.__batch_service

    def report_service(self) -> ReportService:
        if not self.__report_service:
            self.__report_service = ReportService(
                os_service=self.os_service(),
                s3_client=self.s3(),
                environment_service=self.environment_service(),
                s3_settings_service=self.s3_setting_service()
            )
        return self.__report_service

    def statistics_service(self) -> StatisticsService:
        if not self.__statistics_service:
            self.__statistics_service = StatisticsService(
                s3_service=self.s3_service(),
                environment_service=self.environment_service()
            )
        return self.__statistics_service

    def job_updater_service(self) -> JobUpdaterService:
        if not self.__job_updater_service:
            self.__job_updater_service = JobUpdaterService(
                environment_service=self.environment_service(),
                license_manager_service=self.license_manager_service(),
                tenant_service=self.tenant_service()
            )
        return self.__job_updater_service

    def license_manager_service(self) -> LicenseManagerService:
        if not self.__license_manager_service:
            self.__license_manager_service = LicenseManagerService(
                license_manager_client=self.license_manager_client(),
                token_service=self.token_service()
            )
        return self.__license_manager_service

    def tenant_service(self) -> TenantService:
        if not self.__tenant_service:
            self.__tenant_service = TenantService(
                modular_service=self.modular_service(),
                environment_service=self.environment_service()
            )
        return self.__tenant_service

    def scheduler_service(self) -> SchedulerService:
        if not self.__scheduler_service:
            self.__scheduler_service = SchedulerService(
                client=self.ap_job_scheduler()
                if self.environment_service().is_docker() else
                self.event_bridge_scheduler()
            )
        return self.__scheduler_service

    def token_service(self) -> TokenService:
        if not self.__token_service:
            self.__token_service = TokenService(
                client=self.standalone_key_management()
            )
        return self.__token_service

    def setting_service(self) -> SettingService:
        if not self.__setting_service:
            self.__setting_service = SettingService()
        return self.__setting_service

    def s3_setting_service(self) -> CachedS3SettingsService:
        if not self.__s3_setting_service:
            self.__s3_setting_service = CachedS3SettingsService(
                s3_client=self.s3(),
                environment_service=self.environment_service()
            )
        return self.__s3_setting_service

    def notification_service(self) -> NotificationService:
        if not self.__notification_service:
            self.__notification_service = NotificationService(
                setting_service=self.setting_service(),
                ssm_service=self.ssm_service(),
                s3_service=self.s3_service())
        return self.__notification_service

    def integration_service(self) -> IntegrationService:
        if not self.__integration_service:
            self.__integration_service = IntegrationService(
                modular_service=self.modular_service(),
                ssm_service=self.ssm_service()
            )
        return self.__integration_service
