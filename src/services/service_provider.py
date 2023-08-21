from helpers import SingletonMeta, get_logger

_LOG = get_logger(__name__)


class ServiceProvider(metaclass=SingletonMeta):
    # clients
    __kms_conn = None
    __s3_conn = None
    __batch_conn = None
    __ssm_conn = None
    __cognito_conn = None
    __cloudwatch_conn = None
    __ecr_conn = None
    __lambda_conn = None
    __sts_conn = None
    __modular_conn = None
    __license_manager_conn = None
    __standalone_key_management = None
    __event_bridge_client = None
    __iam_client = None
    __event_bridge_job_scheduler = None
    __ap_job_scheduler = None
    __smtp_conn = None
    __assume_role_s3 = None

    # services
    __job_service = None
    __user_service = None
    __rule_service = None
    __rule_meta_service = None
    __environment_service = None
    __modular_service = None
    __ssm_service = None
    __settings_service = None
    __mappings_collector = None
    __s3_settings_service = None
    __s3_settings_service_local_wrapper = None
    __access_control_service = None
    __iam_cache_service = None
    __credential_manager_service = None
    __event_processor_service = None
    __ruleset_service = None
    __rule_source_service = None
    __license_service = None
    __license_manager_service = None
    __findings_service = None
    __coverage_service = None
    __token_service = None
    __scheduler_service = None
    __restriction_service = None
    __priority_governance_service = None
    __notification_service = None
    __key_management_service = None
    __event_service = None
    __assemble_service = None
    __batch_results_service = None
    __ambiguous_job_service = None
    __report_service = None
    __rule_report_service = None
    __tenant_metrics_service = None
    __customer_metrics_service = None
    __rabbitmq_service = None
    __job_statistics_service = None

    def __str__(self):
        return id(self)

    # clients
    def kms(self):
        if not self.__kms_conn:
            from services.clients.kms import KMSClient
            self.__kms_conn = KMSClient(
                region=self.environment_service().aws_region())
        return self.__kms_conn

    def s3(self):
        # ModularAssumeRoleSSMService is a temporary solution
        if not self.__s3_conn:
            from services.clients.s3 import S3Client
            self.__s3_conn = S3Client(
                region=self.environment_service().aws_region())
        return self.__s3_conn

    def batch(self):
        if not self.__batch_conn:
            from connections.batch_extension.base_job_client import \
                BaseBatchClient
            self.__batch_conn = BaseBatchClient(
                environment_service=self.environment_service(),
                sts_client=self.sts_client())
        return self.__batch_conn

    def ssm(self):
        if not self.__ssm_conn:
            from services.clients.ssm import SSMClient, VaultSSMClient
            _env = self.environment_service()
            if _env.is_docker():
                self.__ssm_conn = VaultSSMClient(environment_service=_env)
            else:
                self.__ssm_conn = SSMClient(environment_service=_env)
        return self.__ssm_conn

    def cognito(self):
        if not self.__cognito_conn:
            if self.environment_service().is_docker():
                from connections.auth_extension.cognito_to_jwt_adapter \
                    import MongoAndSSMAuthClient
                self.__cognito_conn = MongoAndSSMAuthClient(
                    ssm_service=self.ssm_service()
                )
            else:
                from services.clients.cognito import CognitoClient
                self.__cognito_conn = CognitoClient(
                    environment_service=self.environment_service())
        return self.__cognito_conn

    def cloudwatch(self):
        if not self.__cloudwatch_conn:
            from connections.logs_extension.base_logs_client import \
                BaseLogsClient
            self.__cloudwatch_conn = BaseLogsClient(
                region=self.environment_service().aws_region())
        return self.__cloudwatch_conn

    def ecr(self):
        if not self.__ecr_conn:
            from services.clients.ecr import ECRClient
            self.__ecr_conn = ECRClient(
                region=self.environment_service().aws_region())
        return self.__ecr_conn

    def lambda_func(self):
        if not self.__lambda_conn:
            from services.clients.lambda_func import LambdaClient
            self.__lambda_conn = LambdaClient(
                environment_service=self.environment_service())
        return self.__lambda_conn

    def modular_client(self):
        if not self.__modular_conn:
            from services.clients.modular import ModularClient
            self.__modular_conn = ModularClient()
        return self.__modular_conn

    def license_manager_client(self):
        if not self.__license_manager_conn:
            from services.clients.license_manager import \
                LicenseManagerClient
            self.__license_manager_conn = LicenseManagerClient(
                setting_service=self.settings_service()
            )
        return self.__license_manager_conn

    def standalone_key_management(self):
        if not self.__standalone_key_management:
            from services.clients.standalone_key_management import \
                StandaloneKeyManagementClient
            self.__standalone_key_management = \
                StandaloneKeyManagementClient(ssm_client=self.ssm())
        return self.__standalone_key_management

    def events(self):
        if not self.__event_bridge_client:
            from services.clients.event_bridge import EventBridgeClient
            self.__event_bridge_client = EventBridgeClient(
                environment_service=self.environment_service(),
                sts_client=self.sts_client())
        return self.__event_bridge_client

    def iam(self):
        if not self.__iam_client:
            from services.clients.iam import IAMClient
            self.__iam_client = IAMClient(
                sts_client=self.sts_client()
            )
        return self.__iam_client

    def event_bridge_job_scheduler(self):
        if not self.__event_bridge_job_scheduler:
            from services.clients.scheduler import EventBridgeJobScheduler
            self.__event_bridge_job_scheduler = EventBridgeJobScheduler(
                client=self.events(),
                environment_service=self.environment_service(),
                iam_client=self.iam(),
                batch_client=self.batch()
            )
        return self.__event_bridge_job_scheduler

    def ap_job_scheduler(self):
        if not self.__ap_job_scheduler:
            from scheduler.ap_job_scheduler import APJobScheduler
            self.__ap_job_scheduler = APJobScheduler()
        return self.__ap_job_scheduler

    def smtp_client(self):
        if not self.__smtp_conn:
            from services.clients.smtp import SMTPClient
            self.__smtp_conn = SMTPClient()
        return self.__smtp_conn

    def assume_role_s3(self):
        if not self.__assume_role_s3:
            from services.clients.s3 import ModularAssumeRoleS3Service
            self.__assume_role_s3 = ModularAssumeRoleS3Service(
                region=self.environment_service().aws_region())
        return self.__assume_role_s3

    # services

    def job_service(self):
        if not self.__job_service:
            from services.job_service import JobService
            self.__job_service = JobService(
                restriction_service=self.restriction_service()
            )
        return self.__job_service

    def user_service(self):
        if not self.__user_service:
            from services.user_service import CognitoUserService
            self.__user_service = CognitoUserService(
                client=self.cognito())
        return self.__user_service

    def rule_service(self):
        if not self.__rule_service:
            from services.rule_meta_service import RuleService
            self.__rule_service = RuleService(
                mappings_collector=self.mappings_collector()
            )
        return self.__rule_service

    def rule_meta_service(self):
        if not self.__rule_meta_service:
            from services.rule_meta_service import RuleMetaService
            self.__rule_meta_service = RuleMetaService()
        return self.__rule_meta_service

    def environment_service(self):
        if not self.__environment_service:
            from services.environment_service import EnvironmentService
            self.__environment_service = EnvironmentService()
        return self.__environment_service

    # def azure_subscriptions_service(self):
    #     from services.azure_subscriptions_service import \
    #         AzureSubscriptionsService
    #     if not self.__azure_subscriptions_service:
    #         self.__azure_subscriptions_service = \
    #             AzureSubscriptionsService()
    #     return self.__azure_subscriptions_service

    def modular_service(self):
        if not self.__modular_service:
            from services.modular_service import ModularService
            self.__modular_service = ModularService(
                client=self.modular_client())
        return self.__modular_service

    def credential_manager_service(self):
        if not self.__credential_manager_service:
            from services.credentials_manager_service import \
                CredentialsManagerService
            self.__credential_manager_service = CredentialsManagerService()
        return self.__credential_manager_service

    def ssm_service(self):
        if not self.__ssm_service:
            from services.ssm_service import SSMService
            self.__ssm_service = SSMService(client=self.ssm())
        return self.__ssm_service

    def settings_service(self):
        if not self.__settings_service:
            from services.setting_service import CachedSettingsService
            self.__settings_service = CachedSettingsService(
                environment_service=self.environment_service()
            )
        return self.__settings_service

    def mappings_collector(self):
        if not self.__mappings_collector:
            from services.rule_meta_service import \
                LazyLoadedMappingsCollector, MappingsCollector
            self.__mappings_collector = LazyLoadedMappingsCollector(
                collector=MappingsCollector(),
                settings_service=self.settings_service(),
                s3_settings_service=self.s3_settings_service(),
                abort_if_not_found=True
            )
        return self.__mappings_collector

    def s3_settings_service(self):
        if not self.__s3_settings_service:
            from services.s3_settings_service import S3SettingsService
            self.__s3_settings_service = S3SettingsService(
                s3_client=self.s3(),
                environment_service=self.environment_service()
            )
        return self.__s3_settings_service

    def s3_setting_service_local_wrapper(self):
        if not self.__s3_settings_service_local_wrapper:
            from services.s3_settings_service import \
                S3SettingsServiceLocalWrapper
            self.__s3_settings_service_local_wrapper = S3SettingsServiceLocalWrapper(
                s3_setting_service=self.s3_settings_service()
            )
        return self.__s3_settings_service_local_wrapper

    def access_control_service(self):
        if not self.__access_control_service:
            from services.rbac.access_control_service import \
                AccessControlService
            self.__access_control_service = AccessControlService(
                iam_service=self.iam_cache_service()
            )
        return self.__access_control_service

    def iam_cache_service(self):
        if not self.__iam_cache_service:
            from services.rbac.iam_cache_service import CachedIamService
            cache_lifetime = self.environment_service() \
                .get_iam_cache_lifetime()
            self.__iam_cache_service = CachedIamService(
                cache_lifetime=cache_lifetime
            )
        return self.__iam_cache_service

    def event_processor_service(self):
        if not self.__event_processor_service:
            from services.event_processor_service import \
                EventProcessorService
            self.__event_processor_service = EventProcessorService(
                s3_settings_service=self.s3_settings_service(),
                environment_service=self.environment_service(),
                sts_client=self.sts_client(),
                mappings_collector=self.mappings_collector()
            )
        return self.__event_processor_service

    def sts_client(self):
        if not self.__sts_conn:
            from services.clients.sts import StsClient
            self.__sts_conn = StsClient(
                environment_service=self.environment_service())
        return self.__sts_conn

    def ruleset_service(self):
        if not self.__ruleset_service:
            from services.ruleset_service import RulesetService
            self.__ruleset_service = RulesetService(
                license_service=self.license_service(),
                restriction_service=self.restriction_service(),
                s3_client=self.s3()
            )
        return self.__ruleset_service

    def rule_source_service(self):
        if not self.__rule_source_service:
            from services.rule_source_service import RuleSourceService
            self.__rule_source_service = RuleSourceService(
                ssm_service=self.ssm_service(),
                restriction_service=self.restriction_service()
            )
        return self.__rule_source_service

    def license_service(self):
        if not self.__license_service:
            from services.license_service import LicenseService
            self.__license_service = LicenseService(
                settings_service=self.settings_service()
            )
        return self.__license_service

    def license_manager_service(self):
        if not self.__license_manager_service:
            from services.license_manager_service import \
                LicenseManagerService
            self.__license_manager_service = LicenseManagerService(
                license_manager_client=self.license_manager_client(),
                token_service=self.token_service()
            )
        return self.__license_manager_service

    def findings_service(self):
        if not self.__findings_service:
            from services.findings_service import FindingsService
            self.__findings_service = FindingsService(
                environment_service=self.environment_service(),
                s3_client=self.s3()
            )
        return self.__findings_service

    def coverage_service(self):
        if not self.__coverage_service:
            from services.coverage_service import CoverageService
            self.__coverage_service = CoverageService(
                mappings_collector=self.mappings_collector()
            )
        return self.__coverage_service

    def token_service(self):
        if not self.__token_service:
            from services.token_service import TokenService
            self.__token_service = TokenService(
                client=self.standalone_key_management()
            )
        return self.__token_service

    def scheduler_service(self):
        if not self.__scheduler_service:
            from services.scheduler_service import SchedulerService
            client = self.event_bridge_job_scheduler() \
                if not self.environment_service().is_docker() \
                else self.ap_job_scheduler()
            self.__scheduler_service = SchedulerService(
                client=client
            )
        return self.__scheduler_service

    def restriction_service(self):
        if not self.__restriction_service:
            from services.rbac.restriction_service import \
                RestrictionService
            self.__restriction_service = RestrictionService(
                modular_service=self.modular_service()
            )
        return self.__restriction_service

    def priority_governance_service(self):
        if not self.__priority_governance_service:
            from services.rbac.governance.priority_governance_service import \
                PriorityGovernanceService
            self.__priority_governance_service = PriorityGovernanceService(
                modular_service=self.modular_service()
            )
        return self.__priority_governance_service

    def notification_service(self):
        if not self.__notification_service:
            from services.notification_service import NotificationService
            self.__notification_service = NotificationService(
                setting_service=self.settings_service(),
                s3_service=self.s3(),
                ssm_service=self.ssm_service()
            )
        return self.__notification_service

    def key_management_service(self):
        if not self.__key_management_service:
            from services.key_management_service import \
                KeyManagementService
            self.__key_management_service = KeyManagementService(
                key_management_client=self.standalone_key_management()
            )
        return self.__key_management_service

    def event_service(self):
        if not self.__event_service:
            from services.event_service import EventService
            self.__event_service = EventService(
                environment_service=self.environment_service()
            )
        return self.__event_service

    def assemble_service(self):
        if not self.__assemble_service:
            from services.assemble_service import AssembleService
            self.__assemble_service = AssembleService(
                environment_service=self.environment_service(),
            )
        return self.__assemble_service

    def batch_results_service(self):
        if not self.__batch_results_service:
            from services.batch_results_service import BatchResultsService
            self.__batch_results_service = BatchResultsService()
        return self.__batch_results_service

    def ambiguous_job_service(self):
        if not self.__ambiguous_job_service:
            from services.ambiguous_job_service import AmbiguousJobService
            self.__ambiguous_job_service = AmbiguousJobService(
                batch_results_service=self.batch_results_service(),
                job_service=self.job_service()
            )
        return self.__ambiguous_job_service

    def report_service(self):
        if not self.__report_service:
            from services.report_service import ReportService
            self.__report_service = ReportService(
                s3_client=self.s3(),
                environment_service=self.environment_service(),
                mappings_collector=self.mappings_collector()
            )
        return self.__report_service

    def rule_report_service(self):
        if not self.__rule_report_service:
            from services.rule_report_service import RuleReportService
            self.__rule_report_service = RuleReportService(
                ambiguous_job_service=self.ambiguous_job_service(),
                modular_service=self.modular_service(),
                report_service=self.report_service()
            )
        return self.__rule_report_service

    def tenant_metrics_service(self):
        if not self.__tenant_metrics_service:
            from services.metrics_service import TenantMetricsService
            self.__tenant_metrics_service = TenantMetricsService()
        return self.__tenant_metrics_service

    def customer_metrics_service(self):
        if not self.__customer_metrics_service:
            from services.metrics_service import CustomerMetricsService
            self.__customer_metrics_service = CustomerMetricsService()
        return self.__customer_metrics_service

    def rabbitmq_service(self):
        if not self.__rabbitmq_service:
            from services.rabbitmq_service import RabbitMQService
            self.__rabbitmq_service = RabbitMQService(
                modular_service=self.modular_service()
            )
        return self.__rabbitmq_service

    def job_statistics_service(self):
        if not self.__job_statistics_service:
            from services.job_statistics_service import JobStatisticsService
            self.__job_statistics_service = JobStatisticsService()
        return self.__job_statistics_service

    def reset(self, service: str):
        """
        Removes the saved instance of the service. It is useful,
        for example, in case of gitlab service - when we want to use
        different rule-sources configurations
        """
        private_service_name = f'_ServiceProvider__{service}'
        if not hasattr(self, private_service_name):
            raise AssertionError(
                f'In case you are using this method, make sure your '
                f'service {private_service_name} exists amongst the '
                f'private attributes')
        setattr(self, private_service_name, None)
