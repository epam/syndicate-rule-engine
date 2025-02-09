from functools import cached_property
from typing import Union, TYPE_CHECKING

from helpers import SingletonMeta

if TYPE_CHECKING:
    from services.clients.mongo_ssm_auth_client import MongoAndSSMAuthClient
    from scheduler.ap_job_scheduler import APJobScheduler
    from services.ambiguous_job_service import AmbiguousJobService
    from services.assemble_service import AssembleService
    from services.batch_results_service import BatchResultsService
    from services.clients.cognito import CognitoClient
    from services.clients.event_bridge import EventBridgeClient
    from services.clients.iam import IAMClient
    from services.clients.modular import ModularClient
    from services.clients.s3 import ModularAssumeRoleS3Service
    from services.clients.s3 import S3Client
    from services.clients.scheduler import EventBridgeJobScheduler
    from services.clients.ssm import CachedSSMClient
    from services.clients.sts import StsClient
    from services.environment_service import EnvironmentService
    from services.event_processor_service import EventProcessorService
    from services.event_service import EventService
    from services.job_service import JobService
    from services.job_statistics_service import JobStatisticsService
    from services.license_manager_service import LicenseManagerService
    from services.license_service import LicenseService
    from services.rabbitmq_service import RabbitMQService
    from services.report_service import ReportService
    from services.rule_meta_service import RuleService
    from services.rule_source_service import RuleSourceService
    from services.ruleset_service import RulesetService
    from services.s3_settings_service import S3SettingsService
    from services.scheduler_service import SchedulerService
    from services.setting_service import CachedSettingsService
    from services.platform_service import PlatformService
    from services.integration_service import IntegrationService
    from services.clients.batch import CeleryJobClient, BatchClient
    from services.defect_dojo_service import DefectDojoService
    from services.clients.cognito import BaseAuthClient
    from services.rbac_service import RoleService, PolicyService
    from services.clients.step_function import ScriptClient, StepFunctionClient
    from services.chronicle_service import ChronicleInstanceService
    from services.reports import ReportMetricsService
    from services.metadata import MetadataProvider


class ServiceProvider(metaclass=SingletonMeta):
    def __str__(self):
        return id(self)

    @cached_property
    def environment_service(self) -> 'EnvironmentService':
        from services.environment_service import EnvironmentService
        return EnvironmentService()

    @cached_property
    def s3(self) -> 'S3Client':
        from services.clients.s3 import S3Client
        env = self.environment_service
        if env.is_docker():
            return S3Client.factory().build_minio()
        return S3Client.factory().build_s3(env.aws_region())

    @cached_property
    def ssm(self) -> 'CachedSSMClient':
        from services.clients.ssm import VaultSSMClient, SSMClient, CachedSSMClient
        env = self.environment_service
        if env.is_docker():
            cl = VaultSSMClient()
        else:
            cl = SSMClient(environment_service=env)
        return CachedSSMClient(cl)

    @cached_property
    def sts(self) -> 'StsClient':
        from services.clients.sts import StsClient
        if self.environment_service.is_docker():
            return StsClient.build()
        return StsClient.factory().build(
            region_name=self.environment_service.aws_region()
        )

    @cached_property
    def batch(self) -> Union['BatchClient', 'CeleryJobClient']:
        if self.environment_service.is_docker():
            from services.clients.batch import CeleryJobClient
            return CeleryJobClient.build()
        from services.clients.batch import BatchClient
        return BatchClient.factory().build(
            region_name=self.environment_service.aws_region()
        )

    @cached_property
    def onprem_users_client(self) -> 'MongoAndSSMAuthClient':
        from services.clients.mongo_ssm_auth_client import MongoAndSSMAuthClient
        return MongoAndSSMAuthClient(ssm_client=self.ssm)

    @cached_property
    def saas_users_client(self) -> 'CognitoClient':
        from services.clients.cognito import CognitoClient
        return CognitoClient(environment_service=self.environment_service)

    @cached_property
    def users_client(self) -> 'BaseAuthClient':
        if self.environment_service.is_docker():
            return self.onprem_users_client
        return self.saas_users_client

    @cached_property
    def lambda_client(self):
        from services.clients.lambda_func import LambdaClient
        return LambdaClient(environment_service=self.environment_service)

    @cached_property
    def modular_client(self) -> 'ModularClient':
        from services.clients.modular import ModularClient
        return ModularClient()

    @cached_property
    def events(self) -> 'EventBridgeClient':
        from services.clients.event_bridge import EventBridgeClient
        return EventBridgeClient.factory().build(
            region_name=self.environment_service.aws_region()
        )

    @cached_property
    def iam(self) -> 'IAMClient':
        from services.clients.iam import IAMClient
        return IAMClient(sts_client=self.sts)

    @cached_property
    def event_bridge_job_scheduler(self) -> 'EventBridgeJobScheduler':
        from services.clients.scheduler import EventBridgeJobScheduler
        return EventBridgeJobScheduler(
            client=self.events,
            environment_service=self.environment_service,
            iam_client=self.iam,
            batch_client=self.batch
        )

    @cached_property
    def ap_job_scheduler(self) -> 'APJobScheduler':
        from scheduler.ap_job_scheduler import APJobScheduler
        return APJobScheduler(
            batch_client=self.batch,
            setting_service=self.settings_service,
        )

    @cached_property
    def assume_role_s3(self) -> 'ModularAssumeRoleS3Service':
        from services.clients.s3 import ModularAssumeRoleS3Service
        return ModularAssumeRoleS3Service()

    @cached_property
    def job_service(self) -> 'JobService':
        from services.job_service import JobService
        return JobService()

    @cached_property
    def rule_service(self) -> 'RuleService':
        from services.rule_meta_service import RuleService
        return RuleService()

    @cached_property
    def settings_service(self) -> 'CachedSettingsService':
        from services.setting_service import CachedSettingsService
        return CachedSettingsService(
            environment_service=self.environment_service
        )

    @cached_property
    def s3_settings_service(self) -> 'S3SettingsService':
        from services.s3_settings_service import S3SettingsService
        return S3SettingsService(
            s3_client=self.s3,
            environment_service=self.environment_service
        )

    @cached_property
    def role_service(self) -> 'RoleService':
        from services.rbac_service import RoleService
        return RoleService()

    @cached_property
    def policy_service(self) -> 'PolicyService':
        from services.rbac_service import PolicyService
        return PolicyService()

    @cached_property
    def event_processor_service(self) -> 'EventProcessorService':
        from services.event_processor_service import EventProcessorService
        return EventProcessorService(
            s3_settings_service=self.s3_settings_service,
            environment_service=self.environment_service,
            sts_client=self.sts,
        )

    @cached_property
    def ruleset_service(self) -> 'RulesetService':
        from services.ruleset_service import RulesetService
        return RulesetService(
            s3_client=self.s3
        )

    @cached_property
    def rule_source_service(self) -> 'RuleSourceService':
        from services.rule_source_service import RuleSourceService
        return RuleSourceService(
            ssm=self.ssm,
        )

    @cached_property
    def license_service(self) -> 'LicenseService':
        from services.license_service import LicenseService
        return LicenseService(
            application_service=self.modular_client.application_service(),
            parent_service=self.modular_client.parent_service(),
            customer_service=self.modular_client.customer_service(),
            metadata_provider=self.metadata_provider
        )

    @cached_property
    def license_manager_service(self) -> 'LicenseManagerService':
        from services.license_manager_service import LicenseManagerService
        return LicenseManagerService(
            settings_service=self.settings_service,
            ssm=self.ssm,
            ruleset_service=self.ruleset_service
        )

    @cached_property
    def scheduler_service(self) -> 'SchedulerService':
        from services.scheduler_service import SchedulerService
        client = self.event_bridge_job_scheduler \
            if not self.environment_service.is_docker() \
            else self.ap_job_scheduler
        return SchedulerService(client=client)

    @cached_property
    def event_service(self) -> 'EventService':
        from services.event_service import EventService
        return EventService(environment_service=self.environment_service)

    @cached_property
    def assemble_service(self) -> 'AssembleService':
        from services.assemble_service import AssembleService
        return AssembleService(environment_service=self.environment_service)

    @cached_property
    def batch_results_service(self) -> 'BatchResultsService':
        from services.batch_results_service import BatchResultsService
        return BatchResultsService()

    @cached_property
    def ambiguous_job_service(self) -> 'AmbiguousJobService':
        from services.ambiguous_job_service import AmbiguousJobService
        return AmbiguousJobService(
            batch_results_service=self.batch_results_service,
            job_service=self.job_service
        )

    @cached_property
    def report_service(self) -> 'ReportService':
        from services.report_service import ReportService
        return ReportService(
            s3_client=self.s3,
            environment_service=self.environment_service,
        )

    @cached_property
    def rabbitmq_service(self) -> 'RabbitMQService':
        from services.rabbitmq_service import RabbitMQService
        return RabbitMQService(
            modular_client=self.modular_client,
            environment_service=self.environment_service
        )

    @cached_property
    def job_statistics_service(self) -> 'JobStatisticsService':
        from services.job_statistics_service import JobStatisticsService
        return JobStatisticsService()

    @cached_property
    def report_statistics_service(self):
        from services.report_statistics_service import ReportStatisticsService
        return ReportStatisticsService(
            setting_service=self.settings_service
        )

    @cached_property
    def platform_service(self) -> 'PlatformService':
        from services.platform_service import PlatformService
        return PlatformService(
            parent_service=self.modular_client.parent_service(),
            application_service=self.modular_client.application_service()
        )

    @cached_property
    def integration_service(self) -> 'IntegrationService':
        from services.integration_service import IntegrationService
        return IntegrationService(
            parent_service=self.modular_client.parent_service(),
            defect_dojo_service=self.defect_dojo_service,
            chronicle_instance_service=self.chronicle_instance_service
        )

    @cached_property
    def step_function(self) -> Union['ScriptClient', 'StepFunctionClient']:
        from services.clients.step_function import ScriptClient, \
            StepFunctionClient
        if self.environment_service.is_docker():
            return ScriptClient(environment_service=self.environment_service)
        else:
            return StepFunctionClient(
                environment_service=self.environment_service
            )

    @cached_property
    def defect_dojo_service(self) -> 'DefectDojoService':
        from services.defect_dojo_service import DefectDojoService
        return DefectDojoService(
            application_service=self.modular_client.application_service(),
            ssm_service=self.modular_client.assume_role_ssm_service()
        )

    @cached_property
    def chronicle_instance_service(self) -> 'ChronicleInstanceService':
        from services.chronicle_service import ChronicleInstanceService
        return ChronicleInstanceService(
            application_service=self.modular_client.application_service(),
            parent_service=self.modular_client.parent_service()
        )

    @cached_property
    def report_metrics_service(self) -> 'ReportMetricsService':
        from services.reports import ReportMetricsService
        return ReportMetricsService(
            s3_client=self.s3,
            environment_service=self.environment_service
        )

    @cached_property
    def metadata_provider(self) -> 'MetadataProvider':
        from services.metadata import MetadataProvider
        return MetadataProvider(
            lm_service=self.license_manager_service,
            s3_client=self.s3,
            environment_service=self.environment_service
        )
