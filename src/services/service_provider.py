from __future__ import annotations

import threading
from functools import cached_property
from typing import TYPE_CHECKING, Union

from helpers import SingletonMeta, get_logger


if TYPE_CHECKING:
    from modular_sdk.modular import ModularServiceProvider

    from services.cadf_event_sender import CadfEventSender
    from services.chronicle_service import ChronicleInstanceService
    from services.clients.batch import BatchClient, CeleryJobClient
    from services.clients.cognito import BaseAuthClient, CognitoClient
    from services.clients.event_bridge import EventBridgeClient
    from services.clients.iam import IAMClient
    from services.clients.mongo_ssm_auth_client import MongoAndSSMAuthClient
    from services.clients.s3 import ModularAssumeRoleS3Service, S3Client
    from services.clients.ssm import CachedSSMClient
    from services.clients.step_function import ScriptClient, StepFunctionClient
    from services.clients.sts import StsClient
    from services.defect_dojo_service import DefectDojoService
    from services.environment_service import EnvironmentService
    from services.event_driven import (
        EventProcessorService,
        EventMappingCollector,
        EventService,
        S3EventMappingProvider,
    )
    from services.integration_service import IntegrationService
    from services.job_service import JobService
    from services.license_manager_service import LicenseManagerService
    from services.license_service import LicenseService
    from services.metadata import MetadataProvider
    from services.platform_service import PlatformService
    from services.rabbitmq_service import RabbitMQService
    from services.rbac_service import PolicyService, RoleService
    from services.report_service import ReportService
    from services.reports import ReportMetricsService
    from services.resource_exception_service import ResourceExceptionsService
    from services.resources_service import ResourcesService
    from services.rule_meta_service import RuleService
    from services.rule_source_service import RuleSourceService
    from services.ruleset_service import RulesetService
    from services.scheduled_job_service import ScheduledJobService
    from services.service_operation_service import ServiceOperationService
    from services.setting_service import CachedSettingsService


_LOG = get_logger(__name__)


class ServiceProvider(metaclass=SingletonMeta):
    """
    Service provider for the application.
    """

    def __str__(self) -> str:
        return str(id(self))

    @cached_property
    def environment_service(self) -> 'EnvironmentService':
        from services.environment_service import EnvironmentService
        return EnvironmentService()

    @cached_property
    def s3(self) -> 'S3Client':
        from services.clients.s3 import S3Client
        return S3Client.build()

    @cached_property
    def ssm(self) -> 'CachedSSMClient':
        from services.clients.ssm import (
            CachedSSMClient,
            SSMClient,
            VaultSSMClient,
        )
        env = self.environment_service
        if env.is_docker():
            cl = VaultSSMClient()
        else:
            cl = SSMClient(environment_service=env)
        return CachedSSMClient(cl)

    @cached_property
    def sts(self) -> StsClient:
        from services.clients.sts import StsClient
        if self.environment_service.is_docker():
            return StsClient.build()
        return StsClient.build()

    @cached_property
    def batch(self) -> Union['BatchClient', 'CeleryJobClient']:
        if self.environment_service.is_docker():
            from services.clients.batch import CeleryJobClient
            return CeleryJobClient.build()
        from services.clients.batch import BatchClient
        return BatchClient.build()

    @cached_property
    def onprem_users_client(self) -> 'MongoAndSSMAuthClient':
        from services.clients.mongo_ssm_auth_client import (
            MongoAndSSMAuthClient,
        )
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
    def modular_client(self) -> 'ModularServiceProvider':
        from modular_sdk.modular import ModularServiceProvider
        return ModularServiceProvider()

    @cached_property
    def events(self) -> 'EventBridgeClient':
        from services.clients.event_bridge import EventBridgeClient

        # TODO: probably not used
        return EventBridgeClient.build()

    @cached_property
    def iam(self) -> 'IAMClient':
        from services.clients.iam import IAMClient
        return IAMClient(sts_client=self.sts)

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
    def role_service(self) -> 'RoleService':
        from services.rbac_service import RoleService
        return RoleService()

    @cached_property
    def policy_service(self) -> 'PolicyService':
        from services.rbac_service import PolicyService
        return PolicyService()

    @cached_property
    def event_processor_service(self) -> EventProcessorService:
        from services.event_driven import EventProcessorService
        return EventProcessorService(
            environment_service=self.environment_service,
            sts_client=self.sts,
            license_service=self.license_service,
            event_mapping_provider=self.s3_event_mapping_provider,
            tenant_service=self.modular_client.tenant_service(),
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
    def scheduled_job_service(self) -> 'ScheduledJobService':
        from services.scheduled_job_service import ScheduledJobService
        return ScheduledJobService()

    @cached_property
    def event_service(self) -> 'EventService':
        from services.event_driven import EventService
        return EventService(environment_service=self.environment_service)
    
    @cached_property
    def event_mapping_collector(self) -> EventMappingCollector:
        from services.event_driven import EventMappingCollector
        return EventMappingCollector(
            s3_client=self.s3,
            environment_service=self.environment_service
        )
    
    @cached_property
    def s3_event_mapping_provider(self) -> S3EventMappingProvider:
        from services.event_driven import S3EventMappingProvider
        return S3EventMappingProvider(
            s3_client=self.s3,
            environment_service=self.environment_service
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
        from services.clients.step_function import (
            ScriptClient,
            StepFunctionClient,
        )
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
        )

    @cached_property
    def metadata_provider(self) -> 'MetadataProvider':
        from services.metadata import MetadataProvider
        return MetadataProvider(
            lm_service=self.license_manager_service,
            s3_client=self.s3,
            environment_service=self.environment_service,
            hooks=[self.event_mapping_collector]
        )

    @cached_property
    def tls(self) -> threading.local:
        return threading.local()
    
    @cached_property
    def resources_service(self) -> 'ResourcesService':
        from services.resources_service import ResourcesService
        return ResourcesService()
    
    @cached_property
    def resource_exception_service(self) -> 'ResourceExceptionsService':
        from services.resource_exception_service import (
            ResourceExceptionsService,
        )
        return ResourceExceptionsService()

    @cached_property
    def service_operation_service(self) -> 'ServiceOperationService':
        from services.service_operation_service import ServiceOperationService
        return ServiceOperationService()

    @cached_property
    def cadf_event_sender(self) -> 'CadfEventSender':
        from services.cadf_event_sender import CadfEventSender
        return CadfEventSender(rabbitmq_service=self.rabbitmq_service)
