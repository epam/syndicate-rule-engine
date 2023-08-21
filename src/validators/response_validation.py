import sys
from enum import Enum
from inspect import getmembers
from typing import Dict, List, Optional, Union, Literal

from pydantic import BaseModel


class BaseResponseModel(BaseModel):
    trace_id: str


class BaseMessageResponseModel(BaseResponseModel):
    message: str


class UnauthorizedResponseModel(BaseModel):
    message: str


class DojoConfiguration(BaseModel):
    host: Optional[str]
    key: Optional[str]
    user: Optional[str]
    upload_files: Optional[bool]
    display_all_fields: Optional[bool]
    resource_per_finding: Optional[bool]


class SecurityHubConfiguration(BaseModel):
    region: Optional[str]
    product_arn: Optional[str]


class EntitiesMapping(BaseModel):
    product_type_name: Optional[str]
    test_title: Optional[str]
    product_name: Optional[str]
    engagement_name: Optional[str]


class SiemConfigurationType(str, Enum):
    dojo = 'dojo'
    security_hub = 'security_hub'


class Cloud(str, Enum):
    aws = 'aws'
    azure = 'azure'
    gcp = 'gcp'
    GCP = 'GCP'
    AWS = 'AWS'
    AZURE = 'AZURE'


class JobStatus(str, Enum):
    FAILED = 'FAILED'
    SUCCEEDED = 'SUCCEEDED'
    SUBMITTED = 'SUBMITTED'
    PENDING = 'PENDING'
    RUNNABLE = 'RUNNABLE'
    STARTING = 'STARTING'
    RUNNING = 'RUNNING'


class RuleSourceSyncStatus(Enum):
    SYNCING = 'SYNCING'
    SYNCED = 'SYNCED'


class ActivationState(Enum):
    ACTIVE = 'ACTIVE'
    INACTIVE = 'INACTIVE'


# TODO remove Optional everywhere


class Customer(BaseModel):
    name: str
    display_name: Optional[str]
    admins: Optional[List[str]]
    latest_login: Optional[str]
    activation_date: Optional[str]
    inherit: Optional[bool]


class Tenant(BaseModel):
    name: str
    customer_name: str
    activation_date: Optional[str]
    is_active: Optional[bool]
    regions: List[str]
    project: str


class Rule(BaseModel):
    id: str
    description: str
    service_section: str
    cloud: Cloud
    customer: str


class Job(BaseModel):
    job_id: str
    job_owner: str
    tenant_display_name: str
    status: JobStatus
    scan_regions: List[str]
    scan_rulesets: List[str]
    started_at: Optional[str]
    stopped_at: Optional[str]


class Policy(BaseModel):
    customer: str
    name: str
    permissions: List[str]


class Role(BaseModel):
    customer: str
    name: str
    expiration: str
    policies: List[str]


class Ruleset(BaseModel):
    customer: str
    name: str
    version: str
    cloud: Cloud
    rules_number: Optional[int]
    status_code: Optional[str]
    status_reason: Optional[str]
    event_driven: Optional[bool]
    active: Optional[bool]
    license_keys: Optional[List[str]]
    licensed: Optional[bool]
    status_last_update_time: str
    allowed_for: Optional[Union[dict, str]]


class RuleSource(BaseModel):
    id: str
    customer: str
    latest_sync_current_status: RuleSourceSyncStatus
    git_url: str
    git_ref: str
    git_rules_prefix: str
    latest_sync_sync_date: Optional[str]
    restrict_from: Optional[Dict[str, List[str]]]


class CredentialsManager(BaseModel):
    cloud: Cloud
    cloud_identifier: str
    credentials_key: Optional[str]
    expiration: Optional[str]
    trusted_role_arn: Optional[str]
    enabled: Optional[bool]


class RuleMetaUpdate(BaseModel):
    customer: str
    id: Optional[str]
    status: str


class UserTenant(BaseModel):
    tenants: str


class UserRole(BaseModel):
    role: str


class UserCustomer(BaseModel):
    customer: str


class SecurityHubSiem(BaseModel):
    type: str
    customer: str
    configuration: Optional[SecurityHubConfiguration]


class DojoSiem(BaseModel):
    type: str
    customer: str
    configuration: Optional[DojoConfiguration]
    entities_mapping: Optional[EntitiesMapping]


class Findings(BaseModel):
    presigned_url: str


class User(BaseModel):
    username: str
    customer: str
    role: str
    tenants: Optional[str]


class Report(BaseModel):
    bucket_name: str
    file_key: str
    presigned_url: str


class SiemPush(BaseModel):
    job_id: str
    type: SiemConfigurationType
    status: int
    message: Optional[str]
    error: Optional[str]


# class JobReport(BaseModel):
#     job_id: str
#     account_display_name: str
#     total_checks_performed: Optional[int]
#     successful_checks: Optional[int]
#     failed_checks: Optional[int]
#     total_resources_violated_rules: Optional[int]
#     rules_to_scan: Optional[List[str]]
#     created_at: Optional[datetime]
#     submitted_at: Optional[datetime]
#     stopped_at: Optional[datetime]
#     customer_display_name: Optional[str]
#     job_definition: Optional[str]
#     job_owner: Optional[str]
#     job_queue: Optional[str]
#     tenant_display_name: Optional[str]
#     detailed_report_content: Optional[Dict[str, List]]
#     standard_points: Optional[Dict[str, List]]
#     resources: Optional[List]
#     is_event_driven: Optional[bool]
#     scan_regions: List[str]
#     status: str


class TenantLicensePriority(BaseModel):
    tenant: str
    priority_id: str
    ruleset: str
    license_keys: List[str]


class ScheduledJob(BaseModel):
    name: str
    customer_name: str
    creation_date: str
    enabled: bool
    last_execution_time: Optional[str]
    schedule: str
    scan_regions: Optional[list]
    scan_rulesets: Optional[list]


class BackUpResponse(BaseModel):
    title: str
    commit_url: str
    stats: str
    git_files_created: str
    ssm_params_created: str


class HealthCheck(BaseModel):
    API: str
    MinIO: str
    Vault: str
    MongoDB: str


class MailSetting(BaseModel):
    username: str
    password: str
    default_sender: str
    host: str
    port: str
    max_emails: int
    use_tls: bool


class LicenseManagerConfig(BaseModel):
    host: str
    port: int
    version: float


class LicenseManagerClient(BaseModel):
    kid: str
    alg: str
    public_key: Optional[str]
    format: Optional[str]


class JobReport(BaseModel):
    id: str
    type: str
    # Either of the following.
    content: Optional[Dict]
    href: Optional[str]


class TenantReport(BaseModel):
    tenant: str
    # Either of the following.
    content: Optional[Dict]
    href: Optional[str]


class JobRuleReport(BaseModel):
    id: str
    type: str
    # Either of the following.
    content: Optional[List]
    href: Optional[str]


class TenantRuleReport(BaseModel):
    tenant: str
    # Either of the following.
    content: Optional[List]
    href: Optional[str]


class ApplicationMeta(BaseModel):
    awsAid: Optional[str]
    azureAid: Optional[str]
    googleAid: Optional[str]
    awsLk: Optional[str]
    azureLk: Optional[str]
    googleLk: Optional[str]


class AccessApplicationMeta(BaseModel):
    username: Optional[str]
    host: Optional[str]
    port: Optional[int]
    protocol: Optional[Literal['HTTP', 'HTTPS']]
    stage: Optional[str]


class Application(BaseModel):
    application_id: str
    customer_id: str
    description: str
    meta: ApplicationMeta


class AccessApplication(BaseModel):
    application_id: str
    customer_id: str
    description: str
    meta: AccessApplicationMeta


class ParentMeta(BaseModel):
    cloud: Optional[Literal['AWS', 'AZURE', 'GOOGLE', 'ALL']]
    scope: Optional[Literal['ALL', 'SPECIFIC_TENANT']]


class Parent(BaseModel):
    application_id: str
    customer_id: str
    description: str
    is_deleted: bool
    meta: ParentMeta


class RabbitMQ(BaseModel):
    maestro_user: str
    rabbit_exchange: Optional[str]
    request_queue: str
    response_queue: str
    sdk_access_key: str
    customer: str


class RuleSourceDTO(BaseResponseModel):
    items: List[RuleSource]


class RulesetDTO(BaseResponseModel):
    items: List[Ruleset]


class RoleDTO(BaseResponseModel):
    items: List[Role]


class PolicyDTO(BaseResponseModel):
    items: List[Policy]


class JobDTO(BaseResponseModel):
    items: List[Job]


class RuleDTO(BaseResponseModel):
    items: List[Rule]


class CustomerDTO(BaseResponseModel):
    items: List[Customer]


class TenantDTO(BaseResponseModel):
    items: List[Tenant]


class CredentialsManagerDTO(BaseResponseModel):
    items: List[CredentialsManager]


class RuleMetaUpdateDTO(BaseResponseModel):
    items: List[RuleMetaUpdate]


class UserTenantsDTO(BaseResponseModel):
    items: List[UserTenant]


class UserRoleDTO(BaseResponseModel):
    items: List[UserRole]


class UserCustomerDTO(BaseResponseModel):
    items: List[UserCustomer]


class SiemSecurityHubDTO(BaseResponseModel):
    items: List[DojoSiem]


class SiemDojoDTO(BaseResponseModel):
    items: List[SecurityHubSiem]


class FindingsDTO(BaseResponseModel):
    items: List[Findings]


class UserDTO(BaseResponseModel):
    items: List[User]


class ReportDTO(BaseResponseModel):
    items: List[Report]


class SiemPushDTO(BaseResponseModel):
    items: List[SiemPush]


class JobReportDTO(BaseResponseModel):
    items: List[JobReport]


class TenantLicensePriorityDTO(BaseResponseModel):
    items: List[TenantLicensePriority]


class ScheduledJobDTO(BaseResponseModel):
    items: List[ScheduledJob]


class HealthCheckDTO(BaseResponseModel):
    items: List[HealthCheck]


class BackUpResponseDTO(BaseResponseModel):
    items: List[BackUpResponse]


class MailSettingDTO(BaseResponseModel):
    items: List[MailSetting]


class LicenseManagerConfigDTO(BaseResponseModel):
    items: List[LicenseManagerConfig]


class LicenseManagerClientDTO(BaseResponseModel):
    items: List[LicenseManagerClient]


class ApplicationDTO(BaseResponseModel):
    items: List[Application]


class AccessApplicationDTO(BaseResponseModel):
    items: List[AccessApplication]


class ParentDTO(BaseResponseModel):
    items: List[Parent]


class RabbitMQDTO(BaseResponseModel):
    items: List[RabbitMQ]


class BatchResult(BaseModel):
    batch_results_id: str
    job_id: str
    customer_name: str
    tenant_name: str
    cloud_identifier: str
    status: str
    registration_start: str
    registration_end: str
    submitted_at: str


class BatchResultDTO(BaseResponseModel):
    items: List[BatchResult]


class GenericReportDTO(BaseResponseModel):
    items: List[
        Union[JobReport, TenantReport]
    ]


class RuleReportDTO(BaseResponseModel):
    items: List[
        Union[JobRuleReport, TenantRuleReport]
    ]


ALL_MODELS = set(
    obj for name, obj in getmembers(sys.modules[__name__])
    if (name.endswith('DTO') and isinstance(obj, type) and
        issubclass(obj, BaseModel))
)
ALL_MODELS.update((BaseMessageResponseModel, UnauthorizedResponseModel))
