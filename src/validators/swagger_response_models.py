from datetime import datetime
from typing import Literal

from pydantic import BaseModel
from typing_extensions import NotRequired, TypedDict

from helpers.constants import (
    HealthCheckStatus,
    JobState,
    JobType,
    PlatformType,
    PolicyEffect,
    PolicyErrorType,
    ReportFormat,
    RuleDomain,
    RuleSourceSyncingStatus,
    RuleSourceType,
)


class BaseActivation(TypedDict):
    activated_for_all: bool
    within_clouds: NotRequired[list[str]]
    excluding: list[str]
    activated_for: NotRequired[list[str]]

class Resource(TypedDict):
    id: str
    name: str
    location: str
    resource_type: str
    tenant_name: str
    customer_name: str
    data: dict
    sync_date: datetime
    hash: str

class ResourceException(TypedDict):
    id: str
    type: str
    resource_id: str | None
    location: str | None
    resource_type: str | None
    arn: str | None
    tags_filters: list[str] | None
    tenant_name: str
    customer_name: str
    created_at: float
    updated_at: float
    expire_at: float

class Customer(TypedDict):
    name: str
    display_name: str
    admins: list[str]


class Tenant(TypedDict):
    account_id: str
    activation_date: datetime
    customer_name: str
    is_active: bool
    name: str
    regions: list[str]


class Rule(TypedDict):
    description: str
    name: str
    cloud: RuleDomain
    branch: str
    customer: str
    project: str
    resource: str
    rule_source_id: str


class Job(TypedDict):
    created_at: NotRequired[datetime]
    customer_name: str
    id: str
    regions: list[str]
    rulesets: list[str]
    started_at: NotRequired[datetime]
    status: JobState
    stopped_at: NotRequired[datetime]
    submitted_at: str
    tenant_name: str
    scheduled_rule_name: NotRequired[str]
    celery_task_id: NotRequired[str]


class Policy(TypedDict):
    customer: str
    name: str
    permissions: list[str]
    effect: PolicyEffect
    description: str | None
    tenants: list[str]


class Role(TypedDict):
    customer: str
    name: str
    expiration: datetime
    policies: list[str]
    description: str | None


class Ruleset(TypedDict):
    active: bool
    cloud: RuleDomain
    customer: str
    last_update_time: datetime
    license_keys: list[str]
    licensed: bool
    name: str
    rules_number: int
    rules: NotRequired[list[str]]
    version: str


class RuleSourceLatestSync(TypedDict):
    current_status: RuleSourceSyncingStatus
    sync_date: datetime
    release_tag: NotRequired[str]


class RuleSource(TypedDict):
    id: str
    customer: str
    git_project_id: str
    git_url: str
    git_ref: str
    git_rules_prefix: str
    description: str
    has_secret: bool
    latest_sync: NotRequired[RuleSourceLatestSync]
    type: RuleSourceType


class RuleMetaUpdate(TypedDict):
    customer: str
    git_project_id: str
    rule_source_id: str
    status: str


class ScheduledJob(TypedDict):
    name: str
    customer_name: str
    tenant_name: str
    creation_date: datetime
    enabled: bool
    last_execution_time: NotRequired[datetime]
    schedule: str
    scan_regions: list[str]
    scan_rulesets: list[str]


class HealthCheck(TypedDict):
    id: str
    status: HealthCheckStatus
    details: dict
    remediation: NotRequired[str]
    impact: NotRequired[str]


class LicenseManagerConfig(TypedDict):
    host: str
    port: int
    protocol: Literal['HTTP', 'HTTPS']
    stage: str


class LicenseManagerClient(TypedDict):
    algorithm: str
    b64_encoded: bool
    format: str
    key_id: str
    public_key: str


class RabbitMQ(TypedDict):
    maestro_user: str
    rabbit_exchange: NotRequired[str]
    request_queue: str
    response_queue: str
    sdk_access_key: str
    customer: str


class ReportStatus(TypedDict):
    id: str
    triggered_at: str
    attempt: int
    customer_name: str
    level: str
    status: str
    reason: NotRequired[str]
    tenant: NotRequired[str]
    type: str
    user: NotRequired[str]


class K8sPlatform(TypedDict):
    customer: str
    description: str
    id: str
    name: str
    region: NotRequired[str]
    tenant_name: str
    type: PlatformType


class Event(TypedDict):
    """
    202 POST /event
    """
    received: int
    saved: int


class ServiceOperationStatus(TypedDict):
    started_at: datetime
    state: str


class LicenseAllowance(TypedDict):
    balance_exhaustion_model: Literal['collective', 'independent']
    job_balance: int
    time_range: Literal['DAY', 'WEEK', 'MONTH']


class LicenseEventDriven(TypedDict):
    active: bool


class License(TypedDict):
    allowance: LicenseAllowance
    event_driven: LicenseEventDriven
    expiration: datetime
    latest_sync: datetime
    license_key: str
    ruleset_ids: list[str]


class MailSetting(TypedDict):
    username: str
    password: str
    default_sender: str
    host: str
    port: str
    max_emails: int
    use_tls: bool


class Credentials(TypedDict):
    id: str
    type: Literal[
        'AWS_CREDENTIALS',
        'AWS_ROLE',
        'AZURE_CREDENTIALS',
        'AZURE_CERTIFICATE',
        'GCP_SERVICE_ACCOUNT',
        'GCP_COMPUTE_ACCOUNT'
    ]
    description: str
    has_secret: bool
    credentials: dict


# reports
class BaseReportJob(TypedDict):
    format: ReportFormat
    url: NotRequired[str]
    dictionary_url: NotRequired[str]
    obfuscated: bool
    content: NotRequired[dict]

    job_id: NotRequired[str]
    job_type: NotRequired[JobType]
    tenant_name: str
    customer_name: str


class BaseReportEntity(TypedDict):
    format: ReportFormat
    url: NotRequired[str]
    dictionary_url: NotRequired[str]
    obfuscated: bool
    content: NotRequired[dict]

    platform_id: NotRequired[str]
    tenant_name: NotRequired[str]
    customer_name: str


class ErrorReportItem(TypedDict):
    error_type: PolicyErrorType
    policy: str
    reason: str
    region: str


class RulesReportItem(TypedDict):
    api_calls: dict
    execution_time: float
    failed_resources: int
    policy: str
    region: str
    scanned_resources: int
    succeeded: bool


class AverageRulesReportItem(TypedDict):
    average_exec: float
    average_resources_failed: int
    average_resources_scanned: int
    failed_invocations: int
    invocations: int
    max_exec: float
    min_exec: float
    policy: str
    region: str
    resources_failed: int
    resources_scanned: int
    succeeded_invocations: int
    total_api_calls: dict
    total_exec: float


class ViolatedRule(TypedDict):
    name: str
    description: str
    severity: str

    remediation: NotRequired[str]
    article: NotRequired[str]
    impact: NotRequired[str]


class ResourcesReportItem(TypedDict):
    account_id: NotRequired[str]
    platform_id: NotRequired[str]

    job_id: NotRequired[str]
    type: NotRequired[JobType]

    data: dict
    last_found: float
    matched_by: dict
    region: str
    resource_type: str
    violated_rules: ViolatedRule


class DefectDojo(TypedDict):
    id: str
    description: str
    host: str
    port: int
    stage: str
    protocol: Literal['HTTP', 'HTTPS']


class Chronicle(TypedDict):
    id: str
    description: str
    endpoint: str
    credentials_application_id: str
    instance_customer_id: str
    customer: str


class DefectDojoActivation(BaseActivation):
    scan_type: Literal['Generic Findings Import', 'Cloud Custodian Scan']
    product_type: str
    product: str
    engagement: str
    test: str
    send_after_job: bool
    attachment: Literal['json', 'xlsx', 'csv'] | None


class ChronicleActivation(BaseActivation):
    send_after_job: bool


class DojoPushResult(TypedDict):
    job_id: str
    scan_type: Literal['Generic Findings Import', 'Cloud Custodian Scan']
    product_type_name: str
    product_name: str
    engagement_name: str
    test_title: str
    tenant_name: str
    dojo_integration_id: str
    success: bool
    attachment: Literal['json', 'xlsx', 'csv'] | None
    platform_id: NotRequired[str]
    error: NotRequired[str]


class ChroniclePushResult(TypedDict):
    job_id: str
    tenant_name: str
    chronicle_integration_id: str
    success: bool
    platform_id: NotRequired[str]
    error: NotRequired[str]


class SelfIntegration(BaseActivation):
    customer_name: str
    description: str
    username: str
    host: str
    stage: str
    port: int
    protocol: Literal['HTTP', 'HTTPS']
    results_storage: NotRequired[str]


class TenantExcludedRules(TypedDict):
    tenant_name: str
    rules: list[str]


class CustomerExcludedRules(TypedDict):
    customer_name: str
    rules: list[str]


class User(TypedDict):
    username: str
    customer: str | None
    role: str | None
    latest_login: datetime | None
    created_at: datetime | None


class RawReportItem(TypedDict):
    customer_name: str
    tenant_name: str
    obfuscated: bool
    url: str
    dictionary_url: NotRequired[str]
    meta_url: NotRequired[str]


# Here real response models


class RawReportModel(BaseModel):
    data: RawReportItem


class MessageModel(BaseModel):
    message: str


class ErrorData(TypedDict):
    location: list[str]
    message: str


class ErrorsModel(BaseModel):
    """
    400 Validation error
    """
    errors: list[ErrorData]


class MultipleJobsModel(BaseModel):
    """
    200 GET /jobs
    """
    items: list[Job]
    next_token: str | None


class SingleJobModel(BaseModel):
    """
    201 POST /jobs
    201 POST /jobs/k8s
    201 POST /jobs/standard
    200 GET /jobs/{job_id}
    """
    data: Job


class MultipleScheduledJobsModel(BaseModel):
    """
    200 GET /jobs
    """
    items: list[ScheduledJob]


class SingleScheduledJobModel(BaseModel):
    """
    201 POST /jobs
    201 POST /jobs/k8s
    201 POST /jobs/standard
    200 GET /jobs/{job_id}
    """
    data: ScheduledJob


class MultipleK8SPlatformsModel(BaseModel):
    items: list[K8sPlatform]


class SingleK8SPlatformModel(BaseModel):
    data: K8sPlatform

class SignInModel(BaseModel):
    access_token: str  # actually it's Congito's id_token
    refresh_token: str
    expires_in: int


class MultipleResourcesModel(BaseModel):
    items: list[Resource]

class SingleResourceModel(BaseModel):
    data: Resource

class MultipleResourcesExceptionsModel(BaseModel):
    items: list[ResourceException]

class SingleResourceExceptionModel(BaseModel):
    data: ResourceException

class MultipleCustomersModel(BaseModel):
    items: list[Customer]


class MultipleTenantsModel(BaseModel):
    items: list[Tenant]


class SingleTenantsModel(BaseModel):
    data: Tenant


class EventModel(BaseModel):
    data: Event


class MultipleHealthChecksModel(BaseModel):
    items: list[HealthCheck]


class SingleHealthCheckModel(BaseModel):
    data: HealthCheck


class SingleRabbitMQModel(BaseModel):
    data: RabbitMQ


class MultipleRulesModel(BaseModel):
    items: list[Rule]
    next_token: str | None


class MultipleRuleMetaUpdateModel(BaseModel):
    items: list[RuleMetaUpdate]


class MultipleServiceOperationStatusesModel(BaseModel):
    items: list[ServiceOperationStatus]


class SingleRulesetModel(BaseModel):
    data: Ruleset


class MultipleRulesetsModel(BaseModel):
    items: list[Ruleset]


class SingleRuleSourceModel(BaseModel):
    data: RuleSource


class MultipleRuleSourceModel(BaseModel):
    items: list[RuleSource]


class SinglePolicyModel(BaseModel):
    data: Policy


class MultiplePoliciesModel(BaseModel):
    items: list[Policy]


class SingleRoleModel(BaseModel):
    data: Role


class MultipleRoleModel(BaseModel):
    items: list[Role]


class SingleLicenseModel(BaseModel):
    data: License


class MultipleLicensesModel(BaseModel):
    items: list[License]


class SingleMailSettingModel(BaseModel):
    data: MailSetting


class SingleLMClientModel(BaseModel):
    data: LicenseManagerClient


class SingleLMConfigModel(BaseModel):
    data: LicenseManagerConfig


class SingleJobReportModel(BaseModel):
    data: BaseReportJob


class MultipleJobReportModel(BaseModel):
    items: list[BaseReportJob]


class SingleEntityReportModel(BaseModel):
    data: BaseReportEntity


class ErrorsReportModel(BaseModel):
    items: list[ErrorReportItem] | None
    data: BaseReportJob | None  # if href=true


class RulesReportModel(BaseModel):
    items: list[RulesReportItem] | None
    data: BaseReportJob | None  # if href=true


class EntityRulesReportModel(BaseModel):
    items: list[AverageRulesReportItem]


class MultipleReportStatusModel(BaseModel):
    items: list[ReportStatus]


class EntityResourcesReportModel(BaseModel):
    items: list[ResourcesReportItem] | None
    data: BaseReportEntity | None


class JobResourcesReportModel(BaseModel):
    items: list[ResourcesReportItem]
    data: BaseReportJob | None


class SingleLicenseActivationModel(BaseModel):
    data: BaseActivation


class SingleDefeDojoModel(BaseModel):
    data: DefectDojo


class MultipleDefectDojoModel(BaseModel):
    items: DefectDojo


class SingleChronicleModel(BaseModel):
    data: Chronicle


class SingleChronicleActivationModel(BaseModel):
    data: ChronicleActivation


class MultipleChronicleModel(BaseModel):
    items: list[Chronicle]


class SingleDefectDojoActivation(BaseModel):
    data: DefectDojoActivation


class SingleDefectDojoPushResult(BaseModel):
    data: DojoPushResult


class MultipleDefectDojoPushResult(BaseModel):
    items: list[DojoPushResult]


class SingleChroniclePushResult(BaseModel):
    data: ChroniclePushResult


class SingleSelfIntegration(BaseModel):
    data: SelfIntegration


class SingleTenantExcludedRules(BaseModel):
    data: TenantExcludedRules


class SingleCustomerExcludedRules(BaseModel):
    data: CustomerExcludedRules


class MultipleCredentialsModel(BaseModel):
    items: list[Credentials]
    next_token: str | None


class SingleCredentialsModel(BaseModel):
    data: Credentials


class CredentialsActivationModel(BaseModel):
    data: BaseActivation


class SingleUserModel(BaseModel):
    data: User


class MultipleUsersModel(BaseModel):
    items: list[User]
    next_token: str | None
