# classes for swagger models are not instantiated directly in code.
# PreparedEvent models are used instead.

from base64 import standard_b64decode
from datetime import date, datetime, timedelta, timezone
from itertools import chain
from typing import Literal
from typing_extensions import Annotated, Self

from modular_sdk.commons.constants import Cloud as ModularCloud
from pydantic import (
    AmqpDsn,
    AnyUrl,
    BaseModel as PydanticBaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    StringConstraints,
    field_validator,
    model_validator,
)
from pydantic.json_schema import SkipJsonSchema, WithJsonSchema

from helpers.constants import (
    HealthCheckStatus,
    JobState,
    JobType,
    Permission,
    PlatformType,
    PolicyErrorType,
    ReportFormat,
    RuleDomain,
)
from helpers.regions import AllRegions, AllRegionsWithGlobal
from helpers.reports import Standard
from helpers.time_helper import utc_datetime
from services import SERVICE_PROVIDER
from services.rule_meta_service import RuleName
from models.policy import PolicyEffect


class BaseModel(PydanticBaseModel):
    model_config = ConfigDict(
        coerce_numbers_to_str=True,
        populate_by_name=True,
    )
    customer_id: SkipJsonSchema[str] = Field(
        None,
        alias='customer',
        description='Special parameter. Allows to perform actions on behalf '
                    'on the specified customer. This can be allowed only '
                    'for system users. Parameter will be ignored for '
                    'standard users',
    )

    @property
    def customer(self) -> str | None:
        """
        Just backward compatibility
        :return:
        """
        return self.customer_id


class BasePaginationModel(BaseModel):
    limit: int = Field(
        50, 
        ge=1, 
        le=50, 
        description='Max number of items to return'
    )
    next_token: str = Field(
        None, 
        description='Provide next_token received from the previous request'
    )


class TimeRangedMixin:
    """
    Base model which provides time-range constraint
    """

    # start_iso: datetime | date = Field(None, alias='from')
    # end_iso: datetime | date = Field(None, alias='to')

    @classmethod
    def skip_validation_if_no_input(cls):
        return False

    @classmethod
    def max_range(cls) -> timedelta:
        return timedelta(days=7)

    @classmethod
    def to_datetime(cls, d: datetime | date | None) -> datetime | None:
        if not d:
            return
        if isinstance(d, datetime):
            return d
        return datetime.combine(d, datetime.min.time())

    @model_validator(mode='after')
    def validate_dates(self) -> Self:
        """
        What it does:
        - converts start_iso and end_iso to utc tz-aware datetime object
        - validates that start_iso < end_iso, end_iso <= now
        - sets default values in case they are not provided
        :param values:
        :return:
        """
        now = utc_datetime()
        max_range = self.max_range()
        start = self.to_datetime(self.start_iso)
        end = self.to_datetime(self.end_iso)
        if start:
            start = start.astimezone(timezone.utc)
            if start > now:
                raise ValueError('value of \'from\' must be less '
                                 'than current date')
        if end:
            end = end.astimezone(timezone.utc)
            if end > now:
                raise ValueError('value of \'to\' must be less '
                                 'than current date')
        if start and end:
            pass
        elif start:
            end = min(start + max_range, now)
        elif end:
            start = end - max_range
        else:  # both not provided
            if self.skip_validation_if_no_input:
                self.start_iso = None
                self.end_iso = None
                return self
            end = now
            start = end - max_range
        if start >= end:
            raise ValueError('value of \'to\' must '
                             'be bigger than \'from\' date')

        if (end - start) > max_range:
            raise ValueError(
                f'Time range between \'from\' and \'to\' must '
                f'not overflow {max_range}')
        self.start_iso = start
        self.end_iso = end
        return self


class CustomerGetModel(BaseModel):
    """
    GET
    """
    name: str = Field(None)

    @model_validator(mode='after')
    def _(self) -> Self:
        if name := self.customer:  # not system
            self.name = name
        return self


class MultipleTenantsGetModel(BasePaginationModel):
    """
    GET
    """
    # cloud_identifier: Optional[str]  # TODO API separate endpoint for this
    active: bool = Field(None)
    cloud: ModularCloud = Field(None)


class TenantPostModel(BaseModel):
    name: str
    account_id: str
    cloud: Literal['AWS', 'AZURE', 'GOOGLE']
    display_name: str = Field(None)
    primary_contacts: list[str] = Field(default_factory=list)
    secondary_contacts: list[str] = Field(default_factory=list)
    tenant_manager_contacts: list[str] = Field(default_factory=list)
    default_owner: str = Field(None)

    @model_validator(mode='after')
    def set_display_name(self) -> Self:
        if not self.display_name:
            self.display_name = self.name
        return self


class TenantGetActiveLicensesModel(BaseModel):
    limit: int = 1


class TenantRegionPostModel(BaseModel):
    region: str  # means native region name

    @field_validator('region')
    @classmethod
    def validate_region(cls, value: str) -> str:
        """
        Of course, we can use typing "region: AllRegions", but the output is
        huge is validation fails
        """
        if not AllRegions.has(value):
            raise ValueError(f'Not known region: {value}')
        return value


class RulesetPostModel(BaseModel):
    name: str
    version: str
    cloud: RuleDomain
    active: bool = True

    # if empty, all the rules for cloud is chosen
    rules: set = Field(default_factory=set)
    git_project_id: str = Field(None)
    git_ref: str = Field(None)

    service_section: str = Field(None)
    severity: str = Field(None)
    mitre: set[str] = Field(default_factory=set)
    standard: set[str] = Field(default_factory=set)

    @model_validator(mode='after')
    def validate_filters(self) -> Self:
        if self.git_ref and not self.git_project_id:
            raise ValueError('git_project_id must be specified with git_ref')
        cloud = self.cloud
        col = SERVICE_PROVIDER.mappings_collector
        if self.service_section:
            ss = col.service_section
            if not ss:
                raise ValueError('cannot load service section data')
            available = set(
                value for key, value in ss.items()
                if RuleName(key).cloud == cloud
            )
            if self.service_section not in available:
                raise ValueError('Not available service section. '
                                 f'Choose from: {", ".join(available)}')
        if self.severity:
            sv = col.severity
            if not sv:
                raise ValueError('cannot load severity data')
            available = set(
                value for key, value in sv.items()
                if RuleName(key).cloud == cloud
            )
            if self.severity not in available:
                raise ValueError('Not available severity. '
                                 f'Choose from: {", ".join(available)}')
        if self.mitre:
            mt = col.mitre
            if not mt:
                raise ValueError('cannot load mitre data')
            available = set(chain.from_iterable(
                value.keys() for key, value in mt.items()
                if RuleName(key).cloud == cloud
            ))
            not_available = self.mitre - available
            if not_available:
                raise ValueError(
                    f'Not available mitre: {", ".join(not_available)}. '
                    f'Choose from: {", ".join(available)}')
        if self.standard:
            st = col.standard
            if not st:
                raise ValueError('cannot load standards data')
            available = set()
            it = (
                (v or {}) for k, v in st.items()
                if RuleName(k).cloud == cloud
            )
            for st in it:
                available.update(Standard.deserialize_to_strs(st))
                available.update(st.keys())
            not_available = self.standard - available
            if not_available:
                raise ValueError(
                    f'Not available standard: {", ".join(not_available)}. '
                    f'Choose from: {", ".join(available)}')
        return self


class RulesetPatchModel(BaseModel):
    name: str
    version: str

    rules_to_attach: set = Field(default_factory=set)
    rules_to_detach: set = Field(default_factory=set)
    active: bool = Field(None)

    @model_validator(mode='after')
    def at_least_one_given(self) -> Self:
        if not self.rules_to_attach and not self.rules_to_detach and self.active is None:
            raise ValueError(
                'At least one attribute to update must be provided')
        return self


class RulesetDeleteModel(BaseModel):
    name: str
    version: str


class RulesetGetModel(BaseModel):
    """
    GET
    """
    name: str = Field(None)
    version: str = Field(None)
    cloud: RuleDomain = Field(None)
    get_rules: bool = False
    active: bool = Field(None)
    licensed: bool = Field(None)

    @model_validator(mode='after')
    def validate_codependent_params(self) -> Self:
        if self.version and not self.name:
            raise ValueError('\'name\' is required if \'version\' is given')
        if self.name and self.version and (
                self.cloud or self.active is not None):
            raise ValueError(
                'you don\'t have to specify \'cloud\' or \'active\' '
                'if \'name\' and \'version\' are given')
        return self


class RulesetContentGetModel(BaseModel):
    """
    GET
    """
    name: str
    version: str


class RuleDeleteModel(BaseModel):
    rule: str = Field(None)
    cloud: RuleDomain = Field(None)
    git_project_id: str = Field(None)
    git_ref: str = Field(None)

    @model_validator(mode='after')
    def validate_root(self) -> Self:
        if self.git_ref and not self.git_project_id:
            raise ValueError('git_project_id must be specified with git_ref')
        return self


class RuleGetModel(BasePaginationModel):
    """
    GET
    """
    rule: str = Field(None)
    cloud: RuleDomain = Field(None)
    git_project_id: str = Field(None)
    git_ref: str = Field(None)

    @model_validator(mode='after')
    def validate_root(self) -> Self:
        if self.git_ref and not self.git_project_id:
            raise ValueError('git_project_id must be specified with git_ref')
        return self


class RuleUpdateMetaPostModel(BaseModel):
    rule_source_id: str = Field(None)


class RuleSourcePostModel(BaseModel):
    git_project_id: str
    git_url: Annotated[
        str,
        StringConstraints(pattern=r'^https?:\/\/[^\/]+$')
    ] = Field(None)
    git_ref: str = 'main'
    git_rules_prefix: str = '/'
    git_access_type: Annotated[
        Literal['TOKEN'],
        WithJsonSchema({'type': 'string', 'title': 'Git access type',
                        'default': 'TOKEN'})
    ] = 'TOKEN'
    # custom json schema because we don't want "const" key to appear in
    # schema because API gw does not support that,
    # but we do want validation that only TOKEN is supported now
    git_access_secret: str = Field(None)
    description: str

    @model_validator(mode='after')
    def root(self) -> Self:
        self.git_project_id = self.git_project_id.strip('/')
        is_github = self.git_project_id.count('/') == 1
        is_gitlab = self.git_project_id.isdigit()
        if not self.git_url:
            if is_github:
                self.git_url = 'https://api.github.com'
            elif is_gitlab:
                self.git_url = 'https://git.epam.com'
            else:
                raise ValueError(
                    'unknown git_project_id. '
                    'Specify Gitlab project id or Github owner/repo'
                )
        if is_gitlab and not self.git_access_secret:
            raise ValueError('git_access_secret is required for GitLab')
        return self


class RuleSourcePatchModel(BaseModel):
    id: str
    git_access_type: Annotated[
        Literal['TOKEN'],
        WithJsonSchema({'type': 'string', 'title': 'Git access type',
                        'default': 'TOKEN'})
    ] = 'TOKEN'
    git_access_secret: str = Field(None)
    description: str = Field(None)

    @model_validator(mode='after')
    def validate_any_to_update(self) -> Self:
        if not self.git_access_secret and not self.description:
            raise ValueError('Provide data to update')
        return self


class RuleSourceDeleteModel(BaseModel):
    id: str


class RuleSourceGetModel(BaseModel):
    """
    GET
    """
    id: str = Field(None)
    git_project_id: str = Field(None)


class RolePostModel(BaseModel):
    name: str
    policies: set[str]
    expiration: datetime = Field(
        default_factory=lambda: utc_datetime() + timedelta(days=365)
    )
    description: str


class RolePatchModel(BaseModel):
    policies_to_attach: set[str] = Field(default_factory=set)
    policies_to_detach: set[str] = Field(default_factory=set)
    expiration: datetime = Field(None)
    description: str = Field(None)

    @model_validator(mode='after')
    def to_attach_or_to_detach(self) -> Self:
        if not self.policies_to_detach and not self.policies_to_attach and not self.expiration:
            raise ValueError('Provide some parameter to update')
        return self


class PolicyPostModel(BaseModel):
    name: str
    permissions: set[Permission] = Field(default_factory=set)
    permissions_admin: bool = False
    effect: PolicyEffect
    tenants: set[str] = Field(default_factory=set)
    description: str
    # todo add effect and tenants

    @field_validator('permissions', mode='after')
    @classmethod
    def validate_hidden(cls, permission: set[Permission]) -> set[Permission]:
        if not_allowed := permission & set(Permission.iter_disabled()):
            raise ValueError(f'Permissions: {", ".join(not_allowed)} are '
                             f'currently not allowed')
        return permission

    @model_validator(mode='after')
    def _(self) -> Self:
        if not self.permissions_admin and not self.permissions:
            raise ValueError('Provide either permissions or permissions_admin')
        if self.permissions_admin:
            self.permissions = set(Permission.iter_enabled())
        return self


class PolicyPatchModel(BaseModel):
    permissions_to_attach: set[Permission] = Field(default_factory=set)
    permissions_to_detach: set[Permission] = Field(default_factory=set)
    effect: PolicyEffect = Field(None)
    tenants_to_add: set[str] = Field(default_factory=set)
    tenants_to_remove: set[str] = Field(default_factory=set)
    description: str = Field(None)

    @field_validator('permissions_to_attach', mode='after')
    @classmethod
    def validate_hidden(cls, permission: set[Permission]) -> set[Permission]:
        if not_allowed := permission & set(Permission.iter_disabled()):
            raise ValueError(f'Permissions: {", ".join(not_allowed)} are '
                             f'currently not allowed')
        return permission

    @model_validator(mode='after')
    def _(self) -> Self:
        if not any((self.permissions_to_attach, self.permissions_to_detach,
                    self.effect, self.tenants_to_add, self.tenants_to_remove,
                    self.description)):
            raise ValueError('Provide some attribute to update')
        return self


class JobGetModel(TimeRangedMixin, BasePaginationModel):
    start_iso: datetime | date = Field(None, alias='from')
    end_iso: datetime | date = Field(None, alias='to')
    tenant_name: str = Field(None)
    status: JobState = Field(None)

    @classmethod
    def max_range(cls) -> timedelta:
        return timedelta(days=365)


class AWSCredentials(PydanticBaseModel):
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_SESSION_TOKEN: str = Field(None)
    AWS_DEFAULT_REGION: str = 'us-east-1'


# TODO, add certificates & username-password creds
#  https://learn.microsoft.com/en-us/dotnet/api/azure.identity.environmentcredential?view=azure-dotnet
class AZURECredentials(PydanticBaseModel):
    AZURE_TENANT_ID: str
    AZURE_CLIENT_ID: str
    AZURE_CLIENT_SECRET: str
    AZURE_SUBSCRIPTION_ID: str = Field(None)


class GOOGLECredentials1(PydanticBaseModel):
    type: str
    project_id: str
    private_key_id: str
    private_key: str
    client_email: str
    client_id: str
    auth_uri: str
    token_uri: str
    auth_provider_x509_cert_url: str
    client_x509_cert_url: str


class GOOGLECredentials2(PydanticBaseModel):
    type: str
    access_token: str
    refresh_token: str
    client_id: str
    client_secret: str
    project_id: str


class GOOGLECredentials3(PydanticBaseModel):
    access_token: str
    project_id: str


class JobPostModel(BaseModel):
    credentials: AWSCredentials | AZURECredentials | GOOGLECredentials1 | GOOGLECredentials2 | GOOGLECredentials3 = Field(
        None)
    tenant_name: str
    target_rulesets: set[str] = Field(default_factory=set)
    target_regions: set[str] = Field(default_factory=set)
    rules_to_scan: set[str] = Field(default_factory=set)
    # todo allow to provide desired license key


class StandardJobPostModel(BaseModel):
    """
    standard jobs means not licensed job -> without licensed rule-sets
    """
    credentials: AWSCredentials | AZURECredentials | GOOGLECredentials1 | GOOGLECredentials2 | GOOGLECredentials3 = Field(
        None)
    tenant_name: str
    target_rulesets: set[str] = Field(default_factory=set)
    target_regions: set[str] = Field(default_factory=set)


class ScheduledJobPostModel(BaseModel):
    schedule: str
    tenant_name: str = Field(None)
    name: str = Field(None)
    target_rulesets: set[str] = Field(default_factory=set)
    target_regions: set[str] = Field(default_factory=set)


class ScheduledJobGetModel(BaseModel):
    """
    GET
    """
    tenant_name: str = Field(None)


class ScheduledJobPatchModel(BaseModel):
    schedule: str = Field(None)
    enabled: bool = Field(None)

    @model_validator(mode='after')
    def at_least_one_given(self) -> Self:
        if not self.schedule and self.enabled is None:
            raise ValueError('Provide attributes to update')
        return self


class EventPostModel(BaseModel):
    version: Annotated[
        Literal['1.0.0'],
        WithJsonSchema({'type': 'string', 'title': 'Event type version',
                        'default': '1.0.0'})
    ] = '1.0.0'
    vendor: Literal['AWS', 'MAESTRO']
    events: list[dict]


class UserPasswordResetPostModel(BaseModel):
    password: str
    username: str = Field(None)

    # validators
    @field_validator('password')
    @classmethod
    def _(cls, password: str) -> str:
        errors = []
        upper = any(char.isupper() for char in password)
        numeric = any(char.isdigit() for char in password)
        symbol = any(not char.isalnum() for char in password)
        if not upper:
            errors.append('must have uppercase characters')
        if not numeric:
            errors.append('must have numeric characters')
        if not symbol:
            errors.append('must have symbol characters')
        if len(password) < 8:
            errors.append('valid min length for password: 8')

        if errors:
            raise ValueError(', '.join(errors))

        return password


class SignInPostModel(PydanticBaseModel):
    username: str
    password: str


class RefreshPostModel(PydanticBaseModel):
    refresh_token: str


class MailSettingGetModel(BaseModel):
    """
    GET
    """
    disclose: bool = False


class MailSettingPostModel(BaseModel):
    username: str
    password: str
    password_alias: str
    port: int
    host: str
    max_emails: int = 1
    default_sender: str
    use_tls: bool = False


class ReportsSendingSettingPostModel(BaseModel):
    enable: bool = True


class LicenseManagerConfigSettingPostModel(BaseModel):
    host: str
    port: int = Field(None)
    protocol: Annotated[
        Literal['HTTP', 'HTTPS', 'http', 'https'],
        StringConstraints(to_upper=True)
    ] = Field(None)
    stage: str = Field(None)
    api_version: str = Field(None)



class LicenseManagerClientSettingPostModel(BaseModel):
    key_id: str
    algorithm: Annotated[
        Literal['ECC:p521_DSS_SHA:256'],
        WithJsonSchema({'type': 'string', 'title': 'LM algorithm',
                        'default': 'ECC:p521_DSS_SHA:256'})
    ] = 'ECC:p521_DSS_SHA:256'
    private_key: str
    b64_encoded: bool

    @model_validator(mode='after')
    def check_properly_encoded_key(self) -> Self:
        if not self.b64_encoded:
            return self
        try:
            self.private_key = standard_b64decode(self.private_key).decode()
        except (TypeError, BaseException):
            raise ValueError(
                '\'private_key\' must be a safe to decode'
                ' base64-string.'
            )
        return self


class LicenseManagerClientSettingDeleteModel(BaseModel):
    key_id: str


class BatchResultsQueryModel(BasePaginationModel):
    tenant_name: str = Field(None)

    start: datetime = Field(None)
    end: datetime = Field(None)



# reports
class JobFindingsReportGetModel(BaseModel):
    job_type: JobType = JobType.MANUAL
    href: bool = False
    obfuscated: bool = False


class TenantJobsFindingsReportGetModel(TimeRangedMixin, BaseModel):
    start_iso: datetime | date = Field(None, alias='from')
    end_iso: datetime | date = Field(None, alias='to')
    job_type: JobType = Field(None)
    href: bool = False
    obfuscated: bool = False


class JobDetailsReportGetModel(BaseModel):
    job_type: JobType = JobType.MANUAL
    href: bool = False
    obfuscated: bool = False


class TenantJobsDetailsReportGetModel(TimeRangedMixin, BaseModel):
    start_iso: datetime | date = Field(None, alias='from')
    end_iso: datetime | date = Field(None, alias='to')
    job_type: JobType = Field(None)
    href: bool = False
    obfuscated: bool = False


class JobDigestReportGetModel(BaseModel):
    job_type: JobType = JobType.MANUAL


class TenantJobsDigestsReportGetModel(TimeRangedMixin, BaseModel):
    start_iso: datetime | date = Field(None, alias='from')
    end_iso: datetime | date = Field(None, alias='to')
    job_type: JobType = Field(None)


class JobComplianceReportGetModel(BaseModel):
    job_type: JobType = JobType.MANUAL
    format: ReportFormat = ReportFormat.JSON
    href: bool = False


class TenantComplianceReportGetModel(BaseModel):
    format: ReportFormat = ReportFormat.JSON
    href: bool = False


class JobErrorReportGetModel(BaseModel):
    job_type: JobType = JobType.MANUAL
    href: bool = False
    format: ReportFormat = ReportFormat.JSON
    error_type: PolicyErrorType = Field(None)


class JobRuleReportGetModel(BaseModel):
    job_type: JobType = JobType.MANUAL
    href: bool = False
    format: ReportFormat = ReportFormat.JSON


class TenantRuleReportGetModel(TimeRangedMixin, BaseModel):
    start_iso: datetime | date = Field(None, alias='from')
    end_iso: datetime | date = Field(None, alias='to')
    job_type: JobType = Field(None)


class ReportPushByJobIdModel(BaseModel):
    """
    /reports/push/dojo/{job_id}/
    /reports/push/security-hub/{job_id}/
    """
    type: JobType = JobType.MANUAL


class ReportPushMultipleModel(TimeRangedMixin, BaseModel):
    """
    /reports/push/dojo
    /reports/push/security-hub
    """
    tenant_name: str
    start_iso: datetime | date = Field(None, alias='from')
    end_iso: datetime | date = Field(None, alias='to')
    type: JobType = Field(None)


class EventDrivenRulesetGetModel(BaseModel):
    cloud: RuleDomain = Field(None)
    get_rules: bool = False


class EventDrivenRulesetPostModel(BaseModel):
    # name: str
    cloud: RuleDomain
    version: float
    rules: list = Field(default_factory=list)
    # rule_version: Optional[float]


class EventDrivenRulesetDeleteModel(BaseModel):
    # name: str
    cloud: RuleDomain
    version: float


class ProjectGetReportModel(BaseModel):
    tenant_display_names: set[str]
    types: set[Literal['OVERVIEW', 'RESOURCES', 'COMPLIANCE', 'ATTACK_VECTOR', 'FINOPS']] = Field(default_factory=set)
    receivers: set[str] = Field(default_factory=set)
    attempt: SkipJsonSchema[int] = 0
    execution_job_id: SkipJsonSchema[str] = Field(None)


class OperationalGetReportModel(BaseModel):
    tenant_names: set[str]
    types: set[Literal['OVERVIEW', 'RESOURCES', 'COMPLIANCE', 'RULE', 'ATTACK_VECTOR', 'FINOPS', 'KUBERNETES']] = Field(default_factory=set)
    receivers: set[str] = Field(default_factory=set)
    attempt: SkipJsonSchema[int] = 0
    execution_job_id: SkipJsonSchema[str] = Field(None)


class DepartmentGetReportModel(BaseModel):
    types: set[Literal['TOP_RESOURCES_BY_CLOUD', 'TOP_TENANTS_RESOURCES', 'TOP_TENANTS_COMPLIANCE', 'TOP_COMPLIANCE_BY_CLOUD', 'TOP_TENANTS_ATTACKS', 'TOP_ATTACK_BY_CLOUD']] = Field(default_factory=set)
    attempt: SkipJsonSchema[int] = 0
    execution_job_id: SkipJsonSchema[str] = Field(None)


class CLevelGetReportModel(BaseModel):
    types: set[Literal['OVERVIEW', 'COMPLIANCE', 'ATTACK_VECTOR']] = Field(default_factory=set)
    attempt: SkipJsonSchema[int] = 0
    execution_job_id: SkipJsonSchema[str] = Field(None)


class HealthCheckQueryModel(BaseModel):
    status: HealthCheckStatus = Field(None)


class RabbitMQPostModel(BaseModel):
    maestro_user: str
    rabbit_exchange: str = Field(None)
    request_queue: str
    response_queue: str
    sdk_access_key: str
    connection_url: AmqpDsn
    sdk_secret_key: str


class RabbitMQGetModel(BaseModel):
    pass


class RabbitMQDeleteModel(BaseModel):
    pass


class RawReportGetModel(BaseModel):
    obfuscated: bool = False
    meta: bool = False


class ResourcesReportGetModel(BaseModel):
    model_config = ConfigDict(extra='allow')

    resource_type: Annotated[
        str, StringConstraints(to_lower=True, strip_whitespace=True)] = Field(
        None)
    region: AllRegionsWithGlobal = Field(None)

    full: bool = False
    obfuscated: bool = False
    exact_match: bool = True
    search_by_all: bool = False
    format: ReportFormat = ReportFormat.JSON
    href: bool = False

    @field_validator('region', mode='before')
    def _(cls, v: str) -> str:
        if v and isinstance(v, str):
            v = v.lower()
        return v

    @property
    def extras(self) -> dict:
        """
        These attributes will be used to look for resources
        """
        return self.__pydantic_extra__

    @model_validator(mode='after')
    def root(self) -> Self:
        if self.search_by_all and not self.__pydantic_extra__:
            raise ValueError('If search_by_all, an least one query to search '
                             'by must be provided')
        if self.obfuscated and self.format == ReportFormat.JSON and not self.href:
            raise ValueError('Obfuscation is currently not supported for '
                             'raw json report')
        return self

    # all the other attributes to search by can be provided as well.
    # They are not declared


class PlatformK8sResourcesReportGetModel(BaseModel):
    model_config = ConfigDict(extra='allow')

    resource_type: Annotated[
        str, StringConstraints(to_lower=True, strip_whitespace=True)] = Field(
        None)
    full: bool = False
    obfuscated: bool = False
    exact_match: bool = True
    search_by_all: bool = False
    format: ReportFormat = ReportFormat.JSON
    href: bool = False

    @property
    def extras(self) -> dict:
        """
        These attributes will be used to look for resources
        """
        return self.__pydantic_extra__

    @model_validator(mode='after')
    def root(self) -> Self:
        if self.search_by_all and not self.__pydantic_extra__:
            raise ValueError('If search_by_all, an least one query to search '
                             'by must be provided')
        if self.obfuscated and self.format == ReportFormat.JSON and not self.href:
            raise ValueError('Obfuscation is currently not supported for '
                             'raw json report')
        return self

    # all the other attributes to search by can be provided as well.
    # They are not declared


class ResourceReportJobsGetModel(TimeRangedMixin, BaseModel):
    model_config = ConfigDict(extra='allow')

    start_iso: datetime | date = Field(None, alias='from')
    end_iso: datetime | date = Field(None, alias='to')
    job_type: JobType = JobType.MANUAL

    resource_type: Annotated[
        str, StringConstraints(to_lower=True, strip_whitespace=True)] = None
    region: AllRegionsWithGlobal = Field(None)
    full: bool = False
    exact_match: bool = True
    search_by_all: bool = False

    @field_validator('region', mode='before')
    def _(cls, v: str) -> str:
        if v and isinstance(v, str):
            v = v.lower()
        return v

    @property
    def extras(self) -> dict:
        """
        These attributes will be used to look for resources
        """
        return self.__pydantic_extra__

    @model_validator(mode='after')
    def root(self) -> Self:
        if self.search_by_all and not self.__pydantic_extra__:
            raise ValueError('If search_by_all, an least one query to search '
                             'by must be provided')
        return self


class ResourceReportJobGetModel(BaseModel):
    model_config = ConfigDict(extra='allow')

    job_type: JobType = JobType.MANUAL

    resource_type: Annotated[
        str, StringConstraints(to_lower=True, strip_whitespace=True)] = Field(
        None)
    region: AllRegionsWithGlobal = Field(None)
    full: bool = False
    obfuscated: bool = False
    exact_match: bool = True
    search_by_all: bool = False
    href: bool = False

    @field_validator('region', mode='before')
    def _(cls, v: str) -> str:
        if v and isinstance(v, str):
            v = v.lower()
        return v

    @property
    def extras(self) -> dict:
        """
        These attributes will be used to look for resources
        """
        return self.__pydantic_extra__

    @model_validator(mode='after')
    def root(self) -> Self:
        if self.search_by_all and not self.__pydantic_extra__:
            raise ValueError('If search_by_all, an least one query to search '
                             'by must be provided')
        if self.obfuscated and not self.href:
            raise ValueError('Currently obfuscation is supported only if href '
                             'is true')
        return self


class PlatformK8SPostModel(BaseModel):
    tenant_name: Annotated[
        str, StringConstraints(to_upper=True, strip_whitespace=True)]
    name: str
    region: AllRegions = Field(None)
    type: PlatformType
    description: str

    endpoint: HttpUrl = Field(None)
    certificate_authority: str = Field(None)  # base64 encoded
    token: str = Field(None)

    @model_validator(mode='after')
    def root(self) -> Self:
        if (self.type != PlatformType.SELF_MANAGED
                and not self.region):
            raise ValueError('region is required if platform is cloud managed')
        if self.type != PlatformType.EKS and not self.endpoint:
            raise ValueError('endpoint must be '
                             'specified if type is not EKS')
        return self


class PlatformK8sQueryModel(BaseModel):
    tenant_name: str = Field(None)


class K8sJobPostModel(BaseModel):
    """
    K8s platform job
    """
    platform_id: str
    target_rulesets: set[str] = Field(default_factory=set)
    token: str = Field(None)  # temp jwt token


class ReportStatusGetModel(BaseModel):
    """
    GET
    """
    job_id: str
    complete: bool = False


class MetricsStatusGetModel(TimeRangedMixin, BaseModel):
    start_iso: datetime | date = Field(None, alias='from')
    end_iso: datetime | date = Field(None, alias='to')

    @classmethod
    def max_range(cls) -> timedelta:
        return timedelta(days=365)

    @classmethod
    def skip_validation_if_no_input(cls):
        return True


class LicensePostModel(BaseModel):
    tenant_license_key: str = Field(alias='license_key')
    description: str = 'Custodian license'


class LicenseActivationPutModel(BaseModel):
    tenant_names: set[str] = Field(default_factory=set)

    all_tenants: bool = False
    clouds: set[Literal['AWS', 'AZURE', 'GOOGLE', 'KUBERNETES']] = Field(default_factory=set)
    exclude_tenants: set[str] = Field(default_factory=set)

    @model_validator(mode='after')
    def _(self) -> Self:
        if self.tenant_names and any((self.all_tenants, self.clouds,
                                      self.exclude_tenants)):
            raise ValueError('do not provide all_tenants, clouds or '
                             'exclude_tenants if specific '
                             'tenant names are provided')
        if not self.all_tenants and not self.tenant_names:
            raise ValueError('either all_tenants or specific tenant names '
                             'must be given')
        if (self.clouds or self.exclude_tenants) and not self.all_tenants:
            raise ValueError('set all tenants to true if you provide clouds '
                             'or excluded')
        return self


class LicenseActivationPatchModel(BaseModel):
    add_tenants: set[str] = Field(default_factory=set)
    remove_tenants: set[str] = Field(default_factory=set)

    @model_validator(mode='after')
    def _(self) -> Self:
        if not self.add_tenants and not self.remove_tenants:
            raise ValueError('provide either add_tenants or remove_tenants')
        return self


class DefectDojoPostModel(BaseModel):
    url: AnyUrl  # = Field(examples=['http://127.0.0.1:8080/api/v2'])  # api gw models does not support examples
    api_key: str

    description: str


class DefectDojoQueryModel(BaseModel):
    pass


class DefectDojoActivationPutModel(BaseModel):
    tenant_names: set[str] = Field(default_factory=set)

    all_tenants: bool = False
    clouds: set[Literal['AWS', 'AZURE', 'GOOGLE']] = Field(default_factory=set)
    exclude_tenants: set[str] = Field(default_factory=set)

    scan_type: Literal['Generic Findings Import', 'Cloud Custodian Scan'] = Field('Generic Findings Import', description='Defect dojo scan type')
    product_type: str = Field('Rule Engine',
                              description='Defect dojo product type name')
    product: str = Field('{tenant_name}',
                         description='Defect dojo product name')
    engagement: str = Field('Rule-Engine Main',
                            description='Defect dojo engagement name')
    test: str = Field('{job_id}', description='Defect dojo test')
    send_after_job: bool = Field(
        False,
        description='Whether to send the results to dojo after each scan'
    )
    attachment: Literal['json', 'xlsx', 'csv'] = Field(None)

    @model_validator(mode='after')
    def _(self) -> Self:
        if self.tenant_names and any((self.all_tenants, self.clouds,
                                      self.exclude_tenants)):
            raise ValueError('do not provide all_tenants, clouds or '
                             'exclude_tenants if specific '
                             'tenant names are provided')
        if not self.all_tenants and not self.tenant_names:
            raise ValueError('either all_tenants or specific tenant names '
                             'must be given')
        if (self.clouds or self.exclude_tenants) and not self.all_tenants:
            raise ValueError('set all tenants to true if you provide clouds '
                             'or excluded')
        return self


class SelfIntegrationPutModel(BaseModel):
    description: str = 'Custodian access application'
    username: str
    password: str
    auto_resolve_access: bool = False
    url: AnyUrl = Field(None)  # full link: https://host.com:port/hello
    results_storage: str = Field(None)

    tenant_names: set[str] = Field(default_factory=set)

    all_tenants: bool = False
    clouds: set[Literal['AWS', 'AZURE', 'GOOGLE']] = Field(default_factory=set)
    exclude_tenants: set[str] = Field(default_factory=set)

    @model_validator(mode='after')
    def _(self) -> Self:
        if self.tenant_names and any((self.all_tenants, self.clouds,
                                      self.exclude_tenants)):
            raise ValueError('do not provide all_tenants, clouds or '
                             'exclude_tenants if specific '
                             'tenant names are provided')
        if not self.all_tenants and not self.tenant_names:
            raise ValueError('either all_tenants or specific tenant names '
                             'must be given')
        if (self.clouds or self.exclude_tenants) and not self.all_tenants:
            raise ValueError('set all tenants to true if you provide clouds '
                             'or excluded')
        if not self.auto_resolve_access and not self.url:
            raise ValueError('url must be given in case '
                             'auto_resolve_access is not True')
        return self


class SelfIntegrationPatchModel(BaseModel):
    add_tenants: set[str] = Field(default_factory=set)
    remove_tenants: set[str] = Field(default_factory=set)

    @model_validator(mode='after')
    def _(self) -> Self:
        if not self.add_tenants and not self.remove_tenants:
            raise ValueError('provide either add_tenants or remove_tenants')
        return self


class TenantExcludedRulesPutModel(BaseModel):
    rules: set[str]


class CustomerExcludedRulesPutModel(BaseModel):
    rules: set[str]


class CredentialsQueryModel(BasePaginationModel):
    cloud: Literal['AWS', 'AZURE', 'GOOGLE']


class CredentialsBindModel(BaseModel):
    tenant_names: set[str] = Field(default_factory=set)

    all_tenants: bool = False
    exclude_tenants: set[str] = Field(default_factory=set)

    @model_validator(mode='after')
    def _(self) -> Self:
        if self.tenant_names and any((self.all_tenants, self.exclude_tenants)):
            raise ValueError('do not provide all_tenants  or '
                             'exclude_tenants if specific '
                             'tenant names are provided')
        if not self.all_tenants and not self.tenant_names:
            raise ValueError('either all_tenants or specific tenant names '
                             'must be given')
        if self.exclude_tenants and not self.all_tenants:
            raise ValueError('set all tenants to true if you provide  '
                             'excluded')
        return self


class UserPatchModel(BaseModel):
    """
    System admin endpoint
    """
    role_name: str = Field(None)
    password: str = Field(None)

    @model_validator(mode='after')
    def at_least_one(self) -> Self:
        if not any((self.role_name, self.password)):
            raise ValueError('provide at least one attribute to update')
        return self

    @field_validator('password')
    @classmethod
    def _(cls, password: str) -> str:
        errors = []
        upper = any(char.isupper() for char in password)
        lower = any(char.islower() for char in password)
        numeric = any(char.isdigit() for char in password)
        if not upper:
            errors.append('must have uppercase characters')
        if not numeric:
            errors.append('must have numeric characters')
        if not lower:
            errors.append('must have lowercase characters')
        if len(password) < 8:
            errors.append('valid min length for password: 8')

        if errors:
            raise ValueError(', '.join(errors))

        return password


class UserPostModel(BaseModel):
    username: str
    role_name: str = Field(None)
    password: str

    @field_validator('username', mode='after')
    @classmethod
    def check_reserved(cls, username: str) -> str:
        if username in ('whoami', 'reset-password'):
            raise ValueError('Such username cannot be used.')
        return username

    @field_validator('password')
    @classmethod
    def _(cls, password: str) -> str:
        errors = []
        upper = any(char.isupper() for char in password)
        lower = any(char.islower() for char in password)
        numeric = any(char.isdigit() for char in password)
        if not upper:
            errors.append('must have uppercase characters')
        if not numeric:
            errors.append('must have numeric characters')
        if not lower:
            errors.append('must have lowercase characters')
        if len(password) < 8:
            errors.append('valid min length for password: 8')

        if errors:
            raise ValueError(', '.join(errors))

        return password


class UserResetPasswordModel(BaseModel):
    new_password: str

    @field_validator('new_password')
    @classmethod
    def _(cls, password: str) -> str:
        errors = []
        upper = any(char.isupper() for char in password)
        lower = any(char.islower() for char in password)
        numeric = any(char.isdigit() for char in password)
        if not upper:
            errors.append('must have uppercase characters')
        if not numeric:
            errors.append('must have numeric characters')
        if not lower:
            errors.append('must have lowercase characters')
        if len(password) < 8:
            errors.append('valid min length for password: 8')

        if errors:
            raise ValueError(', '.join(errors))

        return password


class SignUpModel(PydanticBaseModel):
    username: str
    password: str
    customer_name: str
    customer_display_name: str
    customer_admins: set[str] = Field(default_factory=set)

    @field_validator('password')
    @classmethod
    def _(cls, password: str) -> str:
        errors = []
        upper = any(char.isupper() for char in password)
        lower = any(char.islower() for char in password)
        numeric = any(char.isdigit() for char in password)
        if not upper:
            errors.append('must have uppercase characters')
        if not numeric:
            errors.append('must have numeric characters')
        if not lower:
            errors.append('must have lowercase characters')
        if len(password) < 8:
            errors.append('valid min length for password: 8')

        if errors:
            raise ValueError(', '.join(errors))

        return password
