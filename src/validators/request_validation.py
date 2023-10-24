import sys
from base64 import standard_b64decode
from datetime import datetime, timedelta
from enum import Enum
from inspect import getmembers
from itertools import chain
from typing import Dict, List, Optional, Literal, Set, Union

from pydantic import BaseModel as BaseModelPydantic, validator, constr, \
    root_validator, AmqpDsn, AnyUrl, HttpUrl
from pydantic.fields import Field
from typing_extensions import TypedDict
from modular_sdk.commons.constants import ParentScope
from helpers.constants import HealthCheckStatus
from helpers.enums import ParentType, RuleDomain
from helpers.regions import AllRegions, AllRegionsWithMultiregional, AWSRegion
from helpers.reports import Standard
from helpers.time_helper import utc_datetime
from services import SERVICE_PROVIDER
from services.rule_meta_service import RuleName

GitURLType = constr(regex=r'^https?:\/\/[^\/]+$')


def password_must_be_secure(password: str) -> str:
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


def get_day_captured_utc(
        _from: Optional[datetime] = None, _back: bool = True,
        reset: dict = None, shift: dict = None, now: datetime = None
):
    """
    Returns an utc-datetime object, shifted by a given parameters,
    which captures start of the day, as well.
    :return: datetime
    """
    _reset = reset or dict()
    _shift = shift or dict()
    now = now or datetime.utcnow()
    _from = _from or now
    op = _from.__sub__ if _back else _from.__add__
    return op(timedelta(**shift)).replace(**reset)


class BaseModel(BaseModelPydantic):
    class Config:
        use_enum_values = True
        extra = 'forbid'


class PreparedEvent(BaseModel):
    """
    Prepared event historically contains both some request meta and incoming
    body on one level.
    You can inherit your models from this one and use them directly as type
    annotations inside handlers (with validate_kwargs) decorator
    """
    class Config:
        use_enum_values = True
        extra = 'allow'

    httpMethod: str  # GET, POST, ...
    path: str  # without prefix (stage)
    queryStringParameters: Optional[dict]
    user_id: str  # cognito user id
    user_role: str  # user role
    user_customer: str  # customer of the user making the request
    # customer of the user on whose behalf the request must be done.
    # For standard users it's the same as user_customer
    customer: Optional[str]
    tenant: Optional[str]  # for some endpoints defined in restriction_service
    tenants: Optional[List[str]]  # the same as one above

    # other validators can inherit this model and contain body attributes


# FYI: if you are to add a new model, name it according to the pattern:
# "{YOU_NAME}Model". But all the middleware configurations must be
# named without "Model" in the end. In case the class name contains
# "Get", it will be considered as a model for GET request


class RegionState(str, Enum):
    INACTIVE = 'INACTIVE'
    ACTIVE = 'ACTIVE'


class KeyFormat(str, Enum):
    PEM = 'PEM'


class Credentials(TypedDict):
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_DEFAULT_REGION: str
    AWS_SESSION_TOKEN: Optional[str]


class CustomerGetModel(BaseModel):
    name: Optional[str]
    complete: Optional[bool] = False


# License
class LicenseGetModel(BaseModel):
    license_key: Optional[str]
    customer: Optional[str]


class LicenseDeleteModel(BaseModel):
    license_key: str
    customer: Optional[str]


class LicenseSyncPostModel(BaseModel):
    license_key: str


class RulesetPostModel(PreparedEvent):
    name: str
    version: str
    cloud: RuleDomain
    active: bool = True
    tenant_allowance: Optional[List] = []
    customer: Optional[str]

    # if empty, all the rules for cloud is chosen
    rules: Optional[Set] = Field(default_factory=set)
    git_project_id: Optional[str]
    git_ref: Optional[str]

    service_section: Optional[str]
    severity: Optional[str]
    mitre: Set[str] = set()
    standard: Set[str] = set()

    @root_validator(pre=False, skip_on_failure=True, allow_reuse=True)
    def validate_filters(cls, values: dict) -> dict:
        if values.get('git_ref') and not values.get('git_project_id'):
            raise ValueError('git_project_id must be specified with git_ref')
        cloud = values['cloud']
        col = SERVICE_PROVIDER.mappings_collector()
        if values.get('service_section'):
            available = set(
                value for key, value in col.service_section.items()
                if RuleName(key).cloud == cloud
            )
            if values.get('service_section') not in available:
                raise ValueError('Not available service section. '
                                 f'Choose from: {", ".join(available)}')
        if values.get('severity'):
            available = set(
                value for key, value in col.severity.items()
                if RuleName(key).cloud == cloud
            )
            if values.get('severity') not in available:
                raise ValueError('Not available severity. '
                                 f'Choose from: {", ".join(available)}')
        if values.get('mitre'):
            available = set(chain.from_iterable(
                value.keys() for key, value in col.mitre.items()
                if RuleName(key).cloud == cloud
            ))
            not_available = values.get('mitre') - available
            if not_available:
                raise ValueError(
                    f'Not available mitre: {", ".join(not_available)}. '
                    f'Choose from: {", ".join(available)}')
        if values.get('standard'):
            available = set()
            it = (
                (v or {})
                for k, v in col.standard.items()
                if RuleName(k).cloud == cloud
            )
            for st in it:
                available.update(Standard.deserialize(st, return_strings=True))
                available.update(st.keys())
            not_available = values.get('standard') - available
            if not_available:
                raise ValueError(
                    f'Not available standard: {", ".join(not_available)}. '
                    f'Choose from: {", ".join(available)}')
        return values


class RulesetPatchModel(PreparedEvent):
    name: str
    version: str
    customer: Optional[str]
    rules_to_attach: Optional[set] = set()
    rules_to_detach: Optional[set] = set()
    active: Optional[bool]
    tenant_allowance: Optional[list] = []
    tenant_restriction: Optional[list] = []

    @root_validator(pre=False, skip_on_failure=True)
    def at_least_one_given(cls, values: dict):
        is_provided = lambda x: x is not None
        keys = [k for k in values.keys() if
                k not in ['name', 'version', 'customer']]
        if not any(is_provided(v) for v in (values.get(key) for key in keys)):
            raise ValueError(
                f'At least one of {", ".join(keys)} must be provided'
            )
        return values


class RulesetDeleteModel(PreparedEvent):
    name: str
    version: str
    customer: Optional[str]


class RulesetGetModel(PreparedEvent):
    name: Optional[str]
    version: Optional[str]
    cloud: Optional[RuleDomain]
    customer: Optional[str]
    get_rules: Optional[bool] = False
    active: Optional[bool]
    licensed: Optional[bool]

    @root_validator(skip_on_failure=True)
    def validate_codependent_params(cls, values: dict) -> dict:
        name, version = values.get('name'), values.get('version')
        if version and not name:
            raise ValueError('\'name\' is required if \'version\' is given')
        if name and version and (values.get('cloud') or values.get('active')):
            raise ValueError(
                'you don\'t have to specify \'cloud\' or \'active\' '
                'if \'name\' and \'version\' are given')
        return values


class RulesetContentGetModel(PreparedEvent):
    name: str
    version: str
    customer: Optional[str]


# Rules
class RuleDeleteModel(PreparedEvent):
    rule: Optional[str]
    customer: Optional[str]
    cloud: Optional[RuleDomain]
    git_project_id: Optional[str]
    git_ref: Optional[str]

    @root_validator(pre=False)
    def validate_root(cls, values: dict) -> dict:
        if values.get('git_ref') and not values.get('git_project_id'):
            raise ValueError('git_project_id must be specified with git_ref')
        return values


class RuleGetModel(PreparedEvent):
    rule: Optional[str]
    cloud: Optional[RuleDomain]
    git_project_id: Optional[str]
    git_ref: Optional[str]
    customer: Optional[str]
    limit: Optional[int] = 50
    next_token: Optional[str]

    @root_validator(pre=False)
    def validate_root(cls, values: dict) -> dict:
        if values.get('git_ref') and not values.get('git_project_id'):
            raise ValueError('git_project_id must be specified with git_ref')
        return values


class RuleUpdateMetaPostModel(BaseModel):
    customer: Optional[str]
    rule_source_id: Optional[str]


# Rule Sources
class GitAccessType(str, Enum):
    TOKEN = 'TOKEN'


class RuleSourcePostModel(BaseModel):
    git_project_id: str
    git_url: Optional[GitURLType]
    git_ref: str = 'main'
    git_rules_prefix: str = '/'
    git_access_type: GitAccessType = 'TOKEN'
    git_access_secret: Optional[str]
    customer: Optional[str]
    tenant_allowance: Optional[list] = []
    description: str

    @root_validator(pre=False, skip_on_failure=True)
    def root(cls, values: dict) -> dict:
        values['git_project_id'] = values['git_project_id'].strip('/')
        is_github = values['git_project_id'].count('/') == 1
        is_gitlab = values['git_project_id'].isdigit()
        if not values.get('git_url'):
            if is_github:
                values['git_url'] = 'https://api.github.com'
            elif is_gitlab:
                values['git_url'] = 'https://git.epam.com'
            else:
                raise ValueError(
                    'unknown git_project_id. '
                    'Specify Gitlab project id or Github owner/repo'
                )
        if is_gitlab and not values.get('git_access_secret'):
            raise ValueError('git_access_secret is required for GitLab')
        return values


class RuleSourcePatchModel(BaseModel):
    id: str
    customer: Optional[str]
    git_access_type: Optional[GitAccessType]
    git_access_secret: Optional[str]
    tenant_allowance: Optional[list]
    tenant_restriction: Optional[list]
    description: Optional[str]

    @root_validator(pre=False)
    def validate_any_to_update(cls, values):
        attrs = [
            'git_access_type',
            'git_access_secret',
            'tenant_allowance',
            'tenant_restriction',
            'description'
        ]
        missing = [attr for attr in attrs
                   if attr not in values or values[attr] is None]
        if len(missing) == len(attrs):
            msg = 'Request requires at least one of the following parameters: '
            msg += ', '.join(map("'{}'".format, attrs))
            raise ValueError(msg)
        return values


class RuleSourceDeleteModel(BaseModel):
    id: str
    customer: Optional[str]


class RuleSourceGetModel(BaseModel):
    id: Optional[str]
    git_project_id: Optional[str]
    customer: Optional[str]


# Tenants
class TenantGetModel(BaseModel):
    tenant_name: Optional[str]
    customer: Optional[str]
    cloud_identifier: Optional[str]
    complete: Optional[bool] = False
    limit: Optional[int] = 10
    next_token: Optional[str]


class TenantPostModel(BaseModel):
    name: str
    display_name: Optional[str]
    cloud: Literal['AWS', 'AZURE', 'GOOGLE']
    cloud_identifier: str
    primary_contacts: List[str] = []
    secondary_contacts: List[str] = []
    tenant_manager_contacts: List[str] = []
    default_owner: Optional[str]

    @root_validator(skip_on_failure=True)
    def set_display_name(cls, values):
        if not values.get('display_name'):
            values['display_name'] = values['name']
        return values


class TenantPatchModel(BaseModel):
    tenant_name: str
    rules_to_exclude: Optional[Set[str]]
    rules_to_include: Optional[Set[str]]

    # send_scan_result: Optional[bool]

    @root_validator(skip_on_failure=True)
    def at_least_one_given(cls, values: dict):
        is_provided = lambda x: x is not None
        keys = [k for k in values.keys() if k != 'tenant_name']
        if not any(is_provided(v) for v in (values.get(key) for key in keys)):
            raise ValueError(
                f'At least one of {", ".join(keys)} must be provided'
            )
        return values


class TenantDeleteModel(BaseModel):
    pass


class TenantRegionPostModel(BaseModel):
    tenant_name: Optional[str]
    region: str  # means native region name

    @validator('region')
    def validate_region(cls, value):
        """
        Of course, we can use typing "region: AllRegions", but the output is
        huge is validation fails
        """
        if not AllRegions.has(value):
            raise ValueError(f'Not known region: {value}')
        return value


# Findings
class FindingsDataType(str, Enum):
    map_type = 'map_type'
    list_type = 'list_type'


class FindingsExpandOn(str, Enum):
    resources = 'resources'


class FindingsGetModel(BaseModel):
    tenant_name: Optional[str]
    # Output type.
    get_url: Optional[bool] = False
    raw: Optional[bool] = False
    # Format scheming.
    expand_on: FindingsExpandOn
    data_type: FindingsDataType
    # Filter scheming.
    # Explicit strings.
    map_key: Optional[str]
    dependent_inclusion: Optional[bool] = False
    rules_to_include: Optional[str]
    resource_types_to_include: Optional[str]
    regions_to_include: Optional[str]
    severities_to_include: Optional[str]


class FindingsDeleteModel(BaseModel):
    tenant_name: Optional[str]


# Credential manager
class CredentialsManagerPostModel(BaseModel):
    cloud_identifier: str
    trusted_role_arn: str
    cloud: Literal['AWS', 'AZURE', 'GCP']
    enabled: bool = True


class CredentialsManagerDeleteModel(BaseModel):
    cloud_identifier: str
    cloud: Literal['AWS', 'AZURE', 'GCP']


class CredentialsManagerPatchModel(BaseModel):
    cloud_identifier: str
    cloud: Literal['AWS', 'AZURE', 'GCP']
    trusted_role_arn: Optional[str]
    enabled: Optional[bool]


class CredentialsManagerGetModel(BaseModel):
    cloud_identifier: Optional[str]
    cloud: Optional[Literal['AWS', 'AZURE', 'GCP']]


# Role
class RolePostModel(BaseModel):
    name: str
    policies: Set[str]
    customer: Optional[str]
    expiration: Optional[datetime] = Field(
        default_factory=lambda: utc_datetime() + timedelta(days=60)
    )


class RoleDeleteModel(BaseModel):
    name: str
    customer: Optional[str]


class RolePatchModel(BaseModel):
    name: str
    policies_to_attach: Optional[Set[str]] = Field(default_factory=set)
    policies_to_detach: Optional[Set[str]] = Field(default_factory=set)
    expiration: Optional[datetime]
    customer: Optional[str]

    @root_validator(skip_on_failure=True)
    def to_attach_or_to_detach(cls, values: dict) -> dict:
        required = ('policies_to_attach', 'policies_to_detach', 'expiration')
        if not any(values.get(key) for key in required):
            raise ValueError(f'At least one of: {", ".join(required)} '
                             f'must be specified')
        return values


class RoleGetModel(BaseModel):
    name: Optional[str]
    customer: Optional[str]


class RoleCacheDeleteModel(BaseModel):
    customer: Optional[str]
    name: Optional[str]


PermissionType = constr(regex=r'^([\w-]+|\*):([\w-]+|\*)$')


# Policy
class PolicyPostModel(BaseModel):
    # todo add check to pass either permissions or permissions_admin
    name: str
    permissions: Set[PermissionType]
    customer: Optional[str]


class PolicyDeleteModel(BaseModel):
    name: str
    customer: Optional[str]


class PolicyPatchModel(BaseModel):
    name: str
    permissions_to_attach: Set[str] = Field(default_factory=set)
    permissions_to_detach: Set[str] = Field(default_factory=set)
    customer: Optional[str]

    @root_validator(skip_on_failure=True)
    def to_attach_or_to_detach(cls, values: dict) -> dict:
        required = ('permissions_to_attach', 'permissions_to_detach')
        if not any(values.get(key) for key in required):
            raise ValueError(f'At least one of: {", ".join(required)} '
                             f'must be specified')
        return values


class PolicyGetModel(BaseModel):
    name: Optional[str]
    customer: Optional[str]


class PolicyCacheDeleteModel(BaseModel):
    name: Optional[str]
    customer: Optional[str]


# Jobs
class JobGetModel(BaseModel):
    customer: Optional[str]
    tenant_name: Optional[str]
    limit: Optional[int] = 10
    next_token: Optional[str]


class SoloJobGetModel(BaseModel):
    job_id: str
    customer: Optional[str]


class AWSCredentials(BaseModel):
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_SESSION_TOKEN: Optional[str]
    AWS_DEFAULT_REGION: Optional[str] = 'us-east-1'


# TODO, add certificates & username-password creds
#  https://learn.microsoft.com/en-us/dotnet/api/azure.identity.environmentcredential?view=azure-dotnet
class AZURECredentials(BaseModel):
    AZURE_TENANT_ID: str
    AZURE_CLIENT_ID: str
    AZURE_CLIENT_SECRET: str
    AZURE_SUBSCRIPTION_ID: Optional[str]


class GOOGLECredentials1(BaseModel):
    class Config:
        use_enum_values = True
        extra = 'allow'

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


class GOOGLECredentials2(BaseModel):
    type: str
    access_token: str
    refresh_token: str
    client_id: str
    client_secret: str
    project_id: str


class GOOGLECredentials3(BaseModel):
    access_token: str
    project_id: str


class JobPostModel(BaseModel):
    credentials: Optional[Union[
        AWSCredentials, AZURECredentials, GOOGLECredentials1,
        GOOGLECredentials2, GOOGLECredentials3]]
    tenant_name: Optional[str]
    target_rulesets: Optional[Set[str]] = Field(default_factory=set)
    target_regions: Optional[Set[str]] = Field(default_factory=set)
    rules_to_scan: Optional[Set[str]] = Field(default_factory=set)
    # ruleset_license_priority: Optional[Dict[str, List[str]]]
    # check_permission: Optional[bool] = True
    customer: Optional[str]


class StandardJobPostModel(BaseModel):
    """
    standard jobs means not licensed job -> without licensed rule-sets
    """
    credentials: Optional[Union[
        AWSCredentials, AZURECredentials, GOOGLECredentials1,
        GOOGLECredentials2, GOOGLECredentials3]]
    tenant_name: Optional[str]
    target_rulesets: Optional[Set[str]] = Field(default_factory=set)
    target_regions: Optional[Set[str]] = Field(default_factory=set)


class JobDeleteModel(BaseModel):
    job_id: str
    # we don't have this parameter, but it does not spoil anything
    customer: Optional[str]


# ScheduledJobs
class ScheduledJobGetModel(BaseModel):
    customer: Optional[str]
    tenant_name: Optional[str]


class SoloScheduledJobGetModel(BaseModel):
    name: str


class ScheduledJobPostModel(BaseModel):
    schedule: str
    tenant_name: Optional[str]
    customer: Optional[str]
    name: Optional[str]
    target_rulesets: Optional[Set[str]] = Field(default_factory=set)
    target_regions: Optional[Set[str]] = Field(default_factory=set)


class ScheduledJobDeleteModel(BaseModel):
    name: str
    customer: Optional[str]


class ScheduledJobPatchModel(BaseModel):
    name: str
    schedule: Optional[str]
    enabled: Optional[bool]
    customer: Optional[str]

    @root_validator(skip_on_failure=True)
    def at_least_one_given(cls, values: dict):
        is_provided = lambda x: x is not None
        keys = ('enabled', 'schedule')
        if not any(is_provided(v) for v in (values.get(key) for key in keys)):
            raise ValueError(
                f'At least one of {", ".join(keys)} must be provided'
            )
        return values


# Event-driven
class EventVendor(str, Enum):
    AWS = 'AWS'
    MAESTRO = 'MAESTRO'


class EventPostModel(BaseModel):
    version: constr(strip_whitespace=True) = '1.0.0'
    vendor: EventVendor
    events: List[Dict]

    @validator('version', pre=False)
    def allowed_version(cls, value: str) -> str:
        if value not in ('1.0.0',):
            raise ValueError(f'Not allowed event version: {value}')
        return value


class UserPasswordResetPostModel(BaseModel):
    password: str
    username: Optional[str]

    # validators
    _validate_password = validator('password', allow_reuse=True)(
        password_must_be_secure)


# User Tenants
class UserTenantsDeleteModel(BaseModel):
    all: Optional[bool] = False
    tenants: Optional[list]
    target_user: Optional[str]


class UserTenantsPatchModel(BaseModel):
    tenants: List[str]
    target_user: Optional[str]


class UserTenantsGetModel(BaseModel):
    target_user: Optional[str]


# User Role
class UserRoleGetModel(BaseModel):
    target_user: Optional[str]


class UserRolePostModel(BaseModel):
    role: str
    target_user: Optional[str]


class UserRolePatchModel(BaseModel):
    role: str
    target_user: Optional[str]


class UserRoleDeleteModel(BaseModel):
    target_user: Optional[str]


# User Customer
class UserCustomerGetModel(BaseModel):
    target_user: Optional[str]


class UserCustomerPostModel(BaseModel):
    customer: str
    target_user: Optional[str]


class UserCustomerPatchModel(BaseModel):
    customer: str
    target_user: Optional[str]


class UserCustomerDeleteModel(BaseModel):
    target_user: Optional[str]


class UserDeleteModel(BaseModel):
    customer: Optional[str]
    username: Optional[str]


# Signup

class SignUpPostModel(BaseModel):
    username: str
    password: str
    customer: str
    role: str
    tenants: Optional[List[str]]

    # validators
    _validate_password = validator('password', allow_reuse=True)(
        password_must_be_secure)


class SignInPostModel(BaseModel):
    username: str
    password: str


# Report


# Siem
# class DojoConfiguration(BaseModel):
#     host: str
#     api_key: str
#     user: str
#     upload_files: Optional[bool] = False
#     display_all_fields: Optional[bool] = False
#     resource_per_finding: Optional[bool] = False
#
#
# class DojoConfigurationPatch(DojoConfiguration):
#     host: Optional[str]
#     api_key: Optional[str]
#     user: Optional[str]
#
#
# class SecurityHubConfiguration(BaseModel):
#     aws_region: str
#     product_arn: str
#     trusted_role_arn: Optional[str]
#
#
# class SecurityHubConfigurationPatch(DojoConfiguration):
#     aws_region: Optional[str]
#     product_arn: Optional[str]
#
#
# class EntitiesMapping(BaseModel):
#     product_type_name: Optional[str]
#     test_title: Optional[str]
#     product_name: Optional[str]
#     engagement_name: Optional[str]


class MailSettingGetModel(BaseModel):
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


class LicenseManagerConfigSettingPostModel(BaseModel):
    host: str
    port: Optional[int]
    protocol: Optional[Literal['HTTP', 'HTTPS']]
    stage: Optional[str]
    api_version: Optional[str]

    @root_validator(pre=True)
    def _(cls, values: dict) -> dict:
        prot = values.get('protocol')
        if isinstance(prot, str):
            values['protocol'] = prot.upper()
        return values


class LicenseManagerClientSettingsGetModel(BaseModel):
    format: KeyFormat


class LicenseManagerClientSettingPostModel(BaseModel):
    key_id: str
    algorithm: str
    private_key: str
    format: KeyFormat = 'PEM'
    b64_encoded: bool

    @root_validator(pre=True)
    def check_properly_encoded_key(cls, values):
        is_encoded = values.get('b64_encoded')
        key: str = values.get('private_key')
        if is_encoded:
            try:
                key = standard_b64decode(key).decode()
            except (TypeError, BaseException):
                raise ValueError(
                    '\'private_key\' must be a safe to decode'
                    ' base64-string.'
                )
            values['private_key'] = key
        return values


class LicenseManagerClientSettingDeleteModel(BaseModel):
    key_id: str


# BatchResults
class SoleBatchResultsGetModel(BaseModel):
    batch_results_id: str
    customer: Optional[str]


class BatchResultsGetModel(BaseModel):
    tenant_name: Optional[str]
    customer: Optional[str]

    start: Optional[datetime]
    end: Optional[datetime]

    limit: Optional[int] = 10
    next_token: Optional[str]


# Reports

class ReportGetModel(BaseModel):
    job_id: Optional[str]
    tenant_name: Optional[str]
    detailed: Optional[bool] = False
    get_url: Optional[bool] = False


class JobType(str, Enum):
    manual = 'manual'
    reactive = 'reactive'


class ErrorReportFormat(str, Enum):
    json = 'json'
    xlsx = 'xlsx'


class RuleReportFormat(str, Enum):
    json = 'json'
    xlsx = 'xlsx'


RANGE_DAYS = 7


class TimeRangedReportModel(BaseModel):
    """
    Base model which provides time-range constraint
    """
    start_iso: Optional[datetime]
    end_iso: Optional[datetime]

    @root_validator(pre=False)
    def check_start_end_iso_range(cls, values):
        """
        :key values[end]: str, upper bound
        :key values[start]: str, lower bound
        Enforces a constraint of 7 day range.
        """
        now = datetime.utcnow()
        end: Optional[datetime] = values.get('end_iso')
        start: Optional[datetime] = values.get('start_iso')

        if not start:
            # Shifts by RANGE_DAYS days, by default.
            start = get_day_captured_utc(
                _from=end, shift=dict(days=RANGE_DAYS),
                reset=dict(hour=0, minute=0, second=0, microsecond=0),
                now=now
            )
        else:
            assert now >= start, '\'start_iso\' cannot ahead the current time.'

        if not end:
            rem = (now - start).days
            if rem > RANGE_DAYS:
                rem = RANGE_DAYS
            # rem would not be < 0, due to assertion.
            end = get_day_captured_utc(
                _from=start, _back=False, shift=dict(days=rem + 1),
                reset=dict(minute=0, second=0, microsecond=0),
                now=now
            )
        else:
            assert now > end, '\'end_iso\' cannot ahead the current time.'

        if end < start:
            raise ValueError(
                'Value of \'end_iso\' must be >= \'start_iso\' date.'
            )
        elif values.get('end_iso') and (end - start).days > RANGE_DAYS:
            raise ValueError(
                'Range of days between \'start_iso\' and \'end_iso\' must '
                f'not overflow {RANGE_DAYS}.'
            )

        values['end_iso'] = end
        values['start_iso'] = start
        return values


class JobReportGetModel(BaseModel):
    """
    /reports/digests/jobs/{id}
    """
    id: str
    type: JobType = 'manual'
    href: bool = True


class TenantsReportGetModel(BaseModel):
    """
    $child = [digests, details]
    /reports/$child/tenants
    /reports/$child/tenants/jobs
    """
    customer: Optional[str]
    type: Optional[JobType]
    href: bool = True


class TenantReportGetModel(TenantsReportGetModel):
    """
    $child = [digests, details]
    /reports/$child/tenants/{tenant_name}/jobs
    """
    tenant_name: str


class TimeRangedTenantsReportGetModel(TimeRangedReportModel,
                                      TenantsReportGetModel):
    ...


class TimeRangedTenantReportGetModel(TimeRangedReportModel,
                                     TenantReportGetModel):
    ...


# Compliance Reporting
# todo models for more flexible queries, such as compliance by regions.
class JobComplianceReportGetModel(JobReportGetModel):
    ...


class TenantComplianceReportGetModel(TenantReportGetModel):
    ...


# Error Reporting
class JobErrorReportGetModel(JobReportGetModel):
    href: bool = False
    format: Optional[ErrorReportFormat] = 'json'


class TenantsErrorReportGetModel(TimeRangedTenantsReportGetModel):
    href: bool = False
    format: Optional[ErrorReportFormat] = 'json'


class TenantErrorReportGetModel(TimeRangedTenantReportGetModel):
    href: bool = False
    format: Optional[ErrorReportFormat] = 'json'


# Rule Reporting
class JobRuleReportGetModel(JobReportGetModel):
    href: bool = False
    format: Optional[RuleReportFormat] = 'json'
    rule: Optional[str]


class TenantsRuleReportGetModel(TimeRangedTenantsReportGetModel):
    href: bool = False
    format: Optional[RuleReportFormat] = 'json'
    rule: Optional[str]


class TenantRuleReportGetModel(TimeRangedTenantReportGetModel):
    href: bool = False
    format: Optional[RuleReportFormat] = 'json'
    rule: Optional[str]


class ReportPushByJobIdModel(BaseModel):
    """
    /reports/push/dojo/{job_id}/
    /reports/push/security-hub/{job_id}/
    """
    job_id: str
    customer: Optional[str]


class ReportPushMultipleModel(TimeRangedReportModel):
    """
    /reports/push/dojo
    /reports/push/security-hub
    """
    tenant_name: str
    customer: Optional[str]
    type: Optional[JobType]


class EventDrivenRulesetGetModel(PreparedEvent):
    cloud: Optional[RuleDomain]
    get_rules: Optional[bool] = False


class EventDrivenRulesetPostModel(PreparedEvent):
    # name: str
    cloud: RuleDomain
    version: float
    rules: Optional[list] = []
    # rule_version: Optional[float]


class EventDrivenRulesetDeleteModel(PreparedEvent):
    # name: str
    cloud: RuleDomain
    version: float


class AccessApplicationPostModel(BaseModel):
    customer: Optional[str]
    description: Optional[str] = 'Custodian access application'
    username: Optional[str]
    password: Optional[str]
    auto_resolve_access: bool = False
    url: Optional[AnyUrl]  # full link: https://host.com:port/hello
    results_storage: Optional[str]

    @root_validator(skip_on_failure=True)
    def _(cls, values: dict) -> dict:
        if bool(values['username']) ^ bool(values['password']):
            raise ValueError('Both username and password must be given')
        if not values.get('auto_resolve_access') and not values.get('url'):
            raise ValueError('`url` must be given in case '
                             '`auto_resolve_access` is not True')

        return values


class AccessApplicationPatchModel(BaseModel):
    application_id: str
    customer: Optional[str]
    description: Optional[str]
    username: Optional[str]
    password: Optional[str]
    auto_resolve_access: bool = False
    url: Optional[AnyUrl]  # full link: https://host.com:port/hello
    results_storage: Optional[str]

    @root_validator(skip_on_failure=True)
    def _(cls, values: dict) -> dict:
        if bool(values['username']) ^ bool(values['password']):
            raise ValueError('Both username and password must be given')
        return values


class AccessApplicationGetModel(BaseModel):
    customer: Optional[str]
    application_id: str


class AccessApplicationListModel(BaseModel):
    customer: Optional[str]


class AccessApplicationDeleteModel(BaseModel):
    application_id: str
    customer: Optional[str]


class DojoApplicationPostModel(BaseModel):
    customer: Optional[str]
    description: Optional[str] = 'Custodian Defect Dojo'
    api_key: str
    url: AnyUrl  # full link: https://host.com:port/hello


class DojoApplicationPatchModel(BaseModel):
    application_id: str
    customer: Optional[str]
    description: Optional[str]
    url: Optional[AnyUrl]
    api_key: Optional[str]

    @root_validator(pre=False)
    def at_least_one_given(cls, values: dict):
        is_provided = lambda x: bool(x)
        keys = [k for k in values.keys() if
                k not in ['application_id', 'customer']]
        if not any(is_provided(v) for v in (values.get(key) for key in keys)):
            raise ValueError(
                f'At least one of {", ".join(keys)} must be provided'
            )
        return values


class DojoApplicationGetModel(BaseModel):
    customer: Optional[str]
    application_id: str


class DojoApplicationListModel(BaseModel):
    customer: Optional[str]


class DojoApplicationDeleteModel(BaseModel):
    application_id: str
    customer: Optional[str]


class ApplicationPostModel(BaseModel):
    customer: Optional[str]
    description: Optional[str] = 'Custodian application'
    cloud: Optional[RuleDomain]
    access_application_id: Optional[str]
    tenant_license_key: Optional[str]

    @root_validator(skip_on_failure=True)
    def _(cls, values: dict) -> dict:
        _cloud_modifier = (bool(values['access_application_id']) or
                           bool(values['tenant_license_key']))
        _cloud = bool(values['cloud'])

        if _cloud ^ _cloud_modifier:
            raise ValueError(
                'Both cloud and access_application_id or '
                'tenant_license_key must be specified or omitted'
            )
        return values


class ApplicationPatchModel(BaseModel):
    application_id: str
    customer: Optional[str]
    description: Optional[str]
    cloud: Optional[RuleDomain]
    access_application_id: Optional[str]
    tenant_license_key: Optional[str]

    @root_validator(skip_on_failure=True)
    def _(cls, values: dict) -> dict:
        _cloud_modifier = (bool(values['access_application_id']) or
                           bool(values['tenant_license_key']))
        _cloud = bool(values['cloud'])

        if _cloud ^ _cloud_modifier:
            raise ValueError(
                'Both cloud and access_application_id or '
                'tenant_license_key must be specified or omitted'
            )
        return values


class ApplicationGetModel(BaseModel):
    customer: Optional[str]
    application_id: str


class ApplicationListModel(BaseModel):
    customer: Optional[str]


class ApplicationDeleteModel(BaseModel):
    application_id: str
    customer: Optional[str]


class ParentPostModel(PreparedEvent):
    customer: Optional[str]
    application_id: str
    description: Optional[str] = 'Custodian parent'
    cloud: Optional[Literal['AWS', 'AZURE', 'GOOGLE']]
    scope: ParentScope
    rules_to_exclude: Set[str] = Field(default_factory=set)
    type: ParentType
    tenant_name: Optional[str]

    @root_validator(pre=False)
    def _(cls, values: dict) -> dict:
        if (values['scope'] != ParentScope.ALL and
                not values.get('tenant_name')):
            raise ValueError(f'tenant_name is required if scope '
                             f'is {values["scope"]}')
        return values


class ParentPatchModel(PreparedEvent):
    parent_id: str
    application_id: Optional[str]
    description: Optional[str]
    rules_to_exclude: Optional[Set[str]] = Field(default_factory=set)
    rules_to_include: Optional[Set[str]] = Field(default_factory=set)
    customer: Optional[str]

    @root_validator(skip_on_failure=True)
    def at_least_one_given(cls, values: dict):
        is_provided = lambda x: bool(x)
        keys = [k for k in values.keys() if k not in ['parent_id', 'customer']]
        if not any(is_provided(v) for v in (values.get(key) for key in keys)):
            raise ValueError(
                f'At least one of {", ".join(keys)} must be provided'
            )
        return values


class ParentGetModel(BaseModel):
    parent_id: str


class ParentDeleteModel(BaseModel):
    parent_id: str


class ParentListModel(BaseModel):
    customer: Optional[str]


class ProjectGetReportModel(BaseModel):
    tenant_display_names: str
    types: Optional[str]
    customer: Optional[str]
    receivers: Optional[str]


class OperationalGetReportModel(BaseModel):
    tenant_names: str
    types: Optional[str]
    receivers: Optional[str]
    customer: Optional[str]


class DepartmentGetReportModel(BaseModel):
    types: Optional[str]
    customer: Optional[str]


class CLevelGetReportModel(BaseModel):
    types: Optional[str]
    customer: Optional[str]


class HealthCheckGetModel(BaseModel):
    status: Optional[HealthCheckStatus]


class SoleHealthCheckGetModel(BaseModel):
    id: str


class RabbitMQPostModel(BaseModel):
    customer: Optional[str]
    maestro_user: str
    rabbit_exchange: Optional[str]
    request_queue: str
    response_queue: str
    sdk_access_key: str
    connection_url: AmqpDsn
    sdk_secret_key: str


class RabbitMQGetModel(BaseModel):
    customer: Optional[str]


class RabbitMQDeleteModel(BaseModel):
    customer: Optional[str]


class ResourcesReportGet(BaseModel):
    tenant_name: str
    identifier: str
    exact_match: bool = True
    search_by: Optional[List[str]] = []
    search_by_all: bool = False
    resource_type: Optional[constr(to_lower=True,
                                   strip_whitespace=True)]  # choice
    region: Optional[AllRegionsWithMultiregional]

    @validator('tenant_name')
    def upper(cls, value: str) -> str:
        return value.upper()

    @validator('search_by', pre=True)
    def to_list(cls, value: str) -> List[str]:
        if not isinstance(value, str):
            raise ValueError('search_by must be a string')
        return value.lower().split(',')

    @validator('region', pre=False)
    def region_to_lover(cls, value: Optional[str]) -> str:
        if value:
            value = value.strip().lower()
        return value

    @root_validator(pre=False, skip_on_failure=True)
    def root(cls, values: dict) -> dict:
        search_by = values.get('search_by')
        search_by_all = values.get('search_by_all')
        if search_by_all and search_by:
            raise ValueError('search_by must not be specified if '
                             'search_by_all')
        return values


class ResourceReportJobsGet(TimeRangedReportModel, TenantReportGetModel):
    identifier: str

    exact_match: bool = True
    search_by: Optional[List[str]] = []
    search_by_all: bool = False
    resource_type: Optional[str]  # choice
    region: Optional[AllRegionsWithMultiregional]

    @validator('tenant_name')
    def upper(cls, value: str) -> str:
        return value.upper()

    @validator('search_by', pre=True)
    def to_list(cls, value: str) -> List[str]:
        if not isinstance(value, str):
            raise ValueError('search_by must be a string')
        return value.lower().split(',')


class ResourceReportJobGet(JobReportGetModel):
    identifier: str

    exact_match: bool = True
    search_by: Optional[List[str]] = []
    search_by_all: bool = False
    resource_type: Optional[str]  # choice
    region: Optional[AllRegionsWithMultiregional]

    @validator('search_by', pre=True)
    def to_list(cls, value: str) -> List[str]:
        if not isinstance(value, str):
            raise ValueError('search_by must be a string')
        return value.lower().split(',')


class PlatformK8sNativePost(PreparedEvent):
    tenant_name: str
    name: str
    endpoint: HttpUrl
    certificate_authority: str  # base64 encoded
    token: Optional[str]
    description: Optional[str]


class PlatformK8sEksPost(PreparedEvent):
    tenant_name: str
    name: str
    region: AWSRegion
    application_id: str
    description: Optional[str]


class PlatformK8sDelete(PreparedEvent):
    id: str


class PlatformK8sQuery(PreparedEvent):
    tenant_name: Optional[str]


class K8sJobPostModel(PreparedEvent):
    """
    K8s platform job
    """
    platform_id: str
    target_rulesets: Optional[Set[str]] = Field(default_factory=set)
    token: Optional[str]  # temp jwt token


ALL_MODELS = set(
    obj for name, obj in getmembers(sys.modules[__name__])
    if (not name.startswith('_') and isinstance(obj, type) and
        issubclass(obj, BaseModel))
)
ALL_MODELS.remove(BaseModel)

ALL_MODELS_WITHOUT_GET = {
    model for model in ALL_MODELS if 'Get' not in model.__name__
}
