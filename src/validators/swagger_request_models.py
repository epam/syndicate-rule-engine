# classes for swagger models are not instantiated directly in code.
# PreparedEvent models are used instead.
import ast
from base64 import standard_b64decode
from datetime import date, datetime, timedelta, timezone
from typing import Literal, Generator
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
    RuleSourceType,
    GITHUB_API_URL_DEFAULT,
    GITLAB_API_URL_DEFAULT,
    PolicyEffect,
    ReportType,
    Env,
    TAGS_KEY_VALUE_SEPARATOR,
    ServiceOperationType,
)
from helpers import Version, NextToken
from helpers.regions import AllRegions, AllRegionsWithGlobal
from helpers.time_helper import utc_datetime
from services.chronicle_service import ChronicleConverterType
from services.ruleset_service import RulesetName
from models.rule import RuleIndex
from celery.schedules import (
    BaseSchedule,
    schedule as celery_schedule,
    crontab as celery_crontab,
)

DEFAULT_LM_PK_ALGORITHM = 'ECC:p521_DSS_SHA:256'


class BaseModel(PydanticBaseModel):
    model_config = ConfigDict(
        coerce_numbers_to_str=True, populate_by_name=True
    )
    customer_id: SkipJsonSchema[str] = Field(
        default=None,
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
        50, ge=1, le=50, description='Max number of items to return'
    )
    next_token: str = Field(
        None,
        description='Provide next_token received from the previous request',
    )

    @field_validator('next_token', mode='after')
    @classmethod
    def _validate_next_token(cls, next_token):
        NextToken.deserialize(next_token)
        return next_token


class TimeRangedMixin:
    """
    Base model which provides time-range constraint
    """

    # start_iso: datetime | date = Field(None, alias='from')
    # end_iso: datetime | date = Field(None, alias='to')

    @classmethod
    def skip_validation_if_no_input(cls) -> bool:
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
                raise ValueError(
                    "value of 'from' must be less than current date"
                )
        if end:
            end = end.astimezone(timezone.utc)
            if end > now:
                raise ValueError(
                    "value of 'to' must be less than current date"
                )
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
            raise ValueError("value of 'to' must be bigger than 'from' date")

        if (end - start) > max_range:
            raise ValueError(
                f"Time range between 'from' and 'to' must "
                f'not overflow {max_range}'
            )
        self.start_iso = start
        self.end_iso = end
        return self


class ResourcesGetModel(BasePaginationModel):
    tenant_name: str = Field(None)
    resource_type: str = Field(
        None,
        description='Cloud Custodian resource type with or without cloud prefix, e.g. aws.ec2 or ec2',
    )
    location: str = Field(
        None,
        description='Location of the resource, e.g. us-east-1, eastus, europe-west1, etc.',
    )
    name: str = Field(None)
    id: str = Field(None)


class ResourcesArnGetModel(BaseModel):
    arn: str


class ResourcesExceptionsGetModel(BasePaginationModel):
    """
    GET
    """

    tenant_name: str = Field(None)
    customer_name: str = Field(None)
    resource_type: str = Field(
        None,
        description='Cloud Custodian resource type with or without cloud prefix, e.g. aws.ec2 or ec2',
    )
    location: str = Field(
        None,
        description='Location of the resource, e.g. us-east-1, eastus, europe-west1, etc.',
    )
    resource_id: str = Field(None)
    arn: str = Field(
        None, description='ARN for AWS, URN for GOOGLE and ID for AZURE or K8S'
    )
    tags_filters: set[str] = Field(
        default_factory=set,
        description='List of tags to filter resources by.',
        examples=[{'tag1=value1', 'tag2=value2'}],
    )
    include_expired: bool = Field(
        False, description='Include expired exceptions.'
    )

    @field_validator('tags_filters', mode='before')
    @classmethod
    def normalize_tags_filters(cls, value):
        """Convert single string to set for single-value query parameters."""
        if isinstance(value, str):
            return set(ast.literal_eval(value))
        return value

    @field_validator('tags_filters', mode='after')
    @classmethod
    def validate_tags_filters(
        cls, value: set[str] | None
    ) -> set[str] | None:
        if value and not all(
            len(tag.split(TAGS_KEY_VALUE_SEPARATOR)) == 2 for tag in value
        ):
            raise ValueError(
                f'All tags must be strings in the format key{TAGS_KEY_VALUE_SEPARATOR}value'
            )
        return value

    @model_validator(mode='after')
    def validate_arn_and_params(self) -> Self:
        if self.arn and (
            self.tenant_name
            or self.location
            or self.resource_type
            or self.resource_id
            or self.tags_filters
        ):
            raise ValueError(
                'ARN is provided, so tenant_name, location, resource_type, resource_id, and tags_filters should not be specified'
            )

        return self


class ResourcesExceptionsPostModel(BaseModel):
    tenant_name: str
    location: str | None = Field(None)
    resource_type: str | None = Field(None)
    resource_id: str | None = Field(None)
    arn: str | None = Field(
        None, description='ARN for AWS, URN for GOOGLE and ID for AZURE or K8S'
    )
    tags_filters: set[str] | None = Field(
        None,
        description='List of tags to filter resources by.',
        examples=[['tag1=value1', 'tag2=value2']],
    )
    expire_at: datetime | date = Field(
        default=datetime.now() + timedelta(days=1),
        description='Expiration date of the exception',
    )

    @field_validator('tags_filters', mode='before')
    @classmethod
    def normalize_tags_filters(cls, value):
        """Convert single string to set for single-value query parameters."""
        if isinstance(value, str):
            return {value}
        return value

    @field_validator('expire_at', mode='after')
    @classmethod
    def validate_expire_at(cls, value: datetime | date) -> datetime | date:
        if value < datetime.now():
            raise ValueError('Expiration date must be in the future')
        if value > datetime.now() + timedelta(
            days=Env.RESOURCES_EXCEPTIONS_MAX_EXPIRATION_DAYS.as_int()
        ):
            raise ValueError(
                f'Expiration date must be within {Env.RESOURCES_EXCEPTIONS_MAX_EXPIRATION_DAYS.as_int()} days from now'
            )
        return value

    @field_validator('tags_filters', mode='after')
    @classmethod
    def validate_tags_filters(
        cls, value: list[str] | None
    ) -> list[str] | None:
        if value and not all(
            len(tag.split(TAGS_KEY_VALUE_SEPARATOR)) == 2 for tag in value
        ):
            raise ValueError(
                f'All tags must be strings in the format key{TAGS_KEY_VALUE_SEPARATOR}value'
            )
        return value

    @model_validator(mode='after')
    def validate_arn_and_params(self) -> Self:
        is_arn = bool(self.arn)
        is_resource = self.location and self.resource_type and self.resource_id
        is_tags = bool(self.tags_filters)

        if not (is_arn or is_resource or is_tags):
            raise ValueError(
                'At least one of ARN, (location, resource_type, resource_id), or tags_filters must be specified'
            )

        if (
            (is_arn and is_resource)
            or (is_arn and is_tags)
            or (is_resource and is_tags)
        ):
            raise ValueError(
                'Only one of ARN, (location, resource_type, resource_id), or tags_filters must be specified'
            )

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
    cloud: Literal['AWS', 'AZURE', 'GOOGLE', 'GCP', 'KUBERNETES']
    description: str = Field(
        ...,
        description='Human-readable description of the ruleset',
    )
    version: str = Field(
        None,
        description='Ruleset version. If not specified, '
        'will be generated automatically based on github '
        'release of rules or based on the previous ruleset version',
    )
    rule_source_id: str = Field(
        None,
        description='Id of rule source object to get rules from. '
        'If the type of that source is GITHUB_RELEASE, '
        'the version from release tag will be used',
    )
    git_project_id: str = Field(None)
    git_ref: str = Field(None)

    rules: set = Field(default_factory=set)
    excluded_rules: set = Field(default_factory=set)

    platforms: set[str] = Field(
        default_factory=set,
        description='Platform for k8s rules to filter based on',
    )
    categories: set[str] = Field(
        default_factory=set, description='Rules category to use'
    )
    service_sections: set[str] = Field(
        default_factory=set, description='Service section to use'
    )
    sources: set[str] = Field(
        default_factory=set, description='Sources to use'
    )

    @field_validator('platforms', mode='after')
    @classmethod
    def validate_platforms(cls, platforms: set[str]) -> set[str]:
        if not platforms:
            return platforms
        all_platforms = {
            i.lower() for i in RuleIndex.platform_map.values() if i
        }
        platforms = {p.strip().lower() for p in platforms}
        not_existing = platforms - all_platforms
        if not_existing:
            raise ValueError(
                f'not available platforms: {", ".join(not_existing)}. Choose from: {", ".join(all_platforms)}'
            )
        return platforms

    @field_validator('categories', mode='after')
    @classmethod
    def validate_categories(cls, categories: set[str]) -> set[str]:
        if not categories:
            return categories
        all_categories = {
            i.lower() for i in RuleIndex.category_map.values() if i
        }
        categories = {c.strip().lower() for c in categories}
        not_existing = categories - all_categories
        if not_existing:
            raise ValueError(
                f'not available categories: {", ".join(not_existing)}. Choose from: {", ".join(all_categories)}'
            )
        return categories

    @field_validator('service_sections', mode='after')
    @classmethod
    def validate_service_sections(cls, service_sections: set[str]) -> set[str]:
        if not service_sections:
            return service_sections
        all_sections = {
            i.lower() for i in RuleIndex.service_section_map.values() if i
        }
        service_sections = {ss.strip().lower() for ss in service_sections}
        not_existing = service_sections - all_sections
        if not_existing:
            raise ValueError(
                f'not available service sections: {", ".join(not_existing)}. Choose from: {", ".join(all_sections)}'
            )
        return service_sections

    @field_validator('sources', mode='after')
    @classmethod
    def validate_sources(cls, sources: set[str]) -> set[str]:
        if not sources:
            return sources
        all_sources = {i.lower() for i in RuleIndex.source_map.values() if i}
        sources = {s.strip().lower() for s in sources}
        not_existing = sources - all_sources
        if not_existing:
            raise ValueError(
                f'not available sources: {", ".join(not_existing)}. Choose from: {", ".join(all_sources)}'
            )
        return sources

    @field_validator('name', mode='after')
    @classmethod
    def validate_name(cls, name: str) -> str:
        if ':' in name:
            raise ValueError('colon in not allowed in ruleset name')
        return name

    @field_validator('cloud', mode='after')
    @classmethod
    def validate_cloud(cls, cloud: str) -> str:
        if cloud == 'GOOGLE':
            cloud = 'GCP'
        return cloud

    @field_validator('version', mode='after')
    @classmethod
    def validate_version(cls, version: str | None) -> str | None:
        if not version:
            return version
        _ = Version(version)  # raises ValueError
        return version

    @model_validator(mode='after')
    def validate_model(self) -> Self:
        if self.git_ref and not self.git_project_id:
            raise ValueError('git_project_id must be specified with git_ref')
        if self.rule_source_id and (self.git_ref or self.git_project_id):
            raise ValueError(
                'Do not specify git_ref or git_project_id '
                'if rule_source_id is specified'
            )
        return self


class RulesetPatchModel(BaseModel):
    name: str
    version: str = Field(
        None,
        description='A version of the ruleset you want to update. '
        'If not specified, the latest previous ruleset will '
        'be used as base to update',
    )
    new_version: str

    rules_to_attach: set = Field(default_factory=set)
    rules_to_detach: set = Field(default_factory=set)
    force: bool = Field(
        None,
        description='Force the creation of a new ruleset version even if no there is no changes',
    )
    description: str = Field(
        None,
        description='Human-readable description of the ruleset',
    )

    @field_validator('name', mode='after')
    @classmethod
    def validate_name(cls, name: str) -> str:
        if ':' in name:
            raise ValueError('colon in not allowed in ruleset name')
        return name

    @field_validator('version', mode='after')
    @classmethod
    def validate_version(cls, version: str | None) -> str | None:
        if not version:
            return version
        _ = Version(version)  # raise ValueError
        return version

    @field_validator('new_version', mode='after')
    @classmethod
    def validate_new_version(cls, new_version: str) -> str:
        _ = Version(new_version)  # raise ValueError
        return new_version


class RulesetDeleteModel(BaseModel):
    name: str
    version: str = Field(
        description='Specific version to remove. * can be specified to '
        'remove all the versions of a specific ruleset'
    )

    @field_validator('name', mode='after')
    @classmethod
    def validate_name(cls, name: str) -> str:
        if ':' in name:
            raise ValueError('colon in not allowed in ruleset name')
        return name

    @field_validator('version', mode='after')
    @classmethod
    def validate_version(cls, version: str) -> str:
        version = version.strip()
        if version != '*':
            _ = Version(version)
        return version

    @property
    def is_all_versions(self) -> bool:
        return self.version == '*'


class RulesetGetModel(BaseModel):
    """
    GET
    """

    name: str = Field(None)
    version: str = Field(None)
    cloud: RuleDomain = Field(None)
    get_rules: bool = False
    licensed: bool = Field(None)

    @field_validator('version', mode='after')
    @classmethod
    def validate_version(cls, version: str | None) -> str | None:
        if not version:
            return version
        version = version.strip()
        _ = Version(version)
        return version

    @model_validator(mode='after')
    def validate_codependent_params(self) -> Self:
        if self.version and not self.name:
            raise ValueError("'name' is required if 'version' is given")
        if self.name and self.version and self.cloud:
            raise ValueError(
                "you don't have to specify 'cloud' or 'active' "
                "if 'name' and 'version' are given"
            )
        return self


class RulesetReleasePostModel(BaseModel):
    name: str
    version: str = Field(
        None,
        description='Specific version to release to LM. * can be specified to '
        'release all the versions of a specific ruleset. '
        'If not specified, the latest version will be released',
    )
    description: str
    display_name: str

    overwrite: bool = Field(
        default=False,
        description='Determines whether to overwrite an existing ruleset version'
    )

    @field_validator('version', mode='after')
    @classmethod
    def validate_version(cls, version: str | None) -> str | None:
        if not version:
            return version
        version = version.strip()
        if version != '*':
            _ = Version(version)
        return version

    @property
    def is_all_versions(self) -> bool:
        return self.version == '*'

    # other params


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

    model_config = ConfigDict(
        coerce_numbers_to_str=True,
        populate_by_name=True,
        use_enum_values=True,
    )

    rule: str = Field(None)
    cloud: RuleDomain = Field(None)
    git_project_id: str = Field(None)
    git_ref: str = Field(None)
    rule_source_id: str = Field(None)

    @model_validator(mode='after')
    def validate_root(self) -> Self:
        if self.rule_source_id and (self.git_project_id or self.git_ref):
            raise ValueError(
                'Do not specify git_project_id or git_ref if rule_source_id '
                'is given'
            )
        if self.git_ref and not self.git_project_id:
            raise ValueError('git_project_id must be specified with git_ref')
        return self


class RuleUpdateMetaPostModel(BaseModel):
    rule_source_id: str = Field(None)


class RuleSourceValidator:
    """
    Reusable validators for RuleSource fields.
    Can be used both in Pydantic models and in handlers after merging data.
    """

    @staticmethod
    def validate(
        git_project_id: str,
        type_: RuleSourceType,
        git_url: HttpUrl | str,
    ) -> str:
        """
        Full validation of rule source state. Returns normalized git_project_id.
        Validates:
        - git_project_id format (owner/repo or numeric id)
        - type compatibility with git_project_id format
        - git_url for GitHub projects (must contain github.com)
        """
        normalized, is_github, is_gitlab = RuleSourceValidator._parse(git_project_id)

        if not is_github and not is_gitlab:
            raise ValueError(
                'unknown git_project_id. '
                'Specify Gitlab project id or Github owner/repo'
            )

        is_github_type = type_ in (RuleSourceType.GITHUB, RuleSourceType.GITHUB_RELEASE)
        if is_github_type and not is_github:
            raise ValueError('GITHUB is only available for GitHub projects')
        if type_ is RuleSourceType.GITLAB and not is_gitlab:
            raise ValueError('GITLAB is only available for GitLab projects')

        # NOTE: GitLab URL is not validated because it can be any self-hosted URL
        git_url_str = str(git_url).strip().strip('/')
        if is_github and 'github.com' not in git_url_str:
            raise ValueError('GitHub URL must be a valid GitHub URL')

        return normalized

    @staticmethod
    def infer_type(git_project_id: str) -> RuleSourceType:
        """Infer RuleSourceType from git_project_id format."""
        _, is_github, is_gitlab = RuleSourceValidator._parse(git_project_id)
        if is_github:
            return RuleSourceType.GITHUB
        if is_gitlab:
            return RuleSourceType.GITLAB
        raise ValueError(
            'unknown git_project_id. '
            'Specify Gitlab project id or Github owner/repo'
        )

    @staticmethod
    def infer_git_url(git_project_id: str) -> str:
        """Infer default git_url from git_project_id format."""
        _, is_github, is_gitlab = RuleSourceValidator._parse(git_project_id)
        if is_github:
            return GITHUB_API_URL_DEFAULT
        if is_gitlab:
            return GITLAB_API_URL_DEFAULT
        raise ValueError(
            'unknown git_project_id. '
            'Specify Gitlab project id or Github owner/repo'
        )

    @staticmethod
    def _normalize(git_project_id: str) -> str:
        return git_project_id.strip().strip('/')

    @staticmethod
    def _is_github(git_project_id: str) -> bool:
        return git_project_id.count('/') == 1

    @staticmethod
    def _is_gitlab(git_project_id: str) -> bool:
        return git_project_id.isdigit()

    @staticmethod
    def _parse(git_project_id: str) -> tuple[str, bool, bool]:
        """Returns (normalized, is_github, is_gitlab)."""
        normalized = RuleSourceValidator._normalize(git_project_id)
        return (
            normalized,
            RuleSourceValidator._is_github(normalized),
            RuleSourceValidator._is_gitlab(normalized),
        )


class RuleSourcePostModel(BaseModel):
    git_project_id: str  # "141234124" or "epam/ecc"
    description: str
    type: RuleSourceType = Field(
        None,
        description='If not specified will be inferred from git_project_id.',
    )

    git_url: HttpUrl = Field(
        None,
        description=f'If not specified will be inferred from git_project_id. '
        f'"{GITHUB_API_URL_DEFAULT}" will be used for GitHub, '
        f'"{GITLAB_API_URL_DEFAULT}" will be used for GitLab',
    )  # can be inferred
    git_ref: str = Field(
        'main',
        description='Git branch to pull rules from. Not used for '
        'GITHUB_RELEASE',
    )
    git_rules_prefix: str = '/'
    git_access_secret: str = Field(None)

    @property
    def baseurl(self) -> str:
        return self.git_url.scheme + '://' + self.git_url.host

    @model_validator(mode='after')
    def root(self) -> Self:
        if not self.git_url:
            self.git_url = HttpUrl(
                RuleSourceValidator.infer_git_url(self.git_project_id)
            )
        if not self.type:
            self.type = RuleSourceValidator.infer_type(self.git_project_id)

        self.git_project_id = RuleSourceValidator.validate(
            git_project_id=self.git_project_id,
            type_=self.type,
            git_url=self.git_url,
        )
        return self


class RuleSourcePatchModel(BaseModel):
    git_access_secret: str = Field(None)
    description: str = Field(None)
    type: RuleSourceType = Field(None)
    git_url: HttpUrl = Field(None)
    git_ref: str = Field(None)
    git_project_id: str = Field(None)
    git_rules_prefix: str = Field(None)

    @property
    def baseurl(self) -> str | None:
        if self.git_url:
            return self.git_url.scheme + '://' + self.git_url.host
        return None

    @model_validator(mode='after')
    def root(self) -> Self:
        has_update = any([
            self.git_access_secret,
            self.description,
            self.type,
            self.git_url,
            self.git_ref,
            self.git_project_id,
            self.git_rules_prefix,
        ])
        if not has_update:
            raise ValueError('Provide data to update')
        if self.git_project_id:
            self.git_url = self.git_url or HttpUrl(
                RuleSourceValidator.infer_git_url(self.git_project_id)
            )
            self.type = self.type or RuleSourceValidator.infer_type(
                self.git_project_id
            )
        # Other fields are validated in the handler
        # because we need to first merge fields and then validate
        return self


class RuleSourceDeleteModel(BaseModel):
    delete_rules: bool = False


class RuleSourcesListModel(BasePaginationModel):
    type: RuleSourceType = Field(None)
    project_id: str = Field(
        None,
        description='Gitlab project id (12345) or Github project id (epam/ecc)',
    )
    has_secret: bool = Field(None)


class RolePostModel(BaseModel):
    name: str
    policies: set[str]
    expiration: datetime = Field(None)
    description: str

    @field_validator('expiration')
    @classmethod
    def _(cls, expiration: datetime | None) -> datetime | None:
        if not expiration:
            return expiration
        if expiration.tzinfo is None:
            expiration = expiration.replace(tzinfo=timezone.utc)
        else:
            expiration.astimezone(timezone.utc)
        if expiration < datetime.now(tz=timezone.utc):
            raise ValueError('Expiration date has already passed')
        return expiration


class RolePatchModel(BaseModel):
    policies_to_attach: set[str] = Field(default_factory=set)
    policies_to_detach: set[str] = Field(default_factory=set)
    expiration: datetime = Field(None)
    description: str = Field(None)

    @field_validator('expiration')
    @classmethod
    def _(cls, expiration: datetime | None) -> datetime | None:
        if not expiration:
            return expiration
        if expiration.tzinfo is None:
            expiration = expiration.replace(tzinfo=timezone.utc)
        else:
            expiration.astimezone(timezone.utc)
        return expiration

    @model_validator(mode='after')
    def to_attach_or_to_detach(self) -> Self:
        if (
            not self.policies_to_detach
            and not self.policies_to_attach
            and not self.expiration
        ):
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
            raise ValueError(
                f'Permissions: {", ".join(not_allowed)} are '
                f'currently not allowed'
            )
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
            raise ValueError(
                f'Permissions: {", ".join(not_allowed)} are '
                f'currently not allowed'
            )
        return permission

    @model_validator(mode='after')
    def _(self) -> Self:
        if not any(
            (
                self.permissions_to_attach,
                self.permissions_to_detach,
                self.effect,
                self.tenants_to_add,
                self.tenants_to_remove,
                self.description,
            )
        ):
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
    model_config = ConfigDict(extra='allow')

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
    model_config = ConfigDict(extra='allow')

    type: str
    access_token: str
    refresh_token: str
    client_id: str
    client_secret: str
    project_id: str


class GOOGLECredentials3(PydanticBaseModel):
    model_config = ConfigDict(extra='allow')

    access_token: str
    project_id: str


class DojoStructure(PydanticBaseModel):
    product: str | None = None
    engagement: str | None = None
    test: str | None = None


class JobPostModel(BaseModel):
    credentials: (
        AWSCredentials
        | AZURECredentials
        | GOOGLECredentials1
        | GOOGLECredentials2
        | GOOGLECredentials3
    ) = Field(None)
    tenant_name: str
    target_rulesets: set[str] = Field(default_factory=set, alias='rulesets')
    target_regions: set[str] = Field(default_factory=set, alias='regions')
    rules_to_scan: set[str] = Field(default_factory=set, alias='rules')
    timeout_minutes: float = Field(
        None,
        description='Job timeout in minutes. This timeout is soft '
        'meaning that when the desired number of minutes have '
        'passed job termination will be triggered',
    )
    license_key: str = Field(
        None,
        description='License to exhaust for this job. Will be resolved '
        'automatically unless an ambiguous occurs',
    )

    application_id: str = Field(
        None,
        description='Application ID with credentials for this job',
    )

    dojo_product: str = Field(
        None,
        description='Defect Dojo product name to which the results will be '
                    'uploaded',
    )
    dojo_engagement: str = Field(
        None,
        description='Defect Dojo engagement name to which the results will be '
                    'uploaded',
    )
    dojo_test: str = Field(
        None,
        description='Defect Dojo test name to which the results will be '
                    'uploaded',
    )

    @field_validator('target_rulesets', mode='after')
    @classmethod
    def validate_rulesets(cls, value: set[str]) -> set[str]:
        """
        Removes license keys and validates
        :param value:
        :return:
        """
        name_to_items = {}
        rulesets = set()
        for item in value:
            i = RulesetName(item)  # raises ValueError
            name_to_items.setdefault(i.name, []).append(i)
            rulesets.add(RulesetName(i.name, i.version, None).to_str())
        if any(len(items) > 1 for items in name_to_items.values()):
            raise ValueError(
                'Only one version of specific ruleset can be used'
            )
        return rulesets

    def iter_rulesets(self) -> Generator[RulesetName, None, None]:
        yield from map(RulesetName, self.target_rulesets)

    def dojo_structure(self) -> dict:
        return DojoStructure(
            product=self.dojo_product,
            engagement=self.dojo_engagement,
            test=self.dojo_test,
        ).model_dump(exclude_none=True)


def sanitize_schedule(schedule: str) -> str:
    """
    May raise ValueError
    :param schedule:
    :return:
    """
    _rate_error_message = (
        'Invalid rate expression. Use `rate(value, unit)` where '
        'value is a positive number, '
        'unit is one of: minute, minutes, hour, hours, day, days. '
        'Valid examples are: rate(1 hour), rate(2 hours). '
        'If the value is equal to 1, then the unit must be singular.'
    )
    if 'rate' in schedule:
        # consider the value to be rate expression only if explicitly
        # specified "rate"
        try:
            value, unit = (
                schedule.replace('rate', '').strip(' ()').split(maxsplit=1)
            )
            value = int(value)
            if unit not in (
                'minute',
                'minutes',
                'hour',
                'hours',
                'day',
                'days',
                'week',
                'weeks',
                'second',
                'seconds',
            ):
                raise ValueError
            if value < 1:
                raise ValueError
            if (
                value == 1
                and unit.endswith('s')
                or value > 1
                and not unit.endswith('s')
            ):
                raise ValueError
        except ValueError:
            raise ValueError(_rate_error_message)
        return f'rate({value} {unit})'
    # considering it to be a cron expression.
    # Currently, on-prem and saas cron expressions differ. On-prem only
    # accepts standard crontab that contains five fields without year
    # (https://en.wikipedia.org/wiki/Cron),
    # whereas saas accepts expressions that are valid for EventBridge
    # rule (https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-cron-expressions.html).
    # The validator below does not 100% ensure that the expression if
    # valid, but it does some things to make the difference less visible
    raw = schedule.replace('cron', '').strip(' ()').split()
    if len(raw) not in (5, 6):
        raise ValueError(
            'Invalid cron expression. '
            'Must contain 5 fields: '
            '(minute hour day-of-month month day-of-week)'
        )
    if Env.is_docker():
        # on-prem supports only 5 fields and does not support "?"
        raw = ['*' if i == '?' else i for i in raw]
        if len(raw) == 6:
            raw.pop()
    else:
        # saas supports only 6 fields
        if len(raw) == 5:
            raw.append('*')
    return f'cron({" ".join(raw)})'


def to_celery_schedule(sch: int | str) -> 'BaseSchedule':
    if isinstance(sch, int):
        return celery_schedule(timedelta(seconds=sch))
    # isinstance(self.schedule, str)
    # already sanitized so it's either rate() or cron(). Look at
    # sanitize_schedule
    if 'rate' in sch:
        val, unit = sch.replace('rate', '').strip('()').split(maxsplit=1)
        if not unit.endswith('s'):
            unit += 's'

        return celery_schedule(timedelta(**{unit: int(val)}))
    # 'cron' in self.schedule
    items = sch.replace('cron', '').strip('()').split(maxsplit=4)
    return celery_crontab(*items)


class ScheduledJobPostModel(BaseModel):
    schedule: int | str
    tenant_name: str
    name: str = Field(
        None,
        description='Name for this scheduled entry. Will be generated if not provided. Must be unique within a customer',
    )
    description: str
    target_rulesets: set[str] = Field(default_factory=set, alias='rulesets')
    target_regions: set[str] = Field(default_factory=set, alias='regions')

    license_key: str = Field(
        None,
        description='License to exhaust for this job. Will be resolved '
        'automatically unless an ambiguous occurs',
    )

    @field_validator('schedule')
    @classmethod
    def validate_schedule(cls, sch: int | str) -> int | str:
        # TODO: check minimum allowed period
        if isinstance(sch, int):
            return sch
        return sanitize_schedule(sch)

    @field_validator('target_rulesets', mode='after')
    @classmethod
    def validate_rulesets(cls, value: set[str]) -> set[str]:
        """
        Removes license keys and validates
        :param value:
        :return:
        """
        rulesets = set()
        for item in value:
            i = RulesetName(item)  # raises ValueError
            rulesets.add(RulesetName(i.name, i.version, None).to_str())
        return rulesets

    def iter_rulesets(self) -> Generator[RulesetName, None, None]:
        yield from map(RulesetName, self.target_rulesets)

    def celery_schedule(self) -> BaseSchedule:
        return to_celery_schedule(self.schedule)


class ScheduledJobGetModel(BasePaginationModel):
    """
    GET
    """

    tenant_name: str = Field(None)


class ScheduledJobPatchModel(BaseModel):
    schedule: int | str = Field(None)
    enabled: bool = Field(None)
    description: str = Field(None)

    @model_validator(mode='after')
    def at_least_one_given(self) -> Self:
        if (
            not self.schedule
            and self.enabled is None
            and self.description is None
        ):
            raise ValueError('Provide attributes to update')
        return self

    @field_validator('schedule')
    @classmethod
    def validate_schedule(cls, sch: int | str | None) -> int | str | None:
        # TODO: check minimum allowed period
        if isinstance(sch, int) or sch is None:
            return sch
        return sanitize_schedule(sch)

    def celery_schedule(self) -> BaseSchedule | None:
        if self.schedule is not None:
            return to_celery_schedule(self.schedule)


class EventPostModel(BaseModel):
    version: Annotated[
        Literal['1.0.0'],
        WithJsonSchema(
            {
                'type': 'string',
                'title': 'Event type version',
                'default': '1.0.0',
            }
        ),
    ] = '1.0.0'
    vendor: Literal['AWS', 'MAESTRO']
    events: list[dict]


def validate_password(password: str) -> list[str]:
    errors = []
    upper = any(char.isupper() for char in password)
    lower = any(char.islower() for char in password)
    numeric = any(char.isdigit() for char in password)
    symbol = any(not char.isalnum() for char in password)
    if not upper:
        errors.append('must have uppercase characters')
    if not numeric:
        errors.append('must have numeric characters')
    if not lower:
        errors.append('must have lowercase characters')
    if not symbol:
        errors.append('must have at least one symbol')
    if len(password) < 8:
        errors.append('valid min length for password: 8')
    return errors


class UserPasswordResetPostModel(BaseModel):
    password: str
    username: str = Field(None)

    # validators
    @field_validator('password')
    @classmethod
    def _(cls, password: str) -> str:
        if errors := validate_password(password):
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
        StringConstraints(to_upper=True),
    ] = Field(None)
    stage: str = Field(None)


class LicenseManagerConfigSettingPatchModel(BaseModel):
    host: str = Field(None)
    port: int = Field(None)
    protocol: Annotated[
        Literal['HTTP', 'HTTPS', 'http', 'https'],
        StringConstraints(to_upper=True),
    ] = Field(None)
    stage: str = Field(None)

    @model_validator(mode='after')
    def at_least_one_to_update(self) -> Self:
        if not any((self.host, self.port, self.protocol, self.stage)):
            raise ValueError('Provide at least one attribute to update')
        return self


class LicenseManagerClientSettingPostModel(BaseModel):
    key_id: str
    algorithm: Annotated[
        Literal['ECC:p521_DSS_SHA:256'],
        WithJsonSchema(
            {
                'type': 'string',
                'title': 'LM algorithm',
                'default': DEFAULT_LM_PK_ALGORITHM,
            }
        ),
    ] = DEFAULT_LM_PK_ALGORITHM
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
                "'private_key' must be a safe to decode base64-string."
            )
        return self


class LicenseManagerClientSettingPatchModel(BaseModel):
    key_id: str = Field(None)
    algorithm: Annotated[
        Literal['ECC:p521_DSS_SHA:256'],
        WithJsonSchema(
            {
                'type': 'string',
                'title': 'LM algorithm',
                'default': DEFAULT_LM_PK_ALGORITHM,
            }
        ),
    ] = DEFAULT_LM_PK_ALGORITHM
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
                "'private_key' must be a safe to decode base64-string."
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
    /reports/push/chronicle/{job_id}/
    """

    type: JobType = JobType.MANUAL

class ReportPushDojoByJobIdModel(ReportPushByJobIdModel):
    """
    /reports/push/dojo/{job_id}/
    """

    dojo_product: str = Field(
        None,
        description='Defect Dojo product name to which the results will be '
                    'uploaded',
    )
    dojo_engagement: str = Field(
        None,
        description='Defect Dojo engagement name to which the results will be '
                    'uploaded',
    )
    dojo_test: str = Field(
        None,
        description='Defect Dojo test name to which the results will be '
                    'uploaded',
    )

    def dojo_structure(self) -> dict:
        return DojoStructure(
            product=self.dojo_product,
            engagement=self.dojo_engagement,
            test=self.dojo_test,
        ).model_dump(exclude_none=True)


class ReportPushMultipleModel(TimeRangedMixin, BaseModel):
    """
    /reports/push/dojo
    /reports/push/security-hub
    """

    tenant_name: str
    start_iso: datetime | date = Field(None, alias='from')
    end_iso: datetime | date = Field(None, alias='to')
    type: JobType = Field(None)

class ReportPushDojoMultipleModel(ReportPushMultipleModel):
    """
    /reports/push/dojo
    """

    dojo_product: str = Field(
        None,
        description='Defect Dojo product name to which the results will be '
                    'uploaded',
    )
    dojo_engagement: str = Field(
        None,
        description='Defect Dojo engagement name to which the results will be '
                    'uploaded',
    )
    dojo_test: str = Field(
        None,
        description='Defect Dojo test name to which the results will be '
                    'uploaded',
    )

    def dojo_structure(self) -> dict:
        return DojoStructure(
            product=self.dojo_product,
            engagement=self.dojo_engagement,
            test=self.dojo_test
        ).model_dump(exclude_none=True)


class ProjectGetReportModel(BaseModel):
    tenant_display_names: set[
        Annotated[str, StringConstraints(to_lower=True, strip_whitespace=True)]
    ]
    types: set[
        Literal[
            'OVERVIEW', 'RESOURCES', 'COMPLIANCE', 'ATTACK_VECTOR', 'FINOPS'
        ]
    ] = Field(default_factory=set)
    receivers: set[str] = Field(default_factory=set)
    include_linked: bool = False
    # attempt: SkipJsonSchema[int] = 0
    # execution_job_id: SkipJsonSchema[str] = Field(None)

    @property
    def new_types(self) -> tuple[ReportType, ...]:
        old_new = {
            'OVERVIEW': ReportType.PROJECT_OVERVIEW,
            'RESOURCES': ReportType.PROJECT_RESOURCES,
            'COMPLIANCE': ReportType.PROJECT_COMPLIANCE,
            'ATTACK_VECTOR': ReportType.PROJECT_ATTACKS,
            'FINOPS': ReportType.PROJECT_FINOPS,
        }
        if not self.types:
            return tuple(old_new.values())
        res = []
        for t in self.types:
            if t in old_new:
                res.append(old_new[t])
        return tuple(res)


class OperationalGetReportModel(BaseModel):
    tenant_names: set[str]
    types: set[
        Literal[
            'OVERVIEW',
            'RESOURCES',
            'COMPLIANCE',
            'RULE',
            'ATTACK_VECTOR',
            'FINOPS',
            'KUBERNETES',
            'DEPRECATIONS',
        ]
    ] = Field(default_factory=set)
    receivers: set[str] = Field(default_factory=set)
    # attempt: SkipJsonSchema[int] = 0
    # execution_job_id: SkipJsonSchema[str] = Field(None)

    @property
    def new_types(self) -> tuple[ReportType, ...]:
        """
        Converts to new types
        """
        old_new = {
            'OVERVIEW': ReportType.OPERATIONAL_OVERVIEW,
            'RESOURCES': ReportType.OPERATIONAL_RESOURCES,
            'COMPLIANCE': ReportType.OPERATIONAL_COMPLIANCE,
            'RULE': ReportType.OPERATIONAL_RULES,
            'FINOPS': ReportType.OPERATIONAL_FINOPS,
            'ATTACK_VECTOR': ReportType.OPERATIONAL_ATTACKS,
            'KUBERNETES': ReportType.OPERATIONAL_KUBERNETES,
            'DEPRECATIONS': ReportType.OPERATIONAL_DEPRECATION,
        }
        if not self.types:
            return tuple(old_new.values())
        res = []
        for t in self.types:
            if t in old_new:
                res.append(old_new[t])
        return tuple(res)


class DepartmentGetReportModel(BaseModel):
    types: set[
        Literal[
            'TOP_RESOURCES_BY_CLOUD',
            'TOP_TENANTS_RESOURCES',
            'TOP_TENANTS_COMPLIANCE',
            'TOP_COMPLIANCE_BY_CLOUD',
            'TOP_TENANTS_ATTACKS',
            'TOP_ATTACK_BY_CLOUD',
        ]
    ] = Field(default_factory=set)
    # attempt: SkipJsonSchema[int] = 0
    # execution_job_id: SkipJsonSchema[str] = Field(None)

    @property
    def new_types(self) -> tuple[ReportType, ...]:
        """
        Convert to new types
        """
        old_new = {
            'TOP_RESOURCES_BY_CLOUD': ReportType.DEPARTMENT_TOP_RESOURCES_BY_CLOUD,
            'TOP_TENANTS_RESOURCES': ReportType.DEPARTMENT_TOP_TENANTS_RESOURCES,
            'TOP_TENANTS_COMPLIANCE': ReportType.DEPARTMENT_TOP_TENANTS_COMPLIANCE,
            'TOP_COMPLIANCE_BY_CLOUD': ReportType.DEPARTMENT_TOP_COMPLIANCE_BY_CLOUD,
            'TOP_TENANTS_ATTACKS': ReportType.DEPARTMENT_TOP_TENANTS_ATTACKS,
            'TOP_ATTACK_BY_CLOUD': ReportType.DEPARTMENT_TOP_ATTACK_BY_CLOUD,
        }
        if not self.types:
            return tuple(old_new.values())
        res = []
        for t in self.types:
            if t in old_new:
                res.append(old_new[t])
        return tuple(res)


class CLevelGetReportModel(BaseModel):
    receivers: set[str] = Field(default_factory=set)
    types: set[Literal['OVERVIEW', 'COMPLIANCE', 'ATTACK_VECTOR']] = Field(
        default_factory=set
    )
    # attempt: SkipJsonSchema[int] = 0
    # execution_job_id: SkipJsonSchema[str] = Field(None)

    @property
    def new_types(self) -> tuple[ReportType, ...]:
        """
        Converts to new types
        """
        old_new = {
            'OVERVIEW': ReportType.C_LEVEL_OVERVIEW,
            'COMPLIANCE': ReportType.C_LEVEL_COMPLIANCE,
            'ATTACK_VECTOR': ReportType.C_LEVEL_ATTACKS,
        }
        if not self.types:
            return tuple(old_new.values())
        res = []
        for t in self.types:
            if t in old_new:
                res.append(old_new[t])
        return tuple(res)


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
        str, StringConstraints(to_lower=True, strip_whitespace=True)
    ] = Field(None)
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
            raise ValueError(
                'If search_by_all, an least one query to search '
                'by must be provided'
            )
        if (
            self.obfuscated
            and self.format == ReportFormat.JSON
            and not self.href
        ):
            raise ValueError(
                'Obfuscation is currently not supported for raw json report'
            )
        return self

    # all the other attributes to search by can be provided as well.
    # They are not declared


class PlatformK8sResourcesReportGetModel(BaseModel):
    model_config = ConfigDict(extra='allow')

    resource_type: Annotated[
        str, StringConstraints(to_lower=True, strip_whitespace=True)
    ] = Field(None)
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
            raise ValueError(
                'If search_by_all, an least one query to search '
                'by must be provided'
            )
        if (
            self.obfuscated
            and self.format == ReportFormat.JSON
            and not self.href
        ):
            raise ValueError(
                'Obfuscation is currently not supported for raw json report'
            )
        return self

    # all the other attributes to search by can be provided as well.
    # They are not declared


class ResourceReportJobsGetModel(TimeRangedMixin, BaseModel):
    model_config = ConfigDict(extra='allow')

    start_iso: datetime | date = Field(None, alias='from')
    end_iso: datetime | date = Field(None, alias='to')
    job_type: JobType = JobType.MANUAL

    resource_type: Annotated[
        str, StringConstraints(to_lower=True, strip_whitespace=True)
    ] = None
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
            raise ValueError(
                'If search_by_all, an least one query to search '
                'by must be provided'
            )
        return self


class ResourceReportJobGetModel(BaseModel):
    model_config = ConfigDict(extra='allow')

    job_type: JobType = JobType.MANUAL

    resource_type: Annotated[
        str, StringConstraints(to_lower=True, strip_whitespace=True)
    ] = Field(None)
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
            raise ValueError(
                'If search_by_all, an least one query to search '
                'by must be provided'
            )
        if self.obfuscated and not self.href:
            raise ValueError(
                'Currently obfuscation is supported only if href is true'
            )
        return self


class PlatformK8SPostModel(BaseModel):
    tenant_name: Annotated[
        str, StringConstraints(to_upper=True, strip_whitespace=True)
    ]
    name: str
    region: AllRegions = Field(None)
    type: PlatformType
    description: str

    endpoint: HttpUrl = Field(None)
    certificate_authority: str = Field(None)  # base64 encoded
    token: str = Field(None)

    @model_validator(mode='after')
    def root(self) -> Self:
        if self.type != PlatformType.SELF_MANAGED and not self.region:
            raise ValueError('region is required if platform is cloud managed')
        if self.type != PlatformType.EKS and not self.endpoint:
            raise ValueError('endpoint must be specified if type is not EKS')
        return self


class PlatformK8sQueryModel(BaseModel):
    tenant_name: str = Field(None)


class K8sJobPostModel(BaseModel):
    """
    K8s platform job
    """

    platform_id: str
    target_rulesets: set[str] = Field(default_factory=set, alias='rulesets')
    token: str = Field(None)  # temp jwt token

    timeout_minutes: float = Field(
        None,
        description='Job timeout in minutes. This timeout is soft '
        'meaning that when the desired number of minutes have '
        'passed job termination will be triggered',
    )
    license_key: str = Field(
        None,
        description='License to exhaust for this job. Will be resolved '
        'automatically unless an ambiguous occurs',
    )

    dojo_product: str = Field(
        None,
        description='Defect Dojo product name to which the results will be '
                    'uploaded',
    )
    dojo_engagement: str = Field(
        None,
        description='Defect Dojo engagement name to which the results will be '
                    'uploaded',
    )
    dojo_test: str = Field(
        None,
        description='Defect Dojo test name to which the results will be '
                    'uploaded',
    )

    @field_validator('target_rulesets', mode='after')
    @classmethod
    def validate_rulesets(cls, value: set[str]) -> set[str]:
        """
        Removes license keys and validates
        :param value:
        :return:
        """
        rulesets = set()
        for item in value:
            i = RulesetName(item)  # raises ValueError
            rulesets.add(RulesetName(i.name, i.version, None).to_str())
        return rulesets

    def iter_rulesets(self) -> Generator[RulesetName, None, None]:
        yield from map(RulesetName, self.target_rulesets)

    def dojo_structure(self) -> dict:
        return DojoStructure(
            product=self.dojo_product,
            engagement=self.dojo_engagement,
            test=self.dojo_test,
        ).model_dump(exclude_none=True)


class ReportStatusGetModel(BaseModel):
    """
    GET
    """

    job_id: str
    complete: bool = False


class ServiceOperationStatusGetModel(TimeRangedMixin, BaseModel):
    type: ServiceOperationType
    start_iso: datetime | date = Field(None, alias='from')
    end_iso: datetime | date = Field(None, alias='to')

    @classmethod
    def max_range(cls) -> timedelta:
        return timedelta(days=365)

    @classmethod
    def skip_validation_if_no_input(cls) -> bool:
        return True


class LicensePostModel(BaseModel):
    tenant_license_key: str = Field(alias='license_key')


class LicenseActivationPutModel(BaseModel):
    tenant_names: set[str] = Field(default_factory=set)

    all_tenants: bool = False
    clouds: set[Literal['AWS', 'AZURE', 'GOOGLE', 'KUBERNETES']] = Field(
        default_factory=set
    )
    exclude_tenants: set[str] = Field(default_factory=set)

    @model_validator(mode='after')
    def _(self) -> Self:
        if self.tenant_names and any(
            (self.all_tenants, self.clouds, self.exclude_tenants)
        ):
            raise ValueError(
                'do not provide all_tenants, clouds or '
                'exclude_tenants if specific '
                'tenant names are provided'
            )
        if not self.all_tenants and not self.tenant_names:
            raise ValueError(
                'either all_tenants or specific tenant names must be given'
            )
        if (self.clouds or self.exclude_tenants) and not self.all_tenants:
            raise ValueError(
                'set all tenants to true if you provide clouds or excluded'
            )
        return self


class LicenseActivationPatchModel(BaseModel):
    add_tenants: set[str] = Field(default_factory=set)
    remove_tenants: set[str] = Field(default_factory=set)

    @model_validator(mode='after')
    def _(self) -> Self:
        if not self.add_tenants and not self.remove_tenants:
            raise ValueError('provide either add_tenants or remove_tenants')
        return self


class LicenseSyncModel(BaseModel):
    overwrite_rulesets: bool = Field(
        default=False,
        description='Overwrite existing rulesets in S3 even if they already exist',
    )


class DefectDojoPostModel(BaseModel):
    url: AnyUrl  # = Field(examples=['http://127.0.0.1:8080/api/v2'])  # api gw models does not support examples
    api_key: str

    description: str


class ChroniclePostModel(BaseModel):
    endpoint: HttpUrl  # https://malachiteingestion-pa.googleapis.com/v2
    description: str
    credentials_application_id: str  # application with google creds
    instance_customer_id: str

    @property
    def baseurl(self) -> str:
        return self.endpoint.scheme + '://' + self.endpoint.host


class ChronicleActivationPutModel(BaseModel):
    tenant_names: set[str] = Field(default_factory=set)

    all_tenants: bool = False
    clouds: set[Literal['AWS', 'AZURE', 'GOOGLE']] = Field(default_factory=set)
    exclude_tenants: set[str] = Field(default_factory=set)

    send_after_job: bool = Field(
        False,
        description='Whether to send the results to dojo after each scan',
    )
    convert_to: ChronicleConverterType = Field(
        ChronicleConverterType.EVENTS,
        description='How to convert Rule Engine data '
        'before sending to Chronicle',
    )

    @model_validator(mode='after')
    def _(self) -> Self:
        if self.tenant_names and any(
            (self.all_tenants, self.clouds, self.exclude_tenants)
        ):
            raise ValueError(
                'do not provide all_tenants, clouds or '
                'exclude_tenants if specific '
                'tenant names are provided'
            )
        if not self.all_tenants and not self.tenant_names:
            raise ValueError(
                'either all_tenants or specific tenant names must be given'
            )
        if (self.clouds or self.exclude_tenants) and not self.all_tenants:
            raise ValueError(
                'set all tenants to true if you provide clouds or excluded'
            )
        return self


class DefectDojoQueryModel(BaseModel):
    pass


class DefectDojoActivationPutModel(BaseModel):
    tenant_names: set[str] = Field(default_factory=set)

    all_tenants: bool = False
    clouds: set[Literal['AWS', 'AZURE', 'GOOGLE']] = Field(default_factory=set)
    exclude_tenants: set[str] = Field(default_factory=set)

    scan_type: Literal['Generic Findings Import', 'Cloud Custodian Scan'] = (
        Field('Generic Findings Import', description='Defect dojo scan type')
    )
    product_type: str = Field(
        'Rule Engine', description='Defect dojo product type name'
    )
    product: str = Field(
        '{tenant_name}', description='Defect dojo product name'
    )
    engagement: str = Field(
        'Rule-Engine Main', description='Defect dojo engagement name'
    )
    test: str = Field('{job_id}', description='Defect dojo test')
    send_after_job: bool = Field(
        False,
        description='Whether to send the results to dojo after each scan',
    )
    attachment: Literal['json', 'xlsx', 'csv'] = Field(None)

    @model_validator(mode='after')
    def _(self) -> Self:
        if self.tenant_names and any(
            (self.all_tenants, self.clouds, self.exclude_tenants)
        ):
            raise ValueError(
                'do not provide all_tenants, clouds or '
                'exclude_tenants if specific '
                'tenant names are provided'
            )
        if not self.all_tenants and not self.tenant_names:
            raise ValueError(
                'either all_tenants or specific tenant names must be given'
            )
        if (self.clouds or self.exclude_tenants) and not self.all_tenants:
            raise ValueError(
                'set all tenants to true if you provide clouds or excluded'
            )
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
        if self.tenant_names and any(
            (self.all_tenants, self.clouds, self.exclude_tenants)
        ):
            raise ValueError(
                'do not provide all_tenants, clouds or '
                'exclude_tenants if specific '
                'tenant names are provided'
            )
        if not self.all_tenants and not self.tenant_names:
            raise ValueError(
                'either all_tenants or specific tenant names must be given'
            )
        if (self.clouds or self.exclude_tenants) and not self.all_tenants:
            raise ValueError(
                'set all tenants to true if you provide clouds or excluded'
            )
        if not self.auto_resolve_access and not self.url:
            raise ValueError(
                'url must be given in case auto_resolve_access is not True'
            )
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
            raise ValueError(
                'do not provide all_tenants  or '
                'exclude_tenants if specific '
                'tenant names are provided'
            )
        if not self.all_tenants and not self.tenant_names:
            raise ValueError(
                'either all_tenants or specific tenant names must be given'
            )
        if self.exclude_tenants and not self.all_tenants:
            raise ValueError(
                'set all tenants to true if you provide  excluded'
            )
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
        if errors := validate_password(password):
            raise ValueError(', '.join(errors))
        return password


class UserPostModel(BaseModel):
    username: str
    role_name: str
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
        if errors := validate_password(password):
            raise ValueError(', '.join(errors))
        return password


class UserResetPasswordModel(BaseModel):
    new_password: str

    @field_validator('new_password')
    @classmethod
    def _(cls, password: str) -> str:
        if errors := validate_password(password):
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
        if errors := validate_password(password):
            raise ValueError(', '.join(errors))
        return password
