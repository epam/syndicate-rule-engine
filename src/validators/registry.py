from http import HTTPStatus
from typing import Generator

from helpers.constants import CustodianEndpoint, HTTPMethod, Permission, LambdaName
from services.openapi_spec_generator import EndpointInfo
from validators.swagger_request_models import (
    BaseModel,
    BasePaginationModel,
    BatchResultsQueryModel,
    RuleSourcesListModel,
    RulesetReleasePostModel,
    CLevelGetReportModel,
    ChronicleActivationPutModel,
    ChroniclePostModel,
    CredentialsBindModel,
    CredentialsQueryModel,
    CustomerExcludedRulesPutModel,
    CustomerGetModel,
    DefectDojoActivationPutModel,
    DefectDojoPostModel,
    DefectDojoQueryModel,
    DepartmentGetReportModel,
    EventPostModel,
    HealthCheckQueryModel,
    JobComplianceReportGetModel,
    JobDetailsReportGetModel,
    JobDigestReportGetModel,
    JobErrorReportGetModel,
    JobFindingsReportGetModel,
    JobGetModel,
    JobPostModel,
    JobRuleReportGetModel,
    K8sJobPostModel,
    LicenseActivationPatchModel,
    LicenseActivationPutModel,
    LicenseManagerClientSettingDeleteModel,
    LicenseManagerClientSettingPostModel,
    LicenseManagerConfigSettingPostModel,
    LicensePostModel,
    MailSettingGetModel,
    MailSettingPostModel,
    MetricsStatusGetModel,
    MultipleTenantsGetModel,
    OperationalGetReportModel,
    PlatformK8SPostModel,
    PlatformK8sQueryModel,
    PlatformK8sResourcesReportGetModel,
    PolicyPatchModel,
    PolicyPostModel,
    ProjectGetReportModel,
    RabbitMQDeleteModel,
    RabbitMQGetModel,
    RabbitMQPostModel,
    RawReportGetModel,
    RefreshPostModel,
    ReportPushByJobIdModel,
    ReportPushMultipleModel,
    ReportStatusGetModel,
    ReportsSendingSettingPostModel,
    ResourceReportJobGetModel,
    ResourceReportJobsGetModel,
    ResourcesReportGetModel,
    RolePatchModel,
    RolePostModel,
    RuleDeleteModel,
    RuleGetModel,
    RuleSourceDeleteModel,
    RuleSourcePatchModel,
    RuleSourcePostModel,
    RuleUpdateMetaPostModel,
    RulesetDeleteModel,
    RulesetGetModel,
    RulesetPatchModel,
    RulesetPostModel,
    ScheduledJobGetModel,
    ScheduledJobPatchModel,
    ScheduledJobPostModel,
    SelfIntegrationPatchModel,
    SelfIntegrationPutModel,
    SignInPostModel,
    SignUpModel,
    TenantComplianceReportGetModel,
    TenantExcludedRulesPutModel,
    TenantGetActiveLicensesModel,
    TenantJobsDetailsReportGetModel,
    TenantJobsDigestsReportGetModel,
    TenantJobsFindingsReportGetModel,
    TenantRuleReportGetModel,
    UserPatchModel,
    UserPostModel,
    UserResetPasswordModel,
    ResourcesGetModel,
    ResourcesArnGetModel
)
from validators.swagger_response_models import (
    CredentialsActivationModel,
    EntityResourcesReportModel,
    EntityRulesReportModel,
    ErrorsModel,
    ErrorsReportModel,
    EventModel,
    JobResourcesReportModel,
    MessageModel,
    MultipleBatchResultsModel,
    MultipleChronicleModel,
    MultipleCredentialsModel,
    MultipleCustomersModel,
    MultipleDefectDojoModel,
    MultipleDefectDojoPushResult,
    SingleChroniclePushResult,
    MultipleHealthChecksModel,
    MultipleJobReportModel,
    MultipleJobsModel,
    MultipleK8SPlatformsModel,
    MultipleLicensesModel,
    MultipleMetricsStatusesModel,
    MultiplePoliciesModel,
    MultipleReportStatusModel,
    MultipleRoleModel,
    MultipleRuleMetaUpdateModel,
    MultipleRuleSourceModel,
    MultipleRulesModel,
    MultipleRulesetsModel,
    MultipleScheduledJobsModel,
    MultipleTenantsModel,
    MultipleUsersModel,
    MultipleResourcesModel,
    RawReportModel,
    RulesReportModel,
    SignInModel,
    SingleBatchResultModel,
    SingleChronicleActivationModel,
    SingleChronicleModel,
    SingleCredentialsModel,
    SingleCustomerExcludedRules,
    SingleDefeDojoModel,
    SingleDefectDojoActivation,
    SingleDefectDojoPushResult,
    SingleEntityReportModel,
    SingleHealthCheckModel,
    SingleJobModel,
    SingleJobReportModel,
    SingleK8SPlatformModel,
    SingleLMClientModel,
    SingleLMConfigModel,
    SingleLicenseActivationModel,
    SingleLicenseModel,
    SingleMailSettingModel,
    SinglePolicyModel,
    SingleRabbitMQModel,
    SingleRoleModel,
    SingleRuleSourceModel,
    SingleRulesetModel,
    SingleScheduledJobModel,
    SingleSelfIntegration,
    SingleTenantExcludedRules,
    SingleTenantsModel,
    SingleUserModel,
    SingleResourceModel
)


data: tuple[EndpointInfo, ...] = (
    # auth
    EndpointInfo(
        path=CustodianEndpoint.SIGNUP,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.API_HANDLER,
        request_model=SignUpModel,
        responses=[(HTTPStatus.CREATED, MessageModel, None),
                   (HTTPStatus.CONFLICT, MessageModel, None)],
        auth=False,
        description='Registers a new API user, creates a new customer '
                    'and admin role for that user'
    ),
    EndpointInfo(
        path=CustodianEndpoint.SIGNIN,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.API_HANDLER,
        request_model=SignInPostModel,
        responses=[(HTTPStatus.OK, SignInModel, None)],
        auth=False,
        description='Allows log in and receive access and refresh tokens'
    ),
    EndpointInfo(
        path=CustodianEndpoint.REFRESH,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.API_HANDLER,
        request_model=RefreshPostModel,
        responses=[(HTTPStatus.OK, SignInModel, None)],
        auth=False,
        description='Allows to refresh the access token'
    ),
    # event
    EndpointInfo(
        path=CustodianEndpoint.EVENT,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.API_HANDLER,
        request_model=EventPostModel,
        responses=[(HTTPStatus.ACCEPTED, EventModel, None)],
        permission=Permission.EVENT_POST,
        description='Receives event-driven events'
    ),

    # health
    EndpointInfo(
        path=CustodianEndpoint.HEALTH,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.API_HANDLER,
        request_model=HealthCheckQueryModel,
        responses=[(HTTPStatus.OK, MultipleHealthChecksModel, None)],
        description='Performs all available health checks',
        auth=False
    ),
    EndpointInfo(
        path=CustodianEndpoint.HEALTH_ID,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleHealthCheckModel, None)],
        description='Performs a specific health check by its id',
        auth=False
    ),

    EndpointInfo(
        path=CustodianEndpoint.JOBS_K8S,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.API_HANDLER,
        request_model=K8sJobPostModel,
        responses=[(HTTPStatus.ACCEPTED, SingleJobModel, None)],
        permission=Permission.JOB_POST_K8S,
        description='Allows to submit a licensed job for a K8S cluster'
    ),
    EndpointInfo(
        path=CustodianEndpoint.JOBS,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.API_HANDLER,
        request_model=JobGetModel,
        responses=[(HTTPStatus.OK, MultipleJobsModel, None)],
        permission=Permission.JOB_QUERY,
        description='Allows to query jobs'
    ),
    EndpointInfo(
        path=CustodianEndpoint.JOBS,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.API_HANDLER,
        request_model=JobPostModel,
        responses=[(HTTPStatus.ACCEPTED, SingleJobModel, None)],
        permission=Permission.JOB_POST_LICENSED,
        description='Allows to submit a licensed job for a cloud'
    ),
    EndpointInfo(
        path=CustodianEndpoint.JOBS_JOB,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleJobModel, None)],
        permission=Permission.JOB_GET,
        description='Allows to get a specific job by id'
    ),
    EndpointInfo(
        path=CustodianEndpoint.JOBS_JOB,
        method=HTTPMethod.DELETE,
        lambda_name=LambdaName.API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.ACCEPTED, MessageModel, None)],
        permission=Permission.JOB_TERMINATE,
        description='Allows to terminate a job that is running'
    ),

    # scheduled jobs
    EndpointInfo(
        path=CustodianEndpoint.SCHEDULED_JOB,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.API_HANDLER,
        request_model=ScheduledJobGetModel,
        responses=[(HTTPStatus.OK, MultipleScheduledJobsModel, None)],
        permission=Permission.SCHEDULED_JOB_QUERY,
        description='Allows to query registered scheduled jobs'
    ),
    EndpointInfo(
        path=CustodianEndpoint.SCHEDULED_JOB,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.API_HANDLER,
        request_model=ScheduledJobPostModel,
        responses=[(HTTPStatus.CREATED, SingleScheduledJobModel, None)],
        permission=Permission.SCHEDULED_JOB_CREATE,
        description='Allows to register a scheduled job'
    ),
    EndpointInfo(
        path=CustodianEndpoint.SCHEDULED_JOB_NAME,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleScheduledJobModel, None)],
        permission=Permission.SCHEDULED_JOB_GET,
        description='Allows to get a registered scheduled job by its name'
    ),
    EndpointInfo(
        path=CustodianEndpoint.SCHEDULED_JOB_NAME,
        method=HTTPMethod.PATCH,
        lambda_name=LambdaName.API_HANDLER,
        request_model=ScheduledJobPatchModel,
        responses=[(HTTPStatus.OK, SingleScheduledJobModel, None)],
        permission=Permission.SCHEDULED_JOB_UPDATE,
        description='Allows to update a registered scheduled job by name'
    ),
    EndpointInfo(
        path=CustodianEndpoint.SCHEDULED_JOB_NAME,
        method=HTTPMethod.DELETE,
        lambda_name=LambdaName.API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.SCHEDULED_JOB_DELETE,
        description='Allows to deregister a scheduled job'
    ),

    # resources
    EndpointInfo(
        path=CustodianEndpoint.RESOURCES,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=ResourcesGetModel,
        responses=[(HTTPStatus.OK, MultipleResourcesModel, None)],
        permission=Permission.RESOURCES_GET,
        description='Allows to get resources information'
    ),
    EndpointInfo(
        path=CustodianEndpoint.RESOURCES_ARN,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=ResourcesArnGetModel,
        responses=[(HTTPStatus.OK, SingleResourceModel, None)],
        permission=Permission.RESOURCES_GET,
        description='Allows to get a resource by its ARN'
    ),

    # customers
    EndpointInfo(
        path=CustodianEndpoint.CUSTOMERS,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=CustomerGetModel,
        responses=[(HTTPStatus.OK, MultipleCustomersModel, None)],
        permission=Permission.CUSTOMER_DESCRIBE,
        description='Allows to describe customers'
    ),
    EndpointInfo(
        path=CustodianEndpoint.CUSTOMERS_RABBITMQ,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=RabbitMQGetModel,
        responses=[(HTTPStatus.OK, SingleRabbitMQModel, None)],
        permission=Permission.RABBITMQ_DESCRIBE,
        description='Allows to describe RabbitMQ configuration'
    ),
    EndpointInfo(
        path=CustodianEndpoint.CUSTOMERS_RABBITMQ,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=RabbitMQPostModel,
        responses=[(HTTPStatus.OK, SingleRabbitMQModel, None)],
        permission=Permission.RABBITMQ_CREATE,
        description='Allows to create a RabbitMQ configuration for customer'
    ),
    EndpointInfo(
        path=CustodianEndpoint.CUSTOMERS_RABBITMQ,
        method=HTTPMethod.DELETE,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=RabbitMQDeleteModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.RABBITMQ_DELETE,
        description='Allows to remove a RabbitMQ configuration'
    ),
    EndpointInfo(
        path=CustodianEndpoint.CUSTOMERS_EXCLUDED_RULES,
        method=HTTPMethod.PUT,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=CustomerExcludedRulesPutModel,
        responses=[(HTTPStatus.OK, SingleCustomerExcludedRules, None)],
        permission=Permission.CUSTOMER_SET_EXCLUDED_RULES,
        description='Allows to exclude rules for customer'
    ),
    EndpointInfo(
        path=CustodianEndpoint.CUSTOMERS_EXCLUDED_RULES,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleCustomerExcludedRules, None)],
        permission=Permission.CUSTOMER_GET_EXCLUDED_RULES,
        description='Allows to get customer`s excluded rules'
    ),

    # tenants
    EndpointInfo(
        path=CustodianEndpoint.TENANTS,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=MultipleTenantsGetModel,
        responses=[(HTTPStatus.OK, MultipleTenantsModel, None)],
        permission=Permission.TENANT_QUERY,
        description='Allows to query tenants'
    ),
    EndpointInfo(
        path=CustodianEndpoint.TENANTS_TENANT_NAME,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleTenantsModel, None)],
        permission=Permission.TENANT_GET,
        description='Allows to get a tenant by name'
    ),
    EndpointInfo(
        path=CustodianEndpoint.TENANTS_TENANT_NAME_ACTIVE_LICENSES,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=TenantGetActiveLicensesModel,
        responses=[(HTTPStatus.OK, MultipleLicensesModel, None)],
        permission=Permission.TENANT_GET_ACTIVE_LICENSES,
        description='Allows to get licenses that are activated for a specific tenant'
    ),
    EndpointInfo(
        path=CustodianEndpoint.TENANTS_TENANT_NAME_EXCLUDED_RULES,
        method=HTTPMethod.PUT,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=TenantExcludedRulesPutModel,
        responses=[(HTTPStatus.OK, SingleTenantExcludedRules, None)],
        permission=Permission.TENANT_SET_EXCLUDED_RULES,
        description='Allows to exclude rules for tenant'
    ),
    EndpointInfo(
        path=CustodianEndpoint.TENANTS_TENANT_NAME_EXCLUDED_RULES,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleTenantExcludedRules, None)],
        permission=Permission.TENANT_GET_EXCLUDED_RULES,
        description='Allows to get rules that are excluded for tenant'
    ),

    # credentials
    EndpointInfo(
        path=CustodianEndpoint.CREDENTIALS,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=CredentialsQueryModel,
        responses=[(HTTPStatus.OK, MultipleCredentialsModel, None)],
        permission=Permission.CREDENTIALS_DESCRIBE,
        description='Allows to get credentials configurations within a customer'
    ),
    EndpointInfo(
        path=CustodianEndpoint.CREDENTIALS_ID,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleCredentialsModel, None)],
        permission=Permission.CREDENTIALS_DESCRIBE,
        description='Allows to get a credentials configuration by id'
    ),
    EndpointInfo(
        path=CustodianEndpoint.CREDENTIALS_ID_BINDING,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, CredentialsActivationModel, None)],
        permission=Permission.CREDENTIALS_GET_BINDING,
        description='Allows to show tenants that are linked to specific credentials configuration'
    ),
    EndpointInfo(
        path=CustodianEndpoint.CREDENTIALS_ID_BINDING,
        method=HTTPMethod.PUT,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=CredentialsBindModel,
        responses=[(HTTPStatus.OK, CredentialsActivationModel, None)],
        permission=Permission.CREDENTIALS_BIND,
        description='Allows to link tenants to a specific credentials configuration'
    ),
    EndpointInfo(
        path=CustodianEndpoint.CREDENTIALS_ID_BINDING,
        method=HTTPMethod.DELETE,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.CREDENTIALS_UNBIND,
        description='Allows to unlink a specific credentials configuration from all tenants'
    ),

    # rules
    EndpointInfo(
        path=CustodianEndpoint.RULES,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=RuleGetModel,
        responses=[(HTTPStatus.OK, MultipleRulesModel, None)],
        permission=Permission.RULE_DESCRIBE,
        description='Allows to describe locally available rules'
    ),
    EndpointInfo(
        path=CustodianEndpoint.RULES,
        method=HTTPMethod.DELETE,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=RuleDeleteModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.RULE_DELETE,
        description='Allows to delete local rules content'
    ),
    EndpointInfo(
        path=CustodianEndpoint.RULE_META_UPDATER,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=RuleUpdateMetaPostModel,
        responses=[(HTTPStatus.ACCEPTED, MultipleRuleMetaUpdateModel, None)],
        permission=Permission.RULE_UPDATE_META,
        description='Allows to submit a job that will pull latest rules content'
    ),

    # metrics
    EndpointInfo(
        path=CustodianEndpoint.METRICS_UPDATE,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.ACCEPTED, MessageModel, None)],
        permission=Permission.METRICS_UPDATE,
        description='Allows to submit a job that will update metrics'
    ),
    EndpointInfo(
        path=CustodianEndpoint.METRICS_STATUS,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.API_HANDLER,
        request_model=MetricsStatusGetModel,
        responses=[(HTTPStatus.OK, MultipleMetricsStatusesModel, None)],
        permission=Permission.METRICS_STATUS,
        description='Allows to get latest metrics update status'
    ),

    # rulesets
    EndpointInfo(
        path=CustodianEndpoint.RULESETS,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=RulesetGetModel,
        responses=[(HTTPStatus.OK, MultipleRulesetsModel, None)],
        permission=Permission.RULESET_DESCRIBE,
        description='Allows to query available rulesets'
    ),
    EndpointInfo(
        path=CustodianEndpoint.RULESETS,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=RulesetPostModel,
        responses=[(HTTPStatus.CREATED, SingleRulesetModel, None)],
        permission=Permission.RULESET_CREATE,
        description='Allows to create a local ruleset from local rules'
    ),
    EndpointInfo(
        path=CustodianEndpoint.RULESETS,
        method=HTTPMethod.PATCH,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=RulesetPatchModel,
        responses=[(HTTPStatus.OK, SingleRulesetModel, None)],
        permission=Permission.RULESET_UPDATE,
        description='Allows to update a local ruleset'
    ),
    EndpointInfo(
        path=CustodianEndpoint.RULESETS,
        method=HTTPMethod.DELETE,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=RulesetDeleteModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.RULESET_DELETE,
        description='Allows to delete a local ruleset'
    ),
    EndpointInfo(
        path=CustodianEndpoint.RULESETS_RELEASE,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=RulesetReleasePostModel,
        responses=[(HTTPStatus.OK, None, None)],
        permission=Permission.RULESET_RELEASE,
        description='Allows to release a ruleset to the license manager'
    ),

    # rulesources
    EndpointInfo(
        path=CustodianEndpoint.RULE_SOURCES,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=RuleSourcesListModel,
        responses=[(HTTPStatus.OK, MultipleRuleSourceModel, None)],
        permission=Permission.RULE_SOURCE_DESCRIBE,
        description='Allows to list all locally added rule sources'
    ),
    EndpointInfo(
        path=CustodianEndpoint.RULE_SOURCES,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=RuleSourcePostModel,
        responses=[(HTTPStatus.CREATED, SingleRuleSourceModel, None)],
        permission=Permission.RULE_SOURCE_CREATE,
        description='Allows to add a rule-source locally'
    ),
    EndpointInfo(
        path=CustodianEndpoint.RULE_SOURCES_ID,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleRuleSourceModel, None)],
        permission=Permission.RULE_SOURCE_DESCRIBE,
        description='Allows to get a single rule source item'
    ),
    EndpointInfo(
        path=CustodianEndpoint.RULE_SOURCES_ID,
        method=HTTPMethod.PATCH,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=RuleSourcePatchModel,
        responses=[(HTTPStatus.OK, SingleRuleSourceModel, None)],
        permission=Permission.RULE_SOURCE_UPDATE,
        description='Allows to update a local rule-source'
    ),
    EndpointInfo(
        path=CustodianEndpoint.RULE_SOURCES_ID,
        method=HTTPMethod.DELETE,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=RuleSourceDeleteModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.RULE_SOURCE_DELETE,
        description='Allows to delete a local rule-source'
    ),
    EndpointInfo(
        path=CustodianEndpoint.RULE_SOURCES_ID_SYNC,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.ACCEPTED, None, None)],
        permission=Permission.RULE_SOURCE_SYNC,
        description='Allows to pull latest meta for rule source'
    ),

    # policies
    EndpointInfo(
        path=CustodianEndpoint.POLICIES,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BasePaginationModel,
        responses=[(HTTPStatus.OK, MultiplePoliciesModel, None)],
        permission=Permission.POLICY_DESCRIBE,
        description='Allows to list rbac policies'
    ),
    EndpointInfo(
        path=CustodianEndpoint.POLICIES_NAME,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SinglePolicyModel, None)],
        permission=Permission.POLICY_DESCRIBE,
        description='Allows to get a policy by name'
    ),
    EndpointInfo(
        path=CustodianEndpoint.POLICIES,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=PolicyPostModel,
        responses=[(HTTPStatus.CREATED, SinglePolicyModel, None)],
        permission=Permission.POLICY_CREATE,
        description='Allows to create a policy'
    ),
    EndpointInfo(
        path=CustodianEndpoint.POLICIES_NAME,
        method=HTTPMethod.PATCH,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=PolicyPatchModel,
        responses=[(HTTPStatus.OK, SinglePolicyModel, None)],
        permission=Permission.POLICY_UPDATE,
        description='Allows to update a policy name'
    ),
    EndpointInfo(
        path=CustodianEndpoint.POLICIES_NAME,
        method=HTTPMethod.DELETE,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.POLICY_DELETE,
        description='Allows to delete a policy by name'
    ),

    # roles
    EndpointInfo(
        path=CustodianEndpoint.ROLES,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BasePaginationModel,
        responses=[(HTTPStatus.OK, MultipleRoleModel, None)],
        permission=Permission.ROLE_DESCRIBE,
        description='Allows to list rbac roles'
    ),
    EndpointInfo(
        path=CustodianEndpoint.ROLES_NAME,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleRoleModel, None)],
        permission=Permission.ROLE_DESCRIBE,
        description='Allows to get a role by name'
    ),
    EndpointInfo(
        path=CustodianEndpoint.ROLES,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=RolePostModel,
        responses=[(HTTPStatus.CREATED, SingleRoleModel, None)],
        permission=Permission.ROLE_CREATE,
        description='Allows to create a role'
    ),
    EndpointInfo(
        path=CustodianEndpoint.ROLES_NAME,
        method=HTTPMethod.PATCH,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=RolePatchModel,
        responses=[(HTTPStatus.OK, SingleRoleModel, None)],
        permission=Permission.ROLE_UPDATE,
        description='Allows to update a role by name'
    ),
    EndpointInfo(
        path=CustodianEndpoint.ROLES_NAME,
        method=HTTPMethod.DELETE,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.ROLE_DELETE,
        description='Allows to delete a role by name'
    ),

    # licenses
    EndpointInfo(
        path=CustodianEndpoint.LICENSES,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=LicensePostModel,
        responses=[(HTTPStatus.ACCEPTED, None, None)],
        permission=Permission.LICENSE_ADD,
        description='Allows to add a license from LM by tenant license key'
    ),
    EndpointInfo(
        path=CustodianEndpoint.LICENSES,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, MultipleLicensesModel, None)],
        permission=Permission.LICENSE_QUERY,
        description='Allows to list locally added licenses'
    ),
    EndpointInfo(
        path=CustodianEndpoint.LICENSES_LICENSE_KEY,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleLicenseModel, None)],
        permission=Permission.LICENSE_GET,
        description='Allows to describe a specific license by license key'
    ),
    EndpointInfo(
        path=CustodianEndpoint.LICENSES_LICENSE_KEY,
        method=HTTPMethod.DELETE,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.LICENSE_DELETE,
        description='Allows to delete a specific license'
    ),
    EndpointInfo(
        path=CustodianEndpoint.LICENSES_LICENSE_KEY_SYNC,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.ACCEPTED, MessageModel, None)],
        permission=Permission.LICENSE_SYNC,
        description='Allows to trigger license sync'
    ),
    EndpointInfo(
        path=CustodianEndpoint.LICENSE_LICENSE_KEY_ACTIVATION,
        method=HTTPMethod.DELETE,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.LICENSE_DELETE_ACTIVATION,
        description='Allows to deactivate a specific license'
    ),
    EndpointInfo(
        path=CustodianEndpoint.LICENSE_LICENSE_KEY_ACTIVATION,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleLicenseActivationModel, None)],
        permission=Permission.LICENSE_GET_ACTIVATION,
        description='Allows to list tenants a license is activated for'
    ),
    EndpointInfo(
        path=CustodianEndpoint.LICENSE_LICENSE_KEY_ACTIVATION,
        method=HTTPMethod.PUT,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=LicenseActivationPutModel,
        responses=[(HTTPStatus.OK, SingleLicenseActivationModel, None)],
        permission=Permission.LICENSE_ACTIVATE,
        description='Allows to activate a specific license for some tenants'
    ),
    EndpointInfo(
        path=CustodianEndpoint.LICENSE_LICENSE_KEY_ACTIVATION,
        method=HTTPMethod.PATCH,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=LicenseActivationPatchModel,
        responses=[(HTTPStatus.OK, SingleLicenseActivationModel, None)],
        permission=Permission.LICENSE_UPDATE_ACTIVATION,
        description='Allows to update tenants the license is activated for'
    ),

    # settings
    EndpointInfo(
        path=CustodianEndpoint.SETTINGS_MAIL,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=MailSettingGetModel,
        responses=[(HTTPStatus.OK, SingleMailSettingModel, None)],
        permission=Permission.SETTINGS_DESCRIBE_MAIL,
        description='Allows to describe mail configuration'
    ),
    EndpointInfo(
        path=CustodianEndpoint.SETTINGS_MAIL,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=MailSettingPostModel,
        responses=[(HTTPStatus.CREATED, SingleMailSettingModel, None)],
        permission=Permission.SETTINGS_CREATE_MAIL,
        description='Allows to set mail configuration'
    ),
    EndpointInfo(
        path=CustodianEndpoint.SETTINGS_MAIL,
        method=HTTPMethod.DELETE,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.SETTINGS_DELETE_MAIL,
        description='Allows to delete mail configuration'
    ),
    EndpointInfo(
        path=CustodianEndpoint.SETTINGS_SEND_REPORTS,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=ReportsSendingSettingPostModel,
        responses=[(HTTPStatus.OK, MessageModel, None)],
        permission=Permission.SETTINGS_CHANGE_SET_REPORTS,
        description='Allows to enable or disable high-level reports sending'
    ),
    EndpointInfo(
        path=CustodianEndpoint.SETTINGS_LICENSE_MANAGER_CONFIG,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleLMConfigModel, None)],
        permission=Permission.SETTINGS_DESCRIBE_LM_CONFIG,
        description='Allows to get license manager configuration'
    ),
    EndpointInfo(
        path=CustodianEndpoint.SETTINGS_LICENSE_MANAGER_CONFIG,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=LicenseManagerConfigSettingPostModel,
        responses=[(HTTPStatus.CREATED, SingleLMConfigModel, None)],
        permission=Permission.SETTINGS_CREATE_LM_CONFIG,
        description='Allows to set license manager configuration'
    ),
    EndpointInfo(
        path=CustodianEndpoint.SETTINGS_LICENSE_MANAGER_CONFIG,
        method=HTTPMethod.DELETE,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.SETTINGS_DELETE_LM_CONFIG,
        description='Allows to delete license manager configuration'
    ),
    EndpointInfo(
        path=CustodianEndpoint.SETTINGS_LICENSE_MANAGER_CLIENT,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleLMClientModel, None)],
        permission=Permission.SETTINGS_DESCRIBE_LM_CLIENT,
        description='Allows to describe license manager client'
    ),
    EndpointInfo(
        path=CustodianEndpoint.SETTINGS_LICENSE_MANAGER_CLIENT,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=LicenseManagerClientSettingPostModel,
        responses=[(HTTPStatus.CREATED, SingleLMClientModel, None)],
        permission=Permission.SETTINGS_CREATE_LM_CLIENT,
        description='Allows to add license manager client'
    ),
    EndpointInfo(
        path=CustodianEndpoint.SETTINGS_LICENSE_MANAGER_CLIENT,
        method=HTTPMethod.DELETE,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=LicenseManagerClientSettingDeleteModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.SETTINGS_DELETE_LM_CLIENT,
        description='Allows to delete license manager client'
    ),

    # batch results
    EndpointInfo(
        path=CustodianEndpoint.BATCH_RESULTS,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.API_HANDLER,
        request_model=BatchResultsQueryModel,
        responses=[(HTTPStatus.OK, MultipleBatchResultsModel, None)],
        permission=Permission.BATCH_RESULTS_QUERY,
        description='Allows to query event driven jobs'
    ),
    EndpointInfo(
        path=CustodianEndpoint.BATCH_RESULTS_JOB_ID,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleBatchResultModel, None)],
        permission=Permission.BATCH_RESULTS_GET,
        description='Allows to get a specific event-driven job by id'
    ),

    # digest reports
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_DIGESTS_JOBS_JOB_ID,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.REPORT_GENERATOR,
        request_model=JobDigestReportGetModel,
        responses=[(HTTPStatus.OK, SingleJobReportModel, None)],
        permission=Permission.REPORT_DIGEST_DESCRIBE,
        description='Allows to get a digest report by job id'
    ),
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_DIGESTS_TENANTS_TENANT_NAME_JOBS,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.REPORT_GENERATOR,
        request_model=TenantJobsDigestsReportGetModel,
        responses=[(HTTPStatus.OK, MultipleJobReportModel, None)],
        permission=Permission.REPORT_DIGEST_DESCRIBE,
        description='Allows to get multiple digest reports by tenant latest jobs'
    ),

    # details reports
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_DETAILS_JOBS_JOB_ID,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.REPORT_GENERATOR,
        request_model=JobDetailsReportGetModel,
        responses=[(HTTPStatus.OK, SingleJobReportModel, None)],
        permission=Permission.REPORT_DETAILS_DESCRIBE,
        description='Allows to get a detailed report by job id'
    ),
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_DETAILS_TENANTS_TENANT_NAME_JOBS,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.REPORT_GENERATOR,
        request_model=TenantJobsDetailsReportGetModel,
        responses=[(HTTPStatus.OK, MultipleJobReportModel, None)],
        permission=Permission.REPORT_DETAILS_DESCRIBE,
        description='Allows to get multiple detailed reports by tenant latest jobs'
    ),

    # findings reports
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_FINDINGS_JOBS_JOB_ID,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.REPORT_GENERATOR,
        request_model=JobFindingsReportGetModel,
        responses=[(HTTPStatus.OK, SingleJobReportModel, None)],
        permission=Permission.REPORT_FINDINGS_DESCRIBE,
        description='Allows to get findings by job id'
    ),
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_FINDINGS_TENANTS_TENANT_NAME_JOBS,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.REPORT_GENERATOR,
        request_model=TenantJobsFindingsReportGetModel,
        responses=[(HTTPStatus.OK, MultipleJobReportModel, None)],
        permission=Permission.REPORT_FINDINGS_DESCRIBE,
        description='Allows to get findings by latest jobs of a tenant'
    ),

    # compliance reports
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_COMPLIANCE_JOBS_JOB_ID,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.REPORT_GENERATOR,
        request_model=JobComplianceReportGetModel,
        responses=[(HTTPStatus.OK, SingleJobReportModel, None)],
        permission=Permission.REPORT_COMPLIANCE_DESCRIBE_JOB,
        description='Allows to get compliance report by a job'
    ),
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_COMPLIANCE_TENANTS_TENANT_NAME,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.REPORT_GENERATOR,
        request_model=TenantComplianceReportGetModel,
        responses=[(HTTPStatus.OK, SingleEntityReportModel, None)],
        permission=Permission.REPORT_COMPLIANCE_DESCRIBE_TENANT,
        description='Allows to get a compliance report by tenant'
    ),

    # errors report
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_ERRORS_JOBS_JOB_ID,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.REPORT_GENERATOR,
        request_model=JobErrorReportGetModel,
        responses=[(HTTPStatus.OK, ErrorsReportModel, None)],
        permission=Permission.REPORT_ERRORS_DESCRIBE,
        description='Allows to get errors occurred during a job'
    ),

    # rules report
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_RULES_JOBS_JOB_ID,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.REPORT_GENERATOR,
        request_model=JobRuleReportGetModel,
        responses=[(HTTPStatus.OK, RulesReportModel, None)],
        permission=Permission.REPORT_RULES_DESCRIBE_JOB,
        description='Allows to get information about rules executed during a job'
    ),
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_RULES_TENANTS_TENANT_NAME,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.REPORT_GENERATOR,
        request_model=TenantRuleReportGetModel,
        responses=[(HTTPStatus.OK, EntityRulesReportModel, None)],
        permission=Permission.REPORT_RULES_DESCRIBE_TENANT,
        description='Allows to get average rules data by latest tenant jobs'
    ),

    # push to dojo report
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_PUSH_DOJO_JOB_ID,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.REPORT_GENERATOR,
        request_model=ReportPushByJobIdModel,
        responses=[(HTTPStatus.OK, SingleDefectDojoPushResult, None)],
        permission=Permission.REPORT_PUSH_TO_DOJO,
        description='Allows to push a specific job to Defect Dojo'
    ),
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_PUSH_DOJO,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.REPORT_GENERATOR,
        request_model=ReportPushMultipleModel,
        responses=[(HTTPStatus.OK, MultipleDefectDojoPushResult, None)],
        permission=Permission.REPORT_PUSH_TO_DOJO_BATCH,
        description='Allows to push multiple jobs to Defect Dojo'
    ),

    # high level reports
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_OPERATIONAL,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.REPORT_GENERATOR,
        request_model=OperationalGetReportModel,
        responses=[(HTTPStatus.OK, MessageModel, None)],
        permission=Permission.REPORT_OPERATIONAL,
        description='Allows to request operational report'
    ),
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_PROJECT,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.REPORT_GENERATOR,
        request_model=ProjectGetReportModel,
        responses=[(HTTPStatus.OK, MessageModel, None)],
        permission=Permission.REPORT_PROJECT,
        description='Allows to request project report'
    ),
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_DEPARTMENT,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.REPORT_GENERATOR,
        request_model=DepartmentGetReportModel,
        responses=[(HTTPStatus.OK, MessageModel, None)],
        permission=Permission.REPORT_DEPARTMENT,
        description='Allows to request department report'
    ),
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_CLEVEL,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.REPORT_GENERATOR,
        request_model=CLevelGetReportModel,
        responses=[(HTTPStatus.OK, MessageModel, None)],
        permission=Permission.REPORT_CLEVEL,
        description='Allows to request clevel report'
    ),
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_DIAGNOSTIC,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.REPORT_GENERATOR,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, MessageModel, None)],
        permission=Permission.REPORT_DIAGNOSTIC,
        description='Allows to get diagnostic report'
    ),
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_STATUS,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=ReportStatusGetModel,
        responses=[(HTTPStatus.OK, MultipleReportStatusModel, None)],
        permission=Permission.REPORT_STATUS,
        description='Allows to get a status of report by id'
    ),

    # resources reports
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_RESOURCES_PLATFORMS_K8S_PLATFORM_ID_LATEST,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.REPORT_GENERATOR,
        request_model=PlatformK8sResourcesReportGetModel,
        responses=[(HTTPStatus.OK, EntityResourcesReportModel, None)],
        permission=Permission.REPORT_RESOURCES_GET_K8S_PLATFORM_LATEST,
        description='Allows to get latest resources report by K8S platform'
    ),
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_RESOURCES_TENANTS_TENANT_NAME_LATEST,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.REPORT_GENERATOR,
        request_model=ResourcesReportGetModel,
        responses=[(HTTPStatus.OK, EntityResourcesReportModel, None)],
        permission=Permission.REPORT_RESOURCES_GET_TENANT_LATEST,
        description='Allows to get latest resources report by tenant'
    ),
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_RESOURCES_TENANTS_TENANT_NAME_JOBS,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.REPORT_GENERATOR,
        request_model=ResourceReportJobsGetModel,
        responses=[(HTTPStatus.OK, JobResourcesReportModel, None)],
        permission=Permission.REPORT_RESOURCES_GET_JOBS_BATCH,
        description='Allows to get latest resources report by latest tenant jobs'
    ),
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_RESOURCES_JOBS_JOB_ID,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.REPORT_GENERATOR,
        request_model=ResourceReportJobGetModel,
        responses=[(HTTPStatus.OK, JobResourcesReportModel, None)],
        permission=Permission.REPORT_RESOURCES_GET_JOBS,
        description='Allows to get latest resources report by job'
    ),
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_RAW_TENANTS_TENANT_NAME_STATE_LATEST,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.REPORT_GENERATOR,
        request_model=RawReportGetModel,
        responses=[(HTTPStatus.OK, RawReportModel, None)],
        permission=Permission.REPORT_RAW_GET_TENANT_LATEST,
        description='Allows to request raw report data by tenant'
    ),

    # platforms
    EndpointInfo(
        path=CustodianEndpoint.PLATFORMS_K8S,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=PlatformK8sQueryModel,
        responses=[(HTTPStatus.OK, MultipleK8SPlatformsModel, None)],
        permission=Permission.PLATFORM_QUERY_K8S,
        description='Allows to query registered K8S platforms'
    ),
    EndpointInfo(
        path=CustodianEndpoint.PLATFORMS_K8S,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=PlatformK8SPostModel,
        responses=[(HTTPStatus.OK, SingleK8SPlatformModel, None)],
        permission=Permission.PLATFORM_CREATE_K8S,
        description='Allows to register K8S platform'
    ),
    EndpointInfo(
        path=CustodianEndpoint.PLATFORMS_K8S_ID,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleK8SPlatformModel, None)],
        permission=Permission.PLATFORM_GET_K8S,
        description='Allows to register K8S platform'
    ),
    EndpointInfo(
        path=CustodianEndpoint.PLATFORMS_K8S_ID,
        method=HTTPMethod.DELETE,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.PLATFORM_DELETE_K8S,
        description='Allows to deregister a K8S platform'
    ),

    # dojo integrations
    EndpointInfo(
        path=CustodianEndpoint.INTEGRATIONS_DEFECT_DOJO,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=DefectDojoPostModel,
        responses=[(HTTPStatus.CREATED, SingleDefeDojoModel, None)],
        permission=Permission.DOJO_INTEGRATION_CREATE,
        description='Allows to register Defect Dojo integration'
    ),
    EndpointInfo(
        path=CustodianEndpoint.INTEGRATIONS_DEFECT_DOJO,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=DefectDojoQueryModel,
        responses=[(HTTPStatus.OK, MultipleDefectDojoModel, None)],
        permission=Permission.DOJO_INTEGRATION_DESCRIBE,
        description='Allows to list registered Defect Dojo integrations'
    ),
    EndpointInfo(
        path=CustodianEndpoint.INTEGRATIONS_DEFECT_DOJO_ID,
        method=HTTPMethod.DELETE,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.DOJO_INTEGRATION_DELETE,
        description='Allows to delete Defect Dojo integration by id'
    ),
    EndpointInfo(
        path=CustodianEndpoint.INTEGRATIONS_DEFECT_DOJO_ID,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleDefeDojoModel, None)],
        permission=Permission.DOJO_INTEGRATION_DESCRIBE,
        description='Allows to describe Defect Dojo integration by id'
    ),
    EndpointInfo(
        path=CustodianEndpoint.INTEGRATIONS_DEFECT_DOJO_ID_ACTIVATION,
        method=HTTPMethod.PUT,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=DefectDojoActivationPutModel,
        responses=[(HTTPStatus.CREATED, SingleDefectDojoActivation, None)],
        permission=Permission.DOJO_INTEGRATION_ACTIVATE,
        description='Allows to activate Defect Dojo integration for tenants'
    ),
    EndpointInfo(
        path=CustodianEndpoint.INTEGRATIONS_DEFECT_DOJO_ID_ACTIVATION,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleDefectDojoActivation, None)],
        permission=Permission.DOJO_INTEGRATION_ACTIVATE,
        description='Allows to get tenants Defect Dojo integration is activated for'
    ),
    EndpointInfo(
        path=CustodianEndpoint.INTEGRATIONS_DEFECT_DOJO_ID_ACTIVATION,
        method=HTTPMethod.DELETE,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.DOJO_INTEGRATION_DELETE_ACTIVATION,
        description='Allows to deactivate Defect Dojo integration'
    ),

    # self integration
    EndpointInfo(
        path=CustodianEndpoint.INTEGRATIONS_SELF,
        method=HTTPMethod.PUT,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=SelfIntegrationPutModel,
        responses=[(HTTPStatus.CREATED, SingleSelfIntegration, None)],
        permission=Permission.SRE_INTEGRATION_CREATE,
        description='Allows to create an application with type CUSTODIAN for integration with Maestro'
    ),
    EndpointInfo(
        path=CustodianEndpoint.INTEGRATIONS_SELF,
        method=HTTPMethod.PATCH,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=SelfIntegrationPatchModel,
        responses=[(HTTPStatus.OK, SingleSelfIntegration, None)],
        permission=Permission.SRE_INTEGRATION_UPDATE,
        description='Allows to change tenants that are active for integrations with Maestro'
    ),
    EndpointInfo(
        path=CustodianEndpoint.INTEGRATIONS_SELF,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleSelfIntegration, None)],
        permission=Permission.SRE_INTEGRATION_DESCRIBE,
        description='Allows to get integration with Maestro'
    ),
    EndpointInfo(
        path=CustodianEndpoint.INTEGRATIONS_SELF,
        method=HTTPMethod.DELETE,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.SRE_INTEGRATION_DELETE,
        description='Allows to delete an integration with Maestro'
    ),

    # Users
    EndpointInfo(
        path=CustodianEndpoint.USERS_USERNAME,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleUserModel, None)],
        permission=Permission.USERS_DESCRIBE,
        description='Allows to get an API user by name'
    ),
    EndpointInfo(
        path=CustodianEndpoint.USERS,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.API_HANDLER,
        request_model=UserPostModel,
        responses=[(HTTPStatus.OK, SingleUserModel, None),
                   (HTTPStatus.CONFLICT, MessageModel, None)],
        permission=Permission.USERS_CREATE,
        description='Allows to create a new API user'
    ),
    EndpointInfo(
        path=CustodianEndpoint.USERS_USERNAME,
        method=HTTPMethod.PATCH,
        lambda_name=LambdaName.API_HANDLER,
        request_model=UserPatchModel,
        responses=[(HTTPStatus.OK, SingleUserModel, None)],
        permission=Permission.USERS_UPDATE,
        description='Allows to update a specific user'
    ),
    EndpointInfo(
        path=CustodianEndpoint.USERS_USERNAME,
        method=HTTPMethod.DELETE,
        lambda_name=LambdaName.API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.USERS_DELETE,
        description='Allows to delete a specific user'
    ),
    EndpointInfo(
        path=CustodianEndpoint.USERS,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.API_HANDLER,
        request_model=BasePaginationModel,
        responses=[(HTTPStatus.OK, MultipleUsersModel, None)],
        permission=Permission.USERS_DESCRIBE,
        description='Allows to list API users'
    ),
    EndpointInfo(
        path=CustodianEndpoint.USERS_WHOAMI,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleUserModel, None)],
        permission=Permission.USERS_GET_CALLER,
        description='Allows to describe the user making this call'
    ),
    EndpointInfo(
        path=CustodianEndpoint.USERS_RESET_PASSWORD,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.API_HANDLER,
        request_model=UserResetPasswordModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.USERS_RESET_PASSWORD,
        description='Allows to change you password'
    ),

    EndpointInfo(
        path=CustodianEndpoint.INTEGRATIONS_CHRONICLE,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=ChroniclePostModel,
        responses=[(HTTPStatus.CREATED, SingleChronicleModel, None)],
        permission=Permission.CHRONICLE_INTEGRATION_CREATE,
        description='Registers a google Chronicle instance'
    ),
    EndpointInfo(
        path=CustodianEndpoint.INTEGRATIONS_CHRONICLE,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BasePaginationModel,
        responses=[(HTTPStatus.OK, MultipleChronicleModel, None)],
        permission=Permission.CHRONICLE_INTEGRATION_DESCRIBE,
        description='Queries google Chronicle instances'
    ),
    EndpointInfo(
        path=CustodianEndpoint.INTEGRATIONS_CHRONICLE_ID,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleChronicleModel, None)],
        permission=Permission.CHRONICLE_INTEGRATION_DESCRIBE,
        description='Retrieves a specific google Chronicle instance'
    ),
    EndpointInfo(
        path=CustodianEndpoint.INTEGRATIONS_CHRONICLE_ID,
        method=HTTPMethod.DELETE,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.CHRONICLE_INTEGRATION_DELETE,
        description='Deregisters a specific google Chronicle instance'
    ),
    EndpointInfo(
        path=CustodianEndpoint.INTEGRATIONS_CHRONICLE_ID_ACTIVATION,
        method=HTTPMethod.PUT,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=ChronicleActivationPutModel,
        responses=[(HTTPStatus.CREATED, SingleChronicleActivationModel, None)],
        permission=Permission.CHRONICLE_INTEGRATION_ACTIVATE,
        description='Allows to activate Chronicle integration for tenants'
    ),
    EndpointInfo(
        path=CustodianEndpoint.INTEGRATIONS_CHRONICLE_ID_ACTIVATION,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleDefectDojoActivation, None)],
        permission=Permission.CHRONICLE_INTEGRATION_GET_ACTIVATION,
        description='Allows to get tenants Chronicle integration is activated for'
    ),
    EndpointInfo(
        path=CustodianEndpoint.INTEGRATIONS_CHRONICLE_ID_ACTIVATION,
        method=HTTPMethod.DELETE,
        lambda_name=LambdaName.CONFIGURATION_API_HANDLER,
        request_model=BaseModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.CHRONICLE_INTEGRATION_DELETE_ACTIVATION,
        description='Allows to deactivate Chronicle integration'
    ),
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_PUSH_CHRONICLE_JOB_ID,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.REPORT_GENERATOR,
        request_model=ReportPushByJobIdModel,
        responses=[(HTTPStatus.OK, SingleChroniclePushResult, None)],
        permission=Permission.REPORT_PUSH_TO_CHRONICLE,
        description='Allows to push a specific job to Chronicle'
    ),
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_PUSH_CHRONICLE_TENANTS_TENANT_NAME,
        method=HTTPMethod.POST,
        lambda_name=LambdaName.REPORT_GENERATOR,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleChroniclePushResult, None)],
        permission=Permission.REPORT_PUSH_TO_CHRONICLE_TENANT,
        description='Allows to push tenant data to Chronicle'
    ),
    EndpointInfo(
        path=CustodianEndpoint.DOC,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.API_HANDLER,
        request_model=None,
        responses=[(HTTPStatus.OK, None, None)],
        auth=False,
    ),
    EndpointInfo(
        path=CustodianEndpoint.DOC_SWAGGER_JSON,
        method=HTTPMethod.GET,
        lambda_name=LambdaName.API_HANDLER,
        request_model=None,
        responses=[(HTTPStatus.OK, None, None)],
        auth=False,
    ),
)

common_responses = (
    (HTTPStatus.BAD_REQUEST, ErrorsModel, 'Validation error'),
    (HTTPStatus.UNAUTHORIZED, MessageModel, 'Invalid credentials'),
    (HTTPStatus.FORBIDDEN, MessageModel, 'Cannot access the resource'),
    (HTTPStatus.INTERNAL_SERVER_ERROR, MessageModel, 'Server error'),
    (HTTPStatus.SERVICE_UNAVAILABLE, MessageModel,
     'Service is temporarily unavailable'),
    (HTTPStatus.GATEWAY_TIMEOUT, MessageModel,
     'Gateway 30s timeout is reached')
)


def iter_all() -> Generator[EndpointInfo, None, None]:
    """
    Extends data
    :return:
    """
    for endpoint in data:
        existing = {r[0] for r in endpoint.responses}
        for tpl in common_responses:
            if tpl[0] in existing:
                continue
            endpoint.responses.append(tpl)
        if '{' in endpoint.path and HTTPStatus.NOT_FOUND not in existing:
            endpoint.responses.append((HTTPStatus.NOT_FOUND, MessageModel,
                                       'Entity is not found'))
        endpoint.responses.sort(key=lambda x: x[0])
        yield endpoint


def iter_models(without_get: bool = True
                ) -> Generator[type[BaseModel], None, None]:
    """
    :param without_get: omits request models for GET method
    :return:
    """
    models = set()
    for endpoint in iter_all():
        request_model = endpoint.request_model
        if without_get and endpoint.method == HTTPMethod.GET:
            request_model = None

        if request_model:
            models.add(request_model)

        models.update(resp[1] for resp in endpoint.responses if resp[1])
    yield from models


permissions_mapping = {
    (CustodianEndpoint(e.path), e.method): e.permission
    for e in iter_all()
}
