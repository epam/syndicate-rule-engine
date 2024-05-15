from http import HTTPStatus
from typing import Generator

from helpers.constants import CustodianEndpoint, HTTPMethod, Permission
from services.openapi_spec_generator import EndpointInfo
from validators.swagger_request_models import (
    BaseModel,
    BasePaginationModel,
    BatchResultsQueryModel,
    CLevelGetReportModel,
    CredentialsBindModel,
    CredentialsQueryModel,
    CustomerExcludedRulesPutModel,
    CustomerGetModel,
    DefectDojoActivationPutModel,
    DefectDojoPostModel,
    DefectDojoQueryModel,
    DepartmentGetReportModel,
    EventDrivenRulesetDeleteModel,
    EventDrivenRulesetGetModel,
    EventDrivenRulesetPostModel,
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
    RuleSourceGetModel,
    RuleSourcePatchModel,
    RuleSourcePostModel,
    RuleUpdateMetaPostModel,
    RulesetContentGetModel,
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
    StandardJobPostModel,
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
    RawReportGetModel
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
    MultipleCredentialsModel,
    MultipleCustomersModel,
    MultipleDefectDojoModel,
    MultipleDefectDojoPushResult,
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
    RulesReportModel,
    SignInModel,
    SingleBatchResultModel,
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
    RawReportModel
)


data: tuple[EndpointInfo, ...] = (
    # auth
    EndpointInfo(
        path=CustodianEndpoint.SIGNUP,
        method=HTTPMethod.POST,
        request_model=SignUpModel,
        responses=[(HTTPStatus.CREATED, MessageModel, None),
                   (HTTPStatus.CONFLICT, MessageModel, None)],
        auth=False
    ),
    EndpointInfo(
        path=CustodianEndpoint.SIGNIN,
        method=HTTPMethod.POST,
        request_model=SignInPostModel,
        responses=[(HTTPStatus.OK, SignInModel, None)],
        auth=False
    ),
    EndpointInfo(
        path=CustodianEndpoint.REFRESH,
        method=HTTPMethod.POST,
        request_model=RefreshPostModel,
        responses=[(HTTPStatus.OK, SignInModel, None)],
        auth=False
    ),

    # event
    EndpointInfo(
        path=CustodianEndpoint.EVENT,
        method=HTTPMethod.POST,
        request_model=EventPostModel,
        responses=[(HTTPStatus.ACCEPTED, EventModel, None)],
        permission=Permission.EVENT_POST
    ),

    # health
    EndpointInfo(
        path=CustodianEndpoint.HEALTH,
        method=HTTPMethod.GET,
        request_model=HealthCheckQueryModel,
        responses=[(HTTPStatus.OK, MultipleHealthChecksModel, None)]
    ),
    EndpointInfo(
        path=CustodianEndpoint.HEALTH_ID,
        method=HTTPMethod.GET,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleHealthCheckModel, None)]
    ),

    # jobs
    EndpointInfo(
        path=CustodianEndpoint.JOBS_STANDARD,
        method=HTTPMethod.POST,
        request_model=StandardJobPostModel,
        responses=[(HTTPStatus.ACCEPTED, SingleJobModel, None)],
        permission=Permission.JOB_POST_STANDARD
    ),
    EndpointInfo(
        path=CustodianEndpoint.JOBS_K8S,
        method=HTTPMethod.POST,
        request_model=K8sJobPostModel,
        responses=[(HTTPStatus.ACCEPTED, SingleJobModel, None)],
        permission=Permission.JOB_POST_K8S
    ),
    EndpointInfo(
        path=CustodianEndpoint.JOBS,
        method=HTTPMethod.GET,
        request_model=JobGetModel,
        responses=[(HTTPStatus.OK, MultipleJobsModel, None)],
        permission=Permission.JOB_QUERY
    ),
    EndpointInfo(
        path=CustodianEndpoint.JOBS,
        method=HTTPMethod.POST,
        request_model=JobPostModel,
        responses=[(HTTPStatus.ACCEPTED, SingleJobModel, None)],
        permission=Permission.JOB_POST_LICENSED
    ),
    EndpointInfo(
        path=CustodianEndpoint.JOBS_JOB,
        method=HTTPMethod.GET,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleJobModel, None)],
        permission=Permission.JOB_GET
    ),
    EndpointInfo(
        path=CustodianEndpoint.JOBS_JOB,
        method=HTTPMethod.DELETE,
        request_model=BaseModel,
        responses=[(HTTPStatus.ACCEPTED, MessageModel, None)],
        permission=Permission.JOB_TERMINATE
    ),

    # scheduled jobs
    EndpointInfo(
        path=CustodianEndpoint.SCHEDULED_JOB,
        method=HTTPMethod.GET,
        request_model=ScheduledJobGetModel,
        responses=[(HTTPStatus.OK, MultipleScheduledJobsModel, None)],
        permission=Permission.SCHEDULED_JOB_QUERY
    ),
    EndpointInfo(
        path=CustodianEndpoint.SCHEDULED_JOB,
        method=HTTPMethod.POST,
        request_model=ScheduledJobPostModel,
        responses=[(HTTPStatus.CREATED, SingleScheduledJobModel, None)],
        permission=Permission.SCHEDULED_JOB_CREATE
    ),
    EndpointInfo(
        path=CustodianEndpoint.SCHEDULED_JOB_NAME,
        method=HTTPMethod.GET,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleScheduledJobModel, None)],
        permission=Permission.SCHEDULED_JOB_GET
    ),
    EndpointInfo(
        path=CustodianEndpoint.SCHEDULED_JOB_NAME,
        method=HTTPMethod.PATCH,
        request_model=ScheduledJobPatchModel,
        responses=[(HTTPStatus.OK, SingleScheduledJobModel, None)],
        permission=Permission.SCHEDULED_JOB_UPDATE
    ),
    EndpointInfo(
        path=CustodianEndpoint.SCHEDULED_JOB_NAME,
        method=HTTPMethod.DELETE,
        request_model=BaseModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.SCHEDULED_JOB_DELETE
    ),

    # customers
    EndpointInfo(
        path=CustodianEndpoint.CUSTOMERS,
        method=HTTPMethod.GET,
        request_model=CustomerGetModel,
        responses=[(HTTPStatus.OK, MultipleCustomersModel, None)],
        permission=Permission.CUSTOMER_DESCRIBE
    ),
    EndpointInfo(
        path=CustodianEndpoint.CUSTOMERS_RABBITMQ,
        method=HTTPMethod.GET,
        request_model=RabbitMQGetModel,
        responses=[(HTTPStatus.OK, SingleRabbitMQModel, None)],
        permission=Permission.RABBITMQ_DESCRIBE
    ),
    EndpointInfo(
        path=CustodianEndpoint.CUSTOMERS_RABBITMQ,
        method=HTTPMethod.POST,
        request_model=RabbitMQPostModel,
        responses=[(HTTPStatus.OK, SingleRabbitMQModel, None)],
        permission=Permission.RABBITMQ_CREATE
    ),
    EndpointInfo(
        path=CustodianEndpoint.CUSTOMERS_RABBITMQ,
        method=HTTPMethod.DELETE,
        request_model=RabbitMQDeleteModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.RABBITMQ_DELETE
    ),
    EndpointInfo(
        path=CustodianEndpoint.CUSTOMERS_EXCLUDED_RULES,
        method=HTTPMethod.PUT,
        request_model=CustomerExcludedRulesPutModel,
        responses=[(HTTPStatus.OK, SingleCustomerExcludedRules, None)],
        permission=Permission.CUSTOMER_SET_EXCLUDED_RULES
    ),
    EndpointInfo(
        path=CustodianEndpoint.CUSTOMERS_EXCLUDED_RULES,
        method=HTTPMethod.GET,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleCustomerExcludedRules, None)],
        permission=Permission.CUSTOMER_GET_EXCLUDED_RULES
    ),

    # tenants
    EndpointInfo(
        path=CustodianEndpoint.TENANTS,
        method=HTTPMethod.GET,
        request_model=MultipleTenantsGetModel,
        responses=[(HTTPStatus.OK, MultipleTenantsModel, None)],
        permission=Permission.TENANT_QUERY
    ),
    EndpointInfo(
        path=CustodianEndpoint.TENANTS_TENANT_NAME,
        method=HTTPMethod.GET,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleTenantsModel, None)],
        permission=Permission.TENANT_GET
    ),
    EndpointInfo(
        path=CustodianEndpoint.TENANTS_TENANT_NAME_ACTIVE_LICENSES,
        method=HTTPMethod.GET,
        request_model=TenantGetActiveLicensesModel,
        responses=[(HTTPStatus.OK, MultipleLicensesModel, None)],
        permission=Permission.TENANT_GET_ACTIVE_LICENSES
    ),
    EndpointInfo(
        path=CustodianEndpoint.TENANTS_TENANT_NAME_EXCLUDED_RULES,
        method=HTTPMethod.PUT,
        request_model=TenantExcludedRulesPutModel,
        responses=[(HTTPStatus.OK, SingleTenantExcludedRules, None)],
        permission=Permission.TENANT_SET_EXCLUDED_RULES
    ),
    EndpointInfo(
        path=CustodianEndpoint.TENANTS_TENANT_NAME_EXCLUDED_RULES,
        method=HTTPMethod.GET,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleTenantExcludedRules, None)],
        permission=Permission.TENANT_GET_EXCLUDED_RULES
    ),

    # credentials
    EndpointInfo(
        path=CustodianEndpoint.CREDENTIALS,
        method=HTTPMethod.GET,
        request_model=CredentialsQueryModel,
        responses=[(HTTPStatus.OK, MultipleCredentialsModel, None)],
        permission=Permission.CREDENTIALS_DESCRIBE,
    ),
    EndpointInfo(
        path=CustodianEndpoint.CREDENTIALS_ID,
        method=HTTPMethod.GET,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleCredentialsModel, None)],
        permission=Permission.CREDENTIALS_DESCRIBE,
    ),
    EndpointInfo(
        path=CustodianEndpoint.CREDENTIALS_ID_BINDING,
        method=HTTPMethod.GET,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, CredentialsActivationModel, None)],
        permission=Permission.CREDENTIALS_GET_BINDING
    ),
    EndpointInfo(
        path=CustodianEndpoint.CREDENTIALS_ID_BINDING,
        method=HTTPMethod.PUT,
        request_model=CredentialsBindModel,
        responses=[(HTTPStatus.OK, CredentialsActivationModel, None)],
        permission=Permission.CREDENTIALS_BIND
    ),
    EndpointInfo(
        path=CustodianEndpoint.CREDENTIALS_ID_BINDING,
        method=HTTPMethod.DELETE,
        request_model=BaseModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.CREDENTIALS_UNBIND
    ),

    # rules
    EndpointInfo(
        path=CustodianEndpoint.RULES,
        method=HTTPMethod.GET,
        request_model=RuleGetModel,
        responses=[(HTTPStatus.OK, MultipleRulesModel, None)],
        permission=Permission.RULE_DESCRIBE
    ),
    EndpointInfo(
        path=CustodianEndpoint.RULES,
        method=HTTPMethod.DELETE,
        request_model=RuleDeleteModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.RULE_DELETE
    ),
    EndpointInfo(
        path=CustodianEndpoint.RULE_META_UPDATER,
        method=HTTPMethod.POST,
        request_model=RuleUpdateMetaPostModel,
        responses=[(HTTPStatus.ACCEPTED, MultipleRuleMetaUpdateModel, None)],
        permission=Permission.RULE_UPDATE_META
    ),

    # metrics
    EndpointInfo(
        path=CustodianEndpoint.METRICS_UPDATE,
        method=HTTPMethod.POST,
        request_model=BaseModel,
        responses=[(HTTPStatus.ACCEPTED, MessageModel, None)],
        permission=Permission.METRICS_UPDATE
    ),
    EndpointInfo(
        path=CustodianEndpoint.METRICS_STATUS,
        method=HTTPMethod.GET,
        request_model=MetricsStatusGetModel,
        responses=[(HTTPStatus.OK, MultipleMetricsStatusesModel, None)],
        permission=Permission.METRICS_STATUS
    ),

    # rulesets
    EndpointInfo(
        path=CustodianEndpoint.RULESETS,
        method=HTTPMethod.GET,
        request_model=RulesetGetModel,
        responses=[(HTTPStatus.OK, MultipleRulesetsModel, None)],
        permission=Permission.RULESET_DESCRIBE
    ),
    EndpointInfo(
        path=CustodianEndpoint.RULESETS,
        method=HTTPMethod.POST,
        request_model=RulesetPostModel,
        responses=[(HTTPStatus.CREATED, SingleRulesetModel, None)],
        permission=Permission.RULESET_CREATE
    ),
    EndpointInfo(
        path=CustodianEndpoint.RULESETS,
        method=HTTPMethod.PATCH,
        request_model=RulesetPatchModel,
        responses=[(HTTPStatus.OK, SingleRulesetModel, None)],
        permission=Permission.RULESET_UPDATE
    ),
    EndpointInfo(
        path=CustodianEndpoint.RULESETS,
        method=HTTPMethod.DELETE,
        request_model=RulesetDeleteModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.RULESET_DELETE
    ),
    EndpointInfo(
        path=CustodianEndpoint.ED_RULESETS,
        method=HTTPMethod.GET,
        request_model=EventDrivenRulesetGetModel,
        responses=[(HTTPStatus.OK, MultipleRulesetsModel, None)],
        permission=Permission.RULESET_DESCRIBE_ED
    ),
    EndpointInfo(
        path=CustodianEndpoint.ED_RULESETS,
        method=HTTPMethod.POST,
        request_model=EventDrivenRulesetPostModel,
        responses=[(HTTPStatus.CREATED, SingleRulesetModel, None)],
        permission=Permission.RULESET_CREATE_ED
    ),
    EndpointInfo(
        path=CustodianEndpoint.ED_RULESETS,
        method=HTTPMethod.DELETE,
        request_model=EventDrivenRulesetDeleteModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.RULESET_DELETE_ED
    ),
    EndpointInfo(
        path=CustodianEndpoint.RULESETS_CONTENT,
        method=HTTPMethod.GET,
        request_model=RulesetContentGetModel,
        responses=[(HTTPStatus.OK, MessageModel, None)],
        permission=Permission.RULESET_GET_CONTENT
    ),

    # rulesources
    EndpointInfo(
        path=CustodianEndpoint.RULE_SOURCES,
        method=HTTPMethod.GET,
        request_model=RuleSourceGetModel,
        responses=[(HTTPStatus.OK, MultipleRuleSourceModel, None)],
        permission=Permission.RULE_SOURCE_DESCRIBE
    ),
    EndpointInfo(
        path=CustodianEndpoint.RULE_SOURCES,
        method=HTTPMethod.POST,
        request_model=RuleSourcePostModel,
        responses=[(HTTPStatus.CREATED, SingleRuleSourceModel, None)],
        permission=Permission.RULE_SOURCE_CREATE
    ),
    EndpointInfo(
        path=CustodianEndpoint.RULE_SOURCES,
        method=HTTPMethod.PATCH,
        request_model=RuleSourcePatchModel,
        responses=[(HTTPStatus.OK, SingleRuleSourceModel, None)],
        permission=Permission.RULE_SOURCE_UPDATE
    ),
    EndpointInfo(
        path=CustodianEndpoint.RULE_SOURCES,
        method=HTTPMethod.DELETE,
        request_model=RuleSourceDeleteModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.RULE_SOURCE_DELETE
    ),

    # policies
    EndpointInfo(
        path=CustodianEndpoint.POLICIES,
        method=HTTPMethod.GET,
        request_model=BasePaginationModel,
        responses=[(HTTPStatus.OK, MultiplePoliciesModel, None)],
        permission=Permission.POLICY_DESCRIBE
    ),
    EndpointInfo(
        path=CustodianEndpoint.POLICIES_NAME,
        method=HTTPMethod.GET,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SinglePolicyModel, None)],
        permission=Permission.POLICY_DESCRIBE
    ),
    EndpointInfo(
        path=CustodianEndpoint.POLICIES,
        method=HTTPMethod.POST,
        request_model=PolicyPostModel,
        responses=[(HTTPStatus.CREATED, SinglePolicyModel, None)],
        permission=Permission.POLICY_CREATE
    ),
    EndpointInfo(
        path=CustodianEndpoint.POLICIES_NAME,
        method=HTTPMethod.PATCH,
        request_model=PolicyPatchModel,
        responses=[(HTTPStatus.OK, SinglePolicyModel, None)],
        permission=Permission.POLICY_UPDATE
    ),
    EndpointInfo(
        path=CustodianEndpoint.POLICIES_NAME,
        method=HTTPMethod.DELETE,
        request_model=BaseModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.POLICY_DELETE
    ),
    # EndpointInfo(
    #     path=CustodianEndpoint.POLICIES_CACHE,
    #     method=HTTPMethod.DELETE,
    #     request_model=PolicyCacheDeleteModel,
    #     responses=[(HTTPStatus.NO_CONTENT, None, None)],
    #     permission=Permission.POLICY_RESET_CACHE
    # ),

    # roles
    EndpointInfo(
        path=CustodianEndpoint.ROLES,
        method=HTTPMethod.GET,
        request_model=BasePaginationModel,
        responses=[(HTTPStatus.OK, MultipleRoleModel, None)],
        permission=Permission.ROLE_DESCRIBE
    ),
    EndpointInfo(
        path=CustodianEndpoint.ROLES_NAME,
        method=HTTPMethod.GET,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleRoleModel, None)],
        permission=Permission.ROLE_DESCRIBE
    ),
    EndpointInfo(
        path=CustodianEndpoint.ROLES,
        method=HTTPMethod.POST,
        request_model=RolePostModel,
        responses=[(HTTPStatus.CREATED, SingleRoleModel, None)],
        permission=Permission.ROLE_CREATE
    ),
    EndpointInfo(
        path=CustodianEndpoint.ROLES_NAME,
        method=HTTPMethod.PATCH,
        request_model=RolePatchModel,
        responses=[(HTTPStatus.OK, SingleRoleModel, None)],
        permission=Permission.ROLE_UPDATE
    ),
    EndpointInfo(
        path=CustodianEndpoint.ROLES_NAME,
        method=HTTPMethod.DELETE,
        request_model=BaseModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.ROLE_DELETE
    ),
    # EndpointInfo(
    #     path=CustodianEndpoint.ROLES_CACHE,
    #     method=HTTPMethod.DELETE,
    #     request_model=RoleCacheDeleteModel,
    #     responses=[(HTTPStatus.NO_CONTENT, None, None)],
    #     permission=Permission.ROLE_RESET_CACHE
    # ),

    # licenses
    EndpointInfo(
        path=CustodianEndpoint.LICENSES,
        method=HTTPMethod.POST,
        request_model=LicensePostModel,
        responses=[(HTTPStatus.ACCEPTED, None, None)],
        permission=Permission.LICENSE_ADD
    ),
    EndpointInfo(
        path=CustodianEndpoint.LICENSES,
        method=HTTPMethod.GET,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, MultipleLicensesModel, None)],
        permission=Permission.LICENSE_QUERY
    ),
    EndpointInfo(
        path=CustodianEndpoint.LICENSES_LICENSE_KEY,
        method=HTTPMethod.GET,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleLicenseModel, None)],
        permission=Permission.LICENSE_GET
    ),
    EndpointInfo(
        path=CustodianEndpoint.LICENSES_LICENSE_KEY,
        method=HTTPMethod.DELETE,
        request_model=BaseModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.LICENSE_DELETE
    ),
    EndpointInfo(
        path=CustodianEndpoint.LICENSES_LICENSE_KEY_SYNC,
        method=HTTPMethod.POST,
        request_model=BaseModel,
        responses=[(HTTPStatus.ACCEPTED, MessageModel, None)],
        permission=Permission.LICENSE_SYNC
    ),
    EndpointInfo(
        path=CustodianEndpoint.LICENSE_LICENSE_KEY_ACTIVATION,
        method=HTTPMethod.DELETE,
        request_model=BaseModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.LICENSE_DELETE_ACTIVATION
    ),
    EndpointInfo(
        path=CustodianEndpoint.LICENSE_LICENSE_KEY_ACTIVATION,
        method=HTTPMethod.GET,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleLicenseActivationModel, None)],
        permission=Permission.LICENSE_GET_ACTIVATION
    ),
    EndpointInfo(
        path=CustodianEndpoint.LICENSE_LICENSE_KEY_ACTIVATION,
        method=HTTPMethod.PUT,
        request_model=LicenseActivationPutModel,
        responses=[(HTTPStatus.OK, SingleLicenseActivationModel, None)],
        permission=Permission.LICENSE_ACTIVATE
    ),
    EndpointInfo(
        path=CustodianEndpoint.LICENSE_LICENSE_KEY_ACTIVATION,
        method=HTTPMethod.PATCH,
        request_model=LicenseActivationPatchModel,
        responses=[(HTTPStatus.OK, SingleLicenseActivationModel, None)],
        permission=Permission.LICENSE_UPDATE_ACTIVATION
    ),

    # settings
    EndpointInfo(
        path=CustodianEndpoint.SETTINGS_MAIL,
        method=HTTPMethod.GET,
        request_model=MailSettingGetModel,
        responses=[(HTTPStatus.OK, SingleMailSettingModel, None)],
        permission=Permission.SETTINGS_DESCRIBE_MAIL
    ),
    EndpointInfo(
        path=CustodianEndpoint.SETTINGS_MAIL,
        method=HTTPMethod.POST,
        request_model=MailSettingPostModel,
        responses=[(HTTPStatus.CREATED, SingleMailSettingModel, None)],
        permission=Permission.SETTINGS_CREATE_MAIL
    ),
    EndpointInfo(
        path=CustodianEndpoint.SETTINGS_MAIL,
        method=HTTPMethod.DELETE,
        request_model=BaseModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.SETTINGS_DELETE_MAIL
    ),
    EndpointInfo(
        path=CustodianEndpoint.SETTINGS_SEND_REPORTS,
        method=HTTPMethod.POST,
        request_model=ReportsSendingSettingPostModel,
        responses=[(HTTPStatus.OK, MessageModel, None)],
        permission=Permission.SETTINGS_CHANGE_SET_REPORTS
    ),
    EndpointInfo(
        path=CustodianEndpoint.SETTINGS_LICENSE_MANAGER_CONFIG,
        method=HTTPMethod.GET,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleLMConfigModel, None)],
        permission=Permission.SETTINGS_DESCRIBE_LM_CONFIG
    ),
    EndpointInfo(
        path=CustodianEndpoint.SETTINGS_LICENSE_MANAGER_CONFIG,
        method=HTTPMethod.POST,
        request_model=LicenseManagerConfigSettingPostModel,
        responses=[(HTTPStatus.CREATED, SingleLMConfigModel, None)],
        permission=Permission.SETTINGS_CREATE_LM_CONFIG
    ),
    EndpointInfo(
        path=CustodianEndpoint.SETTINGS_LICENSE_MANAGER_CONFIG,
        method=HTTPMethod.DELETE,
        request_model=BaseModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.SETTINGS_DELETE_LM_CONFIG
    ),
    EndpointInfo(
        path=CustodianEndpoint.SETTINGS_LICENSE_MANAGER_CLIENT,
        method=HTTPMethod.GET,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleLMClientModel, None)],
        permission=Permission.SETTINGS_DESCRIBE_LM_CLIENT
    ),
    EndpointInfo(
        path=CustodianEndpoint.SETTINGS_LICENSE_MANAGER_CLIENT,
        method=HTTPMethod.POST,
        request_model=LicenseManagerClientSettingPostModel,
        responses=[(HTTPStatus.CREATED, SingleLMClientModel, None)],
        permission=Permission.SETTINGS_CREATE_LM_CLIENT
    ),
    EndpointInfo(
        path=CustodianEndpoint.SETTINGS_LICENSE_MANAGER_CLIENT,
        method=HTTPMethod.DELETE,
        request_model=LicenseManagerClientSettingDeleteModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.SETTINGS_DELETE_LM_CLIENT
    ),

    # batch results
    EndpointInfo(
        path=CustodianEndpoint.BATCH_RESULTS,
        method=HTTPMethod.GET,
        request_model=BatchResultsQueryModel,
        responses=[(HTTPStatus.OK, MultipleBatchResultsModel, None)],
        permission=Permission.BATCH_RESULTS_QUERY
    ),
    EndpointInfo(
        path=CustodianEndpoint.BATCH_RESULTS_JOB_ID,
        method=HTTPMethod.GET,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleBatchResultModel, None)],
        permission=Permission.BATCH_RESULTS_GET
    ),

    # digest reports
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_DIGESTS_JOBS_JOB_ID,
        method=HTTPMethod.GET,
        request_model=JobDigestReportGetModel,
        responses=[(HTTPStatus.OK, SingleJobReportModel, None)],
        permission=Permission.REPORT_DIGEST_DESCRIBE
    ),
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_DIGESTS_TENANTS_TENANT_NAME_JOBS,
        method=HTTPMethod.GET,
        request_model=TenantJobsDigestsReportGetModel,
        responses=[(HTTPStatus.OK, MultipleJobReportModel, None)],
        permission=Permission.REPORT_DIGEST_DESCRIBE
    ),

    # details reports
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_DETAILS_JOBS_JOB_ID,
        method=HTTPMethod.GET,
        request_model=JobDetailsReportGetModel,
        responses=[(HTTPStatus.OK, SingleJobReportModel, None)],
        permission=Permission.REPORT_DETAILS_DESCRIBE
    ),
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_DETAILS_TENANTS_TENANT_NAME_JOBS,
        method=HTTPMethod.GET,
        request_model=TenantJobsDetailsReportGetModel,
        responses=[(HTTPStatus.OK, MultipleJobReportModel, None)],
        permission=Permission.REPORT_DETAILS_DESCRIBE
    ),

    # findings reports
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_FINDINGS_JOBS_JOB_ID,
        method=HTTPMethod.GET,
        request_model=JobFindingsReportGetModel,
        responses=[(HTTPStatus.OK, SingleJobReportModel, None)],
        permission=Permission.REPORT_FINDINGS_DESCRIBE
    ),
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_FINDINGS_TENANTS_TENANT_NAME_JOBS,
        method=HTTPMethod.GET,
        request_model=TenantJobsFindingsReportGetModel,
        responses=[(HTTPStatus.OK, MultipleJobReportModel, None)],
        permission=Permission.REPORT_FINDINGS_DESCRIBE
    ),

    # compliance reports
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_COMPLIANCE_JOBS_JOB_ID,
        method=HTTPMethod.GET,
        request_model=JobComplianceReportGetModel,
        responses=[(HTTPStatus.OK, SingleJobReportModel, None)],
        permission=Permission.REPORT_COMPLIANCE_DESCRIBE_JOB
    ),
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_COMPLIANCE_TENANTS_TENANT_NAME,
        method=HTTPMethod.GET,
        request_model=TenantComplianceReportGetModel,
        responses=[(HTTPStatus.OK, SingleEntityReportModel, None)],
        permission=Permission.REPORT_COMPLIANCE_DESCRIBE_TENANT
    ),

    # errors report
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_ERRORS_JOBS_JOB_ID,
        method=HTTPMethod.GET,
        request_model=JobErrorReportGetModel,
        responses=[(HTTPStatus.OK, ErrorsReportModel, None)],
        permission=Permission.REPORT_ERRORS_DESCRIBE
    ),

    # rules report
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_RULES_JOBS_JOB_ID,
        method=HTTPMethod.GET,
        request_model=JobRuleReportGetModel,
        responses=[(HTTPStatus.OK, RulesReportModel, None)],
        permission=Permission.REPORT_RULES_DESCRIBE_JOB
    ),
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_RULES_TENANTS_TENANT_NAME,
        method=HTTPMethod.GET,
        request_model=TenantRuleReportGetModel,
        responses=[(HTTPStatus.OK, EntityRulesReportModel, None)],
        permission=Permission.REPORT_RULES_DESCRIBE_TENANT
    ),

    # push to dojo report
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_PUSH_DOJO_JOB_ID,
        method=HTTPMethod.POST,
        request_model=ReportPushByJobIdModel,
        responses=[(HTTPStatus.OK, SingleDefectDojoPushResult, None)],
        permission=Permission.REPORT_PUSH_TO_DOJO
    ),
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_PUSH_DOJO,
        method=HTTPMethod.POST,
        request_model=ReportPushMultipleModel,
        responses=[(HTTPStatus.OK, MultipleDefectDojoPushResult, None)],
        permission=Permission.REPORT_PUSH_TO_DOJO_BATCH
    ),

    # high level reports
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_OPERATIONAL,
        method=HTTPMethod.POST,
        request_model=OperationalGetReportModel,
        responses=[(HTTPStatus.OK, MessageModel, None)],
        permission=Permission.REPORT_OPERATIONAL
    ),
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_PROJECT,
        method=HTTPMethod.POST,
        request_model=ProjectGetReportModel,
        responses=[(HTTPStatus.OK, MessageModel, None)],
        permission=Permission.REPORT_PROJECT
    ),
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_DEPARTMENT,
        method=HTTPMethod.POST,
        request_model=DepartmentGetReportModel,
        responses=[(HTTPStatus.OK, MessageModel, None)],
        permission=Permission.REPORT_DEPARTMENT
    ),
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_CLEVEL,
        method=HTTPMethod.POST,
        request_model=CLevelGetReportModel,
        responses=[(HTTPStatus.OK, MessageModel, None)],
        permission=Permission.REPORT_CLEVEL
    ),
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_DIAGNOSTIC,
        method=HTTPMethod.GET,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, MessageModel, None)],
        permission=Permission.REPORT_DIAGNOSTIC
    ),
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_STATUS,
        method=HTTPMethod.GET,
        request_model=ReportStatusGetModel,
        responses=[(HTTPStatus.OK, MultipleReportStatusModel, None)],
        permission=Permission.REPORT_STATUS
    ),

    # meta mappings
    EndpointInfo(
        path=CustodianEndpoint.META_STANDARDS,
        method=HTTPMethod.POST,
        request_model=BaseModel,
        responses=[(HTTPStatus.ACCEPTED, MessageModel, None)],
        permission=Permission.META_UPDATE_STANDARDS
    ),
    EndpointInfo(
        path=CustodianEndpoint.META_MAPPINGS,
        method=HTTPMethod.POST,
        request_model=BaseModel,
        responses=[(HTTPStatus.ACCEPTED, MessageModel, None)],
        permission=Permission.META_UPDATE_MAPPINGS
    ),
    EndpointInfo(
        path=CustodianEndpoint.META_META,
        method=HTTPMethod.POST,
        request_model=BaseModel,
        responses=[(HTTPStatus.ACCEPTED, MessageModel, None)],
        permission=Permission.META_UPDATE_META
    ),

    # resources reports
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_RESOURCES_PLATFORMS_K8S_PLATFORM_ID_LATEST,
        method=HTTPMethod.GET,
        request_model=PlatformK8sResourcesReportGetModel,
        responses=[(HTTPStatus.OK, EntityResourcesReportModel, None)],
        permission=Permission.REPORT_RESOURCES_GET_K8S_PLATFORM_LATEST
    ),
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_RESOURCES_TENANTS_TENANT_NAME_LATEST,
        method=HTTPMethod.GET,
        request_model=ResourcesReportGetModel,
        responses=[(HTTPStatus.OK, EntityResourcesReportModel, None)],
        permission=Permission.REPORT_RESOURCES_GET_TENANT_LATEST
    ),
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_RESOURCES_TENANTS_TENANT_NAME_JOBS,
        method=HTTPMethod.GET,
        request_model=ResourceReportJobsGetModel,
        responses=[(HTTPStatus.OK, JobResourcesReportModel, None)],
        permission=Permission.REPORT_RESOURCES_GET_JOBS_BATCH
    ),
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_RESOURCES_JOBS_JOB_ID,
        method=HTTPMethod.GET,
        request_model=ResourceReportJobGetModel,
        responses=[(HTTPStatus.OK, JobResourcesReportModel, None)],
        permission=Permission.REPORT_RESOURCES_GET_JOBS
    ),
    EndpointInfo(
        path=CustodianEndpoint.REPORTS_RAW_TENANTS_TENANT_NAME_STATE_LATEST,
        method=HTTPMethod.GET,
        request_model=RawReportGetModel,
        responses=[(HTTPStatus.OK, RawReportModel, None)],
        permission=Permission.REPORT_RAW_GET_TENANT_LATEST
    ),

    # platforms
    EndpointInfo(
        path=CustodianEndpoint.PLATFORMS_K8S,
        method=HTTPMethod.GET,
        request_model=PlatformK8sQueryModel,
        responses=[(HTTPStatus.OK, MultipleK8SPlatformsModel, None)],
        permission=Permission.PLATFORM_QUERY_K8S
    ),
    EndpointInfo(
        path=CustodianEndpoint.PLATFORMS_K8S,
        method=HTTPMethod.POST,
        request_model=PlatformK8SPostModel,
        responses=[(HTTPStatus.OK, SingleK8SPlatformModel, None)],
        permission=Permission.PLATFORM_CREATE_K8S
    ),
    EndpointInfo(
        path=CustodianEndpoint.PLATFORMS_K8S_ID,
        method=HTTPMethod.GET,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleK8SPlatformModel, None)],
        permission=Permission.PLATFORM_GET_K8S
    ),
    EndpointInfo(
        path=CustodianEndpoint.PLATFORMS_K8S_ID,
        method=HTTPMethod.DELETE,
        request_model=BaseModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.PLATFORM_DELETE_K8S
    ),

    # dojo integrations
    EndpointInfo(
        path=CustodianEndpoint.INTEGRATIONS_DEFECT_DOJO,
        method=HTTPMethod.POST,
        request_model=DefectDojoPostModel,
        responses=[(HTTPStatus.CREATED, SingleDefeDojoModel, None)],
        permission=Permission.DOJO_INTEGRATION_CREATE
    ),
    EndpointInfo(
        path=CustodianEndpoint.INTEGRATIONS_DEFECT_DOJO,
        method=HTTPMethod.GET,
        request_model=DefectDojoQueryModel,
        responses=[(HTTPStatus.OK, MultipleDefectDojoModel, None)],
        permission=Permission.DOJO_INTEGRATION_DESCRIBE
    ),
    EndpointInfo(
        path=CustodianEndpoint.INTEGRATIONS_DEFECT_DOJO_ID,
        method=HTTPMethod.DELETE,
        request_model=BaseModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.DOJO_INTEGRATION_DELETE
    ),
    EndpointInfo(
        path=CustodianEndpoint.INTEGRATIONS_DEFECT_DOJO_ID,
        method=HTTPMethod.GET,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleDefeDojoModel, None)],
        permission=Permission.DOJO_INTEGRATION_DESCRIBE
    ),
    EndpointInfo(
        path=CustodianEndpoint.INTEGRATIONS_DEFECT_DOJO_ID_ACTIVATION,
        method=HTTPMethod.PUT,
        request_model=DefectDojoActivationPutModel,
        responses=[(HTTPStatus.CREATED, SingleDefectDojoActivation, None)],
        permission=Permission.DOJO_INTEGRATION_ACTIVATE
    ),
    EndpointInfo(
        path=CustodianEndpoint.INTEGRATIONS_DEFECT_DOJO_ID_ACTIVATION,
        method=HTTPMethod.GET,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleDefectDojoActivation, None)],
        permission=Permission.DOJO_INTEGRATION_ACTIVATE
    ),
    EndpointInfo(
        path=CustodianEndpoint.INTEGRATIONS_DEFECT_DOJO_ID_ACTIVATION,
        method=HTTPMethod.DELETE,
        request_model=BaseModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.DOJO_INTEGRATION_DELETE_ACTIVATION
    ),

    # self integration
    EndpointInfo(
        path=CustodianEndpoint.INTEGRATIONS_SELF,
        method=HTTPMethod.PUT,
        request_model=SelfIntegrationPutModel,
        responses=[(HTTPStatus.CREATED, SingleSelfIntegration, None)],
        permission=Permission.SRE_INTEGRATION_CREATE
    ),
    EndpointInfo(
        path=CustodianEndpoint.INTEGRATIONS_SELF,
        method=HTTPMethod.PATCH,
        request_model=SelfIntegrationPatchModel,
        responses=[(HTTPStatus.OK, SingleSelfIntegration, None)],
        permission=Permission.SRE_INTEGRATION_UPDATE
    ),
    EndpointInfo(
        path=CustodianEndpoint.INTEGRATIONS_SELF,
        method=HTTPMethod.GET,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleSelfIntegration, None)],
        permission=Permission.SRE_INTEGRATION_DESCRIBE
    ),
    EndpointInfo(
        path=CustodianEndpoint.INTEGRATIONS_SELF,
        method=HTTPMethod.DELETE,
        request_model=BaseModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.SRE_INTEGRATION_DELETE
    ),

    # Users
    EndpointInfo(
        path=CustodianEndpoint.USERS_USERNAME,
        method=HTTPMethod.GET,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleUserModel, None)],
        permission=Permission.USERS_DESCRIBE
    ),
    EndpointInfo(
        path=CustodianEndpoint.USERS,
        method=HTTPMethod.POST,
        request_model=UserPostModel,
        responses=[(HTTPStatus.OK, SingleUserModel, None),
                   (HTTPStatus.CONFLICT, MessageModel, None)],
        permission=Permission.USERS_CREATE
    ),
    EndpointInfo(
        path=CustodianEndpoint.USERS_USERNAME,
        method=HTTPMethod.PATCH,
        request_model=UserPatchModel,
        responses=[(HTTPStatus.OK, SingleUserModel, None)],
        permission=Permission.USERS_UPDATE,
    ),
    EndpointInfo(
        path=CustodianEndpoint.USERS_USERNAME,
        method=HTTPMethod.DELETE,
        request_model=BaseModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.USERS_DELETE,
    ),
    EndpointInfo(
        path=CustodianEndpoint.USERS,
        method=HTTPMethod.GET,
        request_model=BasePaginationModel,
        responses=[(HTTPStatus.OK, MultipleUsersModel, None)],
        permission=Permission.USERS_DESCRIBE
    ),
    EndpointInfo(
        path=CustodianEndpoint.USERS_WHOAMI,
        method=HTTPMethod.GET,
        request_model=BaseModel,
        responses=[(HTTPStatus.OK, SingleUserModel, None)],
        permission=Permission.USERS_GET_CALLER
    ),
    EndpointInfo(
        path=CustodianEndpoint.USERS_RESET_PASSWORD,
        method=HTTPMethod.POST,
        request_model=UserResetPasswordModel,
        responses=[(HTTPStatus.NO_CONTENT, None, None)],
        permission=Permission.USERS_RESET_PASSWORD
    )
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
