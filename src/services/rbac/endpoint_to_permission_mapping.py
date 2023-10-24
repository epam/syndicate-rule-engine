from helpers.constants import HTTPMethod
from validators.request_validation import JobGetModel, SoloJobGetModel, \
    JobPostModel, JobDeleteModel, TenantGetModel, CustomerGetModel, \
    PolicyGetModel, PolicyPostModel, PolicyPatchModel, PolicyDeleteModel, \
    PolicyCacheDeleteModel, RoleGetModel, RolePostModel, RolePatchModel, \
    RoleDeleteModel, RoleCacheDeleteModel, RuleUpdateMetaPostModel, \
    RuleSourceGetModel, RuleSourcePostModel, RuleSourcePatchModel, \
    RuleSourceDeleteModel, CredentialsManagerGetModel, \
    CredentialsManagerPostModel, CredentialsManagerPatchModel, \
    CredentialsManagerDeleteModel, SignUpPostModel, SignInPostModel, \
    UserDeleteModel, UserPasswordResetPostModel, UserCustomerGetModel, \
    UserCustomerPostModel, UserCustomerPatchModel, UserCustomerDeleteModel, \
    UserRoleGetModel, UserRolePostModel, UserRolePatchModel, \
    UserRoleDeleteModel, UserTenantsGetModel, UserTenantsPatchModel, \
    UserTenantsDeleteModel, EventPostModel, LicenseGetModel, \
    LicenseDeleteModel, LicenseSyncPostModel, FindingsGetModel, \
    FindingsDeleteModel, ScheduledJobGetModel, SoloScheduledJobGetModel, \
    ScheduledJobPostModel, \
    ScheduledJobDeleteModel, ScheduledJobPatchModel, \
    MailSettingGetModel, MailSettingPostModel, \
    LicenseManagerConfigSettingPostModel, TenantErrorReportGetModel, \
    LicenseManagerClientSettingPostModel, JobRuleReportGetModel, \
    SoleBatchResultsGetModel, BatchResultsGetModel, CLevelGetReportModel, \
    JobReportGetModel, TimeRangedTenantsReportGetModel, \
    TimeRangedTenantReportGetModel, LicenseManagerClientSettingsGetModel, \
    JobErrorReportGetModel, TenantsErrorReportGetModel, \
    TenantsRuleReportGetModel, TenantRuleReportGetModel, \
    JobComplianceReportGetModel, TenantComplianceReportGetModel, \
    TenantPostModel, TenantRegionPostModel, \
    TenantPatchModel, OperationalGetReportModel, DepartmentGetReportModel, \
    ProjectGetReportModel, ApplicationPostModel, ApplicationGetModel, \
    ApplicationPatchModel, ApplicationDeleteModel, \
    ParentGetModel, ParentListModel, ParentDeleteModel, \
    ParentPatchModel, HealthCheckGetModel, SoleHealthCheckGetModel, \
    StandardJobPostModel, LicenseManagerClientSettingDeleteModel, \
    ApplicationListModel, RabbitMQGetModel, RabbitMQPostModel, \
    RabbitMQDeleteModel, AccessApplicationDeleteModel, \
    AccessApplicationGetModel, AccessApplicationListModel, \
    AccessApplicationPatchModel, AccessApplicationPostModel, \
    ReportPushByJobIdModel, ReportPushMultipleModel, \
    DojoApplicationDeleteModel, DojoApplicationGetModel, \
    DojoApplicationListModel, DojoApplicationPostModel, \
    DojoApplicationPatchModel, ResourcesReportGet, ResourceReportJobsGet, \
    ResourceReportJobGet

PERMISSIONS = 'permissions'
VALIDATION = 'validation'

# You can specify a validator for an endpoint in two different ways:
# - you can specify it below using VALIDATION key. You must provide a
#   pydantic model which contains query params/json body validation and
#   optionally expected types for path params
# - you can leave VALIDATION empty and specify a Pydantic model as type hint
#   for event inside the corresponding handler. Such validation models must
#   be inherited from PreparedEvent model. See
#   validators.request_validation.PlatformK8sPost and the place where it's used
ENDPOINT_PERMISSION_MAPPING = {
    '/reports/push/dojo/{job_id}/': {
        HTTPMethod.POST: {
            PERMISSIONS: 'report:push_report_to_dojo',
            VALIDATION: ReportPushByJobIdModel
        }
    },
    '/reports/push/security-hub/{job_id}/': {
        HTTPMethod.POST: {
            PERMISSIONS: 'report:push_report_to_security_hub',
            VALIDATION: ReportPushByJobIdModel
        }
    },
    '/reports/push/dojo/': {
        HTTPMethod.POST: {
            PERMISSIONS: 'report:push_report_to_dojo',
            VALIDATION: ReportPushMultipleModel
        }
    },
    '/reports/push/security-hub/': {
        HTTPMethod.POST: {
            PERMISSIONS: 'report:push_report_to_security_hub',
            VALIDATION: ReportPushMultipleModel
        }
    },
    '/reports/operational/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'report:operational',
            VALIDATION: OperationalGetReportModel
        }
    },
    '/reports/project/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'report:project',
            VALIDATION: ProjectGetReportModel
        }
    },
    '/reports/department/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'report:department',
            VALIDATION: DepartmentGetReportModel
        }
    },
    '/reports/clevel/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'report:clevel',
            VALIDATION: CLevelGetReportModel
        }
    },
    '/jobs/standard/': {
        HTTPMethod.POST: {
            PERMISSIONS: 'run:initiate_standard_run',
            VALIDATION: StandardJobPostModel
        },
    },
    '/jobs/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'run:describe_job',
            VALIDATION: JobGetModel
        },
        HTTPMethod.POST: {
            PERMISSIONS: 'run:initiate_run',
            VALIDATION: JobPostModel
        },
    },
    '/jobs/{job_id}/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'run:describe_job',
            VALIDATION: SoloJobGetModel
        },
        HTTPMethod.DELETE: {
            PERMISSIONS: 'run:terminate_run',
            VALIDATION: JobDeleteModel
        }
    },
    '/customers/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'customer:describe_customer',
            VALIDATION: CustomerGetModel
        },
        # HTTPMethod.PATCH: {
        #     PERMISSIONS: 'customer:update_customer',
        #     VALIDATION: CustomerPatchModel
        # },
    },
    '/tenants/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'tenant:describe_tenant',
            VALIDATION: TenantGetModel
        },
        HTTPMethod.POST: {
            PERMISSIONS: 'tenant:activate_tenant',
            VALIDATION: TenantPostModel
        },
        HTTPMethod.PATCH: {
            PERMISSIONS: 'tenant:update_tenant',
            VALIDATION: TenantPatchModel
        }
    },
    '/tenants/regions/': {
        HTTPMethod.POST: {
            PERMISSIONS: 'tenant:activate_region',
            VALIDATION: TenantRegionPostModel
        }
    },
    '/policies/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'iam:describe_policy',
            VALIDATION: PolicyGetModel
        },
        HTTPMethod.POST: {
            PERMISSIONS: 'iam:create_policy',
            VALIDATION: PolicyPostModel
        },
        HTTPMethod.PATCH: {
            PERMISSIONS: 'iam:update_policy',
            VALIDATION: PolicyPatchModel
        },
        HTTPMethod.DELETE: {
            PERMISSIONS: 'iam:remove_policy',
            VALIDATION: PolicyDeleteModel
        }
    },
    '/policies/cache/': {
        HTTPMethod.DELETE: {
            PERMISSIONS: 'iam:remove_policy_cache',  # todo system:
            VALIDATION: PolicyCacheDeleteModel
        }
    },
    '/roles/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'iam:describe_role',
            VALIDATION: RoleGetModel
        },
        HTTPMethod.POST: {
            PERMISSIONS: 'iam:create_role',
            VALIDATION: RolePostModel
        },
        HTTPMethod.PATCH: {
            PERMISSIONS: 'iam:update_role',
            VALIDATION: RolePatchModel
        },
        HTTPMethod.DELETE: {
            PERMISSIONS: 'iam:remove_role',
            VALIDATION: RoleDeleteModel
        }
    },
    '/roles/cache/': {
        HTTPMethod.DELETE: {
            PERMISSIONS: 'iam:remove_role_cache',  # todo system:
            VALIDATION: RoleCacheDeleteModel
        }
    },
    '/rules/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'rule:describe_rule',
        },
        HTTPMethod.DELETE: {
            PERMISSIONS: 'rule:remove_rule',
        }
    },
    '/rules/update-meta/': {
        HTTPMethod.POST: {
            PERMISSIONS: 'system:update_meta',
            VALIDATION: RuleUpdateMetaPostModel
        }
    },
    '/backup/': {
        HTTPMethod.POST: {
            PERMISSIONS: 'system:create_backup',
            VALIDATION: None
        }
    },
    '/metrics/update': {
        HTTPMethod.POST: {
            PERMISSIONS: 'system:update_metrics',
            VALIDATION: None
        }
    },
    '/metrics/status': {
        HTTPMethod.POST: {
            PERMISSIONS: 'system:metrics_status',
            VALIDATION: None
        }
    },
    '/rulesets/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'ruleset:describe_ruleset',
        },
        HTTPMethod.POST: {
            PERMISSIONS: 'ruleset:create_ruleset',
        },
        HTTPMethod.PATCH: {
            PERMISSIONS: 'ruleset:update_ruleset',
        },
        HTTPMethod.DELETE: {
            PERMISSIONS: 'ruleset:remove_ruleset',
        },
    },
    '/rulesets/content/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'ruleset:get_content',
        }
    },
    '/rulesets/event-driven/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'ruleset:describe_event_driven',
        },
        HTTPMethod.POST: {
            PERMISSIONS: 'ruleset:create_event_driven',
        },
        HTTPMethod.DELETE: {
            PERMISSIONS: 'ruleset:delete_event_driven',
        },
    },
    '/rule-sources/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'rule_source:describe_rule_source',
            VALIDATION: RuleSourceGetModel
        },
        HTTPMethod.POST: {
            PERMISSIONS: 'rule_source:create_rule_source',
            VALIDATION: RuleSourcePostModel
        },
        HTTPMethod.PATCH: {
            PERMISSIONS: 'rule_source:update_rule_source',
            VALIDATION: RuleSourcePatchModel
        },
        HTTPMethod.DELETE: {
            PERMISSIONS: 'rule_source:remove_rule_source',
            VALIDATION: RuleSourceDeleteModel
        }
    },
    '/accounts/credential_manager/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'account:describe_credential_manager',
            VALIDATION: CredentialsManagerGetModel
        },
        HTTPMethod.POST: {
            PERMISSIONS: 'account:create_credential_manager',
            VALIDATION: CredentialsManagerPostModel
        },
        HTTPMethod.PATCH: {
            PERMISSIONS: 'account:update_credential_manager',
            VALIDATION: CredentialsManagerPatchModel
        },
        HTTPMethod.DELETE: {
            PERMISSIONS: 'account:remove_credential_manager',
            VALIDATION: CredentialsManagerDeleteModel
        }
    },
    '/signup/': {
        HTTPMethod.POST: {
            PERMISSIONS: 'user:signup',
            VALIDATION: SignUpPostModel
        }
    },
    '/signin/': {
        HTTPMethod.POST: {
            PERMISSIONS: None,
            VALIDATION: SignInPostModel
        }
    },
    '/users/': {
        HTTPMethod.DELETE: {
            PERMISSIONS: 'user:delete_users',
            VALIDATION: UserDeleteModel
        }
    },
    '/users/password-reset/': {
        HTTPMethod.POST: {
            PERMISSIONS: 'user:reset_password',
            VALIDATION: UserPasswordResetPostModel
        }
    },
    '/users/customer/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'user:describe_customer',
            VALIDATION: UserCustomerGetModel
        },
        HTTPMethod.POST: {
            PERMISSIONS: 'user:assign_customer',
            VALIDATION: UserCustomerPostModel
        },
        HTTPMethod.PATCH: {
            PERMISSIONS: 'user:update_customer',
            VALIDATION: UserCustomerPatchModel
        },
        HTTPMethod.DELETE: {
            PERMISSIONS: 'user:unassign_customer',
            VALIDATION: UserCustomerDeleteModel
        }
    },
    '/users/role/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'user:describe_role',
            VALIDATION: UserRoleGetModel
        },
        HTTPMethod.POST: {
            PERMISSIONS: 'user:assign_role',
            VALIDATION: UserRolePostModel
        },
        HTTPMethod.PATCH: {
            PERMISSIONS: 'user:update_role',
            VALIDATION: UserRolePatchModel
        },
        HTTPMethod.DELETE: {
            PERMISSIONS: 'user:unassign_role',
            VALIDATION: UserRoleDeleteModel
        }
    },
    '/users/tenants/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'user:describe_tenants',
            VALIDATION: UserTenantsGetModel
        },
        HTTPMethod.PATCH: {
            PERMISSIONS: 'user:update_tenants',
            VALIDATION: UserTenantsPatchModel
        },
        HTTPMethod.DELETE: {
            PERMISSIONS: 'user:unassign_tenants',
            VALIDATION: UserTenantsDeleteModel
        }
    },
    '/event/': {
        HTTPMethod.POST: {
            PERMISSIONS: 'run:initiate_event_run',
            VALIDATION: EventPostModel
        }
    },
    '/license/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'license:describe_license',
            VALIDATION: LicenseGetModel
        },
        # HTTPMethod.POST: {
        #     PERMISSIONS: 'license:create_license',
        #     VALIDATION: LicensePostModel
        # },
        HTTPMethod.DELETE: {
            PERMISSIONS: 'license:remove_license',
            VALIDATION: LicenseDeleteModel
        }
    },
    '/license/sync/': {
        HTTPMethod.POST: {
            PERMISSIONS: 'license:create_license_sync',
            VALIDATION: LicenseSyncPostModel
        }
    },
    '/findings/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'findings:describe_findings',
            VALIDATION: FindingsGetModel
        },
        HTTPMethod.DELETE: {
            PERMISSIONS: 'findings:remove_findings',
            VALIDATION: FindingsDeleteModel
        }
    },
    '/scheduled-job/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'scheduled-job:describe',
            VALIDATION: ScheduledJobGetModel
        },
        HTTPMethod.POST: {
            PERMISSIONS: 'scheduled-job:register',
            VALIDATION: ScheduledJobPostModel
        },

    },
    '/scheduled-job/{name}/': {
        HTTPMethod.DELETE: {
            PERMISSIONS: 'scheduled-job:deregister',
            VALIDATION: ScheduledJobDeleteModel
        },
        HTTPMethod.PATCH: {
            PERMISSIONS: 'scheduled-job:update',
            VALIDATION: ScheduledJobPatchModel
        },
        HTTPMethod.GET: {
            PERMISSIONS: 'scheduled-job:describe',
            VALIDATION: SoloScheduledJobGetModel
        },
    },
    '/settings/mail/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'settings:describe_mail',
            VALIDATION: MailSettingGetModel
        },
        HTTPMethod.POST: {
            PERMISSIONS: 'settings:create_mail',
            VALIDATION: MailSettingPostModel
        },
        HTTPMethod.DELETE: {
            PERMISSIONS: 'settings:delete_mail'
        }
    },
    '/settings/license-manager/config/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'settings:describe_lm_config'
        },
        HTTPMethod.POST: {
            PERMISSIONS: 'settings:create_lm_config',
            VALIDATION: LicenseManagerConfigSettingPostModel
        },
        HTTPMethod.DELETE: {
            PERMISSIONS: 'settings:delete_lm_config'
        }
    },
    '/settings/license-manager/client/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'settings:describe_lm_client',
            VALIDATION: LicenseManagerClientSettingsGetModel
        },
        HTTPMethod.POST: {
            PERMISSIONS: 'settings:create_lm_client',
            VALIDATION: LicenseManagerClientSettingPostModel
        },
        HTTPMethod.DELETE: {
            PERMISSIONS: 'settings:delete_lm_client',
            VALIDATION: LicenseManagerClientSettingDeleteModel
        }
    },
    '/batch_results/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'batch_results:describe',
            VALIDATION: BatchResultsGetModel
        }
    },
    '/batch_results/{batch_results_id}/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'batch_results:describe',
            VALIDATION: SoleBatchResultsGetModel
        }
    },

    '/reports/digests/jobs/{id}/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'report_digests:describe',
            VALIDATION: JobReportGetModel
        }
    },
    '/reports/digests/tenants/jobs/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'report_digests:describe',
            VALIDATION: TimeRangedTenantsReportGetModel
        }
    },
    '/reports/digests/tenants/{tenant_name}/jobs/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'report_digests:describe',
            VALIDATION: TimeRangedTenantReportGetModel
        }
    },
    '/reports/digests/tenants/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'report_digests:describe',
            VALIDATION: TimeRangedTenantsReportGetModel
        }
    },
    '/reports/digests/tenants/{tenant_name}/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'report_digests:describe',
            VALIDATION: TimeRangedTenantReportGetModel
        }
    },
    '/reports/details/jobs/{id}/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'report_details:describe',
            VALIDATION: JobReportGetModel
        }
    },
    '/reports/details/tenants/jobs/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'report_details:describe',
            VALIDATION: TimeRangedTenantsReportGetModel
        }
    },
    '/reports/details/tenants/{tenant_name}/jobs/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'report_details:describe',
            VALIDATION: TimeRangedTenantReportGetModel
        }
    },
    '/reports/details/tenants/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'report_details:describe',
            VALIDATION: TimeRangedTenantsReportGetModel
        }
    },
    '/reports/details/tenants/{tenant_name}/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'report_details:describe',
            VALIDATION: TimeRangedTenantReportGetModel
        }
    },
    '/reports/compliance/jobs/{id}/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'report_compliance:describe',
            VALIDATION: JobComplianceReportGetModel
        }
    },
    '/reports/compliance/tenants/{tenant_name}/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'report_compliance:describe',
            VALIDATION: TenantComplianceReportGetModel
        }
    },
    '/reports/errors/jobs/{id}/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'report_errors:describe',
            VALIDATION: JobErrorReportGetModel
        }
    },
    '/reports/errors/access/jobs/{id}/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'report_errors:describe',
            VALIDATION: JobErrorReportGetModel
        }
    },
    '/reports/errors/core/jobs/{id}/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'report_errors:describe',
            VALIDATION: JobErrorReportGetModel
        }
    },

    '/reports/errors/tenants/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'report_errors:describe',
            VALIDATION: TenantsErrorReportGetModel
        }
    },
    '/reports/errors/access/tenants/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'report_errors:describe',
            VALIDATION: TenantsErrorReportGetModel
        }
    },
    '/reports/errors/core/tenants/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'report_errors:describe',
            VALIDATION: TenantsErrorReportGetModel
        }
    },

    '/reports/errors/tenants/{tenant_name}/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'report_errors:describe',
            VALIDATION: TenantErrorReportGetModel
        }
    },
    '/reports/errors/access/tenants/{tenant_name}/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'report_errors:describe',
            VALIDATION: TenantErrorReportGetModel
        }
    },
    '/reports/errors/core/tenants/{tenant_name}/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'report_errors:describe',
            VALIDATION: TenantErrorReportGetModel
        }
    },
    '/reports/rules/jobs/{id}/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'report_rules:describe',
            VALIDATION: JobRuleReportGetModel
        }
    },
    '/reports/rules/tenants/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'report_rules:describe',
            VALIDATION: TenantsRuleReportGetModel
        }
    },
    '/reports/rules/tenants/{tenant_name}/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'report_rules:describe',
            VALIDATION: TenantRuleReportGetModel
        }
    },
    '/applications/': {
        HTTPMethod.POST: {
            PERMISSIONS: 'application:activate',
            VALIDATION: ApplicationPostModel
        },
        HTTPMethod.GET: {
            PERMISSIONS: 'application:describe',
            VALIDATION: ApplicationListModel
        },
    },
    '/applications/{application_id}/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'application:describe',
            VALIDATION: ApplicationGetModel
        },
        HTTPMethod.PATCH: {
            PERMISSIONS: 'application:update',
            VALIDATION: ApplicationPatchModel
        },
        HTTPMethod.DELETE: {
            PERMISSIONS: 'application:delete',
            VALIDATION: ApplicationDeleteModel
        }
    },
    '/applications/access/{application_id}/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'access_application:describe',
            VALIDATION: AccessApplicationGetModel
        },
        HTTPMethod.PATCH: {
            PERMISSIONS: 'access_application:update',
            VALIDATION: AccessApplicationPatchModel
        },
        HTTPMethod.DELETE: {
            PERMISSIONS: 'access_application:delete',
            VALIDATION: AccessApplicationDeleteModel
        }
    },
    '/applications/access/': {
        HTTPMethod.POST: {
            PERMISSIONS: 'access_application:activate',
            VALIDATION: AccessApplicationPostModel
        },
        HTTPMethod.GET: {
            PERMISSIONS: 'access_application:describe',
            VALIDATION: AccessApplicationListModel
        },
    },
    '/applications/dojo/{application_id}/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'dojo_application:describe',
            VALIDATION: DojoApplicationGetModel
        },
        HTTPMethod.PATCH: {
            PERMISSIONS: 'dojo_application:update',
            VALIDATION: DojoApplicationPatchModel
        },
        HTTPMethod.DELETE: {
            PERMISSIONS: 'dojo_application:delete',
            VALIDATION: DojoApplicationDeleteModel
        }
    },
    '/applications/dojo/': {
        HTTPMethod.POST: {
            PERMISSIONS: 'dojo_application:activate',
            VALIDATION: DojoApplicationPostModel
        },
        HTTPMethod.GET: {
            PERMISSIONS: 'dojo_application:describe',
            VALIDATION: DojoApplicationListModel
        },
    },
    '/parents/': {
        HTTPMethod.POST: {
            PERMISSIONS: 'parent:activate',
        },
        HTTPMethod.GET: {
            PERMISSIONS: 'parent:describe',
            VALIDATION: ParentListModel
        }
    },
    '/parents/{parent_id}/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'parent:describe',
            VALIDATION: ParentGetModel
        },
        HTTPMethod.DELETE: {
            PERMISSIONS: 'parent:delete',
            VALIDATION: ParentDeleteModel
        },
        HTTPMethod.PATCH: {
            PERMISSIONS: 'parent:update',
        }
    },
    '/health/': {
        HTTPMethod.GET: {
            PERMISSIONS: None,
            VALIDATION: HealthCheckGetModel
        }
    },
    '/health/{id}/': {
        HTTPMethod.GET: {
            PERMISSIONS: None,
            VALIDATION: SoleHealthCheckGetModel
        }
    },
    '/customers/rabbitmq/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'rabbitmq:describe',
            VALIDATION: RabbitMQGetModel
        },
        HTTPMethod.POST: {
            PERMISSIONS: 'rabbitmq:create',
            VALIDATION: RabbitMQPostModel
        },
        HTTPMethod.DELETE: {
            PERMISSIONS: 'rabbitmq:delete',
            VALIDATION: RabbitMQDeleteModel
        }
    },
    '/rule-meta/standards/': {
        HTTPMethod.POST: {
            PERMISSIONS: 'meta:update_standards'
        }
    },
    '/rule-meta/mappings/': {
        HTTPMethod.POST: {
            PERMISSIONS: 'meta:update_mappings'
        }
    },
    '/rule-meta/meta/': {
        HTTPMethod.POST: {
            PERMISSIONS: 'meta:update_meta'
        }
    },
    '/reports/resources/tenants/{tenant_name}/state/latest/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'report_resources:get_latest',
            VALIDATION: ResourcesReportGet
        }
    },
    '/reports/resources/tenants/{tenant_name}/jobs/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'report_resources:get_jobs',
            VALIDATION: ResourceReportJobsGet
        }
    },
    '/reports/resources/jobs/{id}/': {
        HTTPMethod.GET: {
            PERMISSIONS: 'report_resources:get_jobs',
            VALIDATION: ResourceReportJobGet
        }
    },
    '/platforms/k8s/': {
        HTTPMethod.GET: {PERMISSIONS: 'platform:list_k8s'}
    },
    '/platforms/k8s/native/': {
        HTTPMethod.POST: {PERMISSIONS: 'platform:create_k8s_native'},
    },
    '/platforms/k8s/eks/': {
        HTTPMethod.POST: {PERMISSIONS: 'platform:create_k8s_eks'},
    },
    '/platforms/k8s/native/{id}/': {
        HTTPMethod.DELETE: {PERMISSIONS: 'platform:delete_k8s_native'}
    },
    '/platforms/k8s/eks/{id}/': {
        HTTPMethod.DELETE: {PERMISSIONS: 'platform:delete_k8s_eks'}
    },
    '/jobs/k8s/': {
        HTTPMethod.POST: {PERMISSIONS: 'run:initiate_k8s_run'}
    }
}
