from validators.request_validation import JobGetModel, SoloJobGetModel, \
    JobPostModel, JobDeleteModel, TenantGetModel, LicensePriorityGetModel, \
    LicensePriorityPostModel, LicensePriorityPatchModel, CustomerGetModel, \
    PolicyGetModel, PolicyPostModel, PolicyPatchModel, PolicyDeleteModel, \
    PolicyCacheDeleteModel, RoleGetModel, RolePostModel, RolePatchModel, \
    RoleDeleteModel, RoleCacheDeleteModel, RuleGetModel, RuleDeleteModel, \
    RuleUpdateMetaPostModel, RulesetGetModel, RulesetPostModel, \
    RulesetPatchModel, RulesetDeleteModel, RulesetContentGetModel, \
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
    MailSettingGetModel, MailSettingPostModel, LicensePriorityDeleteModel, \
    LicenseManagerConfigSettingPostModel, TenantErrorReportGetModel, \
    LicenseManagerClientSettingPostModel, JobRuleReportGetModel, \
    SoleBatchResultsGetModel, BatchResultsGetModel, CLevelGetReportModel, \
    JobReportGetModel, TimeRangedTenantsReportGetModel, \
    TimeRangedTenantReportGetModel, LicenseManagerClientSettingsGetModel, \
    JobErrorReportGetModel, TenantsErrorReportGetModel, \
    TenantsRuleReportGetModel, TenantRuleReportGetModel, \
    JobComplianceReportGetModel, TenantComplianceReportGetModel, \
    EventDrivenRulesetGetModel, EventDrivenRulesetDeleteModel, \
    EventDrivenRulesetPostModel, TenantPostModel, TenantRegionPostModel, \
    TenantPatchModel, OperationalGetReportModel, DepartmentGetReportModel, \
    ProjectGetReportModel, ApplicationPostModel, ApplicationGetModel, \
    ApplicationPatchModel,  ApplicationDeleteModel, \
    ParentPostModel, ParentGetModel, ParentListModel, ParentDeleteModel, \
    ParentPatchModel, HealthCheckGetModel, SoleHealthCheckGetModel, \
    ParentTenantLinkDeleteModel, ParentTenantLinkPostModel, \
    StandardJobPostModel, LicenseManagerClientSettingDeleteModel, \
    ApplicationListModel, RabbitMQGetModel, RabbitMQPostModel, \
    RabbitMQDeleteModel, AccessApplicationDeleteModel, \
    AccessApplicationGetModel, AccessApplicationListModel, \
    AccessApplicationPatchModel, AccessApplicationPostModel, \
    ReportPushByJobIdModel, ReportPushMultipleModel, \
    DojoApplicationDeleteModel, DojoApplicationGetModel, \
    DojoApplicationListModel, DojoApplicationPostModel, \
    DojoApplicationPatchModel


GET_METHOD = 'GET'
POST_METHOD = 'POST'
PATCH_METHOD = 'PATCH'
DELETE_METHOD = 'DELETE'

PERMISSIONS = 'permissions'
VALIDATION = 'validation'

ENDPOINT_PERMISSION_MAPPING = {
    '/reports/push/dojo/{job_id}/': {
        POST_METHOD: {
            PERMISSIONS: 'report:push_report_to_dojo',
            VALIDATION: ReportPushByJobIdModel
        }
    },
    '/reports/push/security-hub/{job_id}/': {
        POST_METHOD: {
            PERMISSIONS: 'report:push_report_to_security_hub',
            VALIDATION: ReportPushByJobIdModel
        }
    },
    '/reports/push/dojo/': {
        POST_METHOD: {
            PERMISSIONS: 'report:push_report_to_dojo',
            VALIDATION: ReportPushMultipleModel
        }
    },
    '/reports/push/security-hub/': {
        POST_METHOD: {
            PERMISSIONS: 'report:push_report_to_security_hub',
            VALIDATION: ReportPushMultipleModel
        }
    },
    '/reports/operational/': {
        GET_METHOD: {
            PERMISSIONS: 'report:operational',
            VALIDATION: OperationalGetReportModel
        }
    },
    '/reports/project/': {
        GET_METHOD: {
            PERMISSIONS: 'report:project',
            VALIDATION: ProjectGetReportModel
        }
    },
    '/reports/department/': {
        GET_METHOD: {
            PERMISSIONS: 'report:department',
            VALIDATION: DepartmentGetReportModel
        }
    },
    '/reports/clevel/': {
        GET_METHOD: {
            PERMISSIONS: 'report:clevel',
            VALIDATION: CLevelGetReportModel
        }
    },
    '/jobs/standard/': {
        POST_METHOD: {
            PERMISSIONS: 'run:initiate_standard_run',
            VALIDATION: StandardJobPostModel
        },
    },
    '/jobs/': {
        GET_METHOD: {
            PERMISSIONS: 'run:describe_job',
            VALIDATION: JobGetModel
        },
        POST_METHOD: {
            PERMISSIONS: 'run:initiate_run',
            VALIDATION: JobPostModel
        },
    },
    '/jobs/{job_id}/': {
        GET_METHOD: {
            PERMISSIONS: 'run:describe_job',
            VALIDATION: SoloJobGetModel
        },
        DELETE_METHOD: {
            PERMISSIONS: 'run:terminate_run',
            VALIDATION: JobDeleteModel
        }
    },
    '/customers/': {
        GET_METHOD: {
            PERMISSIONS: 'customer:describe_customer',
            VALIDATION: CustomerGetModel
        },
        # PATCH_METHOD: {
        #     PERMISSIONS: 'customer:update_customer',
        #     VALIDATION: CustomerPatchModel
        # },
    },
    '/tenants/': {
        GET_METHOD: {
            PERMISSIONS: 'tenant:describe_tenant',
            VALIDATION: TenantGetModel
        },
        POST_METHOD: {
            PERMISSIONS: 'tenant:activate_tenant',
            VALIDATION: TenantPostModel
        },
        PATCH_METHOD: {
            PERMISSIONS: 'tenant:update_tenant',
            VALIDATION: TenantPatchModel
        }
    },
    '/tenants/regions/': {
        POST_METHOD: {
            PERMISSIONS: 'tenant:activate_region',
            VALIDATION: TenantRegionPostModel
        }
    },
    '/tenants/license-priorities/': {
        GET_METHOD: {
            PERMISSIONS: 'tenant:describe_license_priority',
            VALIDATION: LicensePriorityGetModel
        },
        POST_METHOD: {
            PERMISSIONS: 'tenant:create_license_priority',
            VALIDATION: LicensePriorityPostModel
        },
        PATCH_METHOD: {
            PERMISSIONS: 'tenant:update_license_priority',
            VALIDATION: LicensePriorityPatchModel
        },
        DELETE_METHOD: {
            PERMISSIONS: 'tenant:remove_license_priority',
            VALIDATION: LicensePriorityDeleteModel
        }
    },
    '/policies/': {
        GET_METHOD: {
            PERMISSIONS: 'iam:describe_policy',
            VALIDATION: PolicyGetModel
        },
        POST_METHOD: {
            PERMISSIONS: 'iam:create_policy',
            VALIDATION: PolicyPostModel
        },
        PATCH_METHOD: {
            PERMISSIONS: 'iam:update_policy',
            VALIDATION: PolicyPatchModel
        },
        DELETE_METHOD: {
            PERMISSIONS: 'iam:remove_policy',
            VALIDATION: PolicyDeleteModel
        }
    },
    '/policies/cache/': {
        DELETE_METHOD: {
            PERMISSIONS: 'iam:remove_policy_cache',  # todo system:
            VALIDATION: PolicyCacheDeleteModel
        }
    },
    '/roles/': {
        GET_METHOD: {
            PERMISSIONS: 'iam:describe_role',
            VALIDATION: RoleGetModel
        },
        POST_METHOD: {
            PERMISSIONS: 'iam:create_role',
            VALIDATION: RolePostModel
        },
        PATCH_METHOD: {
            PERMISSIONS: 'iam:update_role',
            VALIDATION: RolePatchModel
        },
        DELETE_METHOD: {
            PERMISSIONS: 'iam:remove_role',
            VALIDATION: RoleDeleteModel
        }
    },
    '/roles/cache/': {
        DELETE_METHOD: {
            PERMISSIONS: 'iam:remove_role_cache',  # todo system:
            VALIDATION: RoleCacheDeleteModel
        }
    },
    '/rules/': {
        GET_METHOD: {
            PERMISSIONS: 'rule:describe_rule',
            VALIDATION: RuleGetModel
        },
        DELETE_METHOD: {
            PERMISSIONS: 'rule:remove_rule',
            VALIDATION: RuleDeleteModel
        }
    },
    '/rules/update-meta/': {
        POST_METHOD: {
            PERMISSIONS: 'system:update_meta',
            VALIDATION: RuleUpdateMetaPostModel
        }
    },
    '/backup/': {
        POST_METHOD: {
            PERMISSIONS: 'system:create_backup',
            VALIDATION: None
        }
    },
    '/metrics/update': {
        POST_METHOD: {
            PERMISSIONS: 'system:update_metrics',
            VALIDATION: None
        }
    },
    '/metrics/status': {
        POST_METHOD: {
            PERMISSIONS: 'system:metrics_status',
            VALIDATION: None
        }
    },
    '/rulesets/': {
        GET_METHOD: {
            PERMISSIONS: 'ruleset:describe_ruleset',
            VALIDATION: RulesetGetModel
        },
        POST_METHOD: {
            PERMISSIONS: 'ruleset:create_ruleset',
            VALIDATION: RulesetPostModel
        },
        PATCH_METHOD: {
            PERMISSIONS: 'ruleset:update_ruleset',
            VALIDATION: RulesetPatchModel
        },
        DELETE_METHOD: {
            PERMISSIONS: 'ruleset:remove_ruleset',
            VALIDATION: RulesetDeleteModel
        },
    },
    '/rulesets/content/': {
        GET_METHOD: {
            PERMISSIONS: 'ruleset:get_content',
            VALIDATION: RulesetContentGetModel
        }
    },
    '/rulesets/event-driven/': {
        GET_METHOD: {
            PERMISSIONS: 'ruleset:describe_event_driven',
            VALIDATION: EventDrivenRulesetGetModel
        },
        POST_METHOD: {
            PERMISSIONS: 'ruleset:create_event_driven',
            VALIDATION: EventDrivenRulesetPostModel
        },
        DELETE_METHOD: {
            PERMISSIONS: 'ruleset:delete_event_driven',
            VALIDATION: EventDrivenRulesetDeleteModel
        },
    },
    '/rule-sources/': {
        GET_METHOD: {
            PERMISSIONS: 'rule_source:describe_rule_source',
            VALIDATION: RuleSourceGetModel
        },
        POST_METHOD: {
            PERMISSIONS: 'rule_source:create_rule_source',
            VALIDATION: RuleSourcePostModel
        },
        PATCH_METHOD: {
            PERMISSIONS: 'rule_source:update_rule_source',
            VALIDATION: RuleSourcePatchModel
        },
        DELETE_METHOD: {
            PERMISSIONS: 'rule_source:remove_rule_source',
            VALIDATION: RuleSourceDeleteModel
        }
    },
    '/accounts/credential_manager/': {
        GET_METHOD: {
            PERMISSIONS: 'account:describe_credential_manager',
            VALIDATION: CredentialsManagerGetModel
        },
        POST_METHOD: {
            PERMISSIONS: 'account:create_credential_manager',
            VALIDATION: CredentialsManagerPostModel
        },
        PATCH_METHOD: {
            PERMISSIONS: 'account:update_credential_manager',
            VALIDATION: CredentialsManagerPatchModel
        },
        DELETE_METHOD: {
            PERMISSIONS: 'account:remove_credential_manager',
            VALIDATION: CredentialsManagerDeleteModel
        }
    },
    '/signup/': {
        POST_METHOD: {
            PERMISSIONS: 'user:signup',
            VALIDATION: SignUpPostModel
        }
    },
    '/signin/': {
        POST_METHOD: {
            PERMISSIONS: None,
            VALIDATION: SignInPostModel
        }
    },
    '/users/': {
        DELETE_METHOD: {
            PERMISSIONS: 'user:delete_users',
            VALIDATION: UserDeleteModel
        }
    },
    '/users/password-reset/': {
        POST_METHOD: {
            PERMISSIONS: 'user:reset_password',
            VALIDATION: UserPasswordResetPostModel
        }
    },
    '/users/customer/': {
        GET_METHOD: {
            PERMISSIONS: 'user:describe_customer',
            VALIDATION: UserCustomerGetModel
        },
        POST_METHOD: {
            PERMISSIONS: 'user:assign_customer',
            VALIDATION: UserCustomerPostModel
        },
        PATCH_METHOD: {
            PERMISSIONS: 'user:update_customer',
            VALIDATION: UserCustomerPatchModel
        },
        DELETE_METHOD: {
            PERMISSIONS: 'user:unassign_customer',
            VALIDATION: UserCustomerDeleteModel
        }
    },
    '/users/role/': {
        GET_METHOD: {
            PERMISSIONS: 'user:describe_role',
            VALIDATION: UserRoleGetModel
        },
        POST_METHOD: {
            PERMISSIONS: 'user:assign_role',
            VALIDATION: UserRolePostModel
        },
        PATCH_METHOD: {
            PERMISSIONS: 'user:update_role',
            VALIDATION: UserRolePatchModel
        },
        DELETE_METHOD: {
            PERMISSIONS: 'user:unassign_role',
            VALIDATION: UserRoleDeleteModel
        }
    },
    '/users/tenants/': {
        GET_METHOD: {
            PERMISSIONS: 'user:describe_tenants',
            VALIDATION: UserTenantsGetModel
        },
        PATCH_METHOD: {
            PERMISSIONS: 'user:update_tenants',
            VALIDATION: UserTenantsPatchModel
        },
        DELETE_METHOD: {
            PERMISSIONS: 'user:unassign_tenants',
            VALIDATION: UserTenantsDeleteModel
        }
    },
    '/event/': {
        POST_METHOD: {
            PERMISSIONS: 'run:initiate_event_run',
            VALIDATION: EventPostModel
        }
    },
    '/license/': {
        GET_METHOD: {
            PERMISSIONS: 'license:describe_license',
            VALIDATION: LicenseGetModel
        },
        # POST_METHOD: {
        #     PERMISSIONS: 'license:create_license',
        #     VALIDATION: LicensePostModel
        # },
        DELETE_METHOD: {
            PERMISSIONS: 'license:remove_license',
            VALIDATION: LicenseDeleteModel
        }
    },
    '/license/sync/': {
        POST_METHOD: {
            PERMISSIONS: 'license:create_license_sync',
            VALIDATION: LicenseSyncPostModel
        }
    },
    '/findings/': {
        GET_METHOD: {
            PERMISSIONS: 'findings:describe_findings',
            VALIDATION: FindingsGetModel
        },
        DELETE_METHOD: {
            PERMISSIONS: 'findings:remove_findings',
            VALIDATION: FindingsDeleteModel
        }
    },
    '/scheduled-job/': {
        GET_METHOD: {
            PERMISSIONS: 'scheduled-job:describe',
            VALIDATION: ScheduledJobGetModel
        },
        POST_METHOD: {
            PERMISSIONS: 'scheduled-job:register',
            VALIDATION: ScheduledJobPostModel
        },

    },
    '/scheduled-job/{name}/': {
        DELETE_METHOD: {
            PERMISSIONS: 'scheduled-job:deregister',
            VALIDATION: ScheduledJobDeleteModel
        },
        PATCH_METHOD: {
            PERMISSIONS: 'scheduled-job:update',
            VALIDATION: ScheduledJobPatchModel
        },
        GET_METHOD: {
            PERMISSIONS: 'scheduled-job:describe',
            VALIDATION: SoloScheduledJobGetModel
        },
    },
    '/settings/mail/': {
        GET_METHOD: {
            PERMISSIONS: 'settings:describe_mail',
            VALIDATION: MailSettingGetModel
        },
        POST_METHOD: {
            PERMISSIONS: 'settings:create_mail',
            VALIDATION: MailSettingPostModel
        },
        DELETE_METHOD: {
            PERMISSIONS: 'settings:delete_mail'
        }
    },
    '/settings/license-manager/config/': {
        GET_METHOD: {
            PERMISSIONS: 'settings:describe_lm_config'
        },
        POST_METHOD: {
            PERMISSIONS: 'settings:create_lm_config',
            VALIDATION: LicenseManagerConfigSettingPostModel
        },
        DELETE_METHOD: {
            PERMISSIONS: 'settings:delete_lm_config'
        }
    },
    '/settings/license-manager/client/': {
        GET_METHOD: {
            PERMISSIONS: 'settings:describe_lm_client',
            VALIDATION: LicenseManagerClientSettingsGetModel
        },
        POST_METHOD: {
            PERMISSIONS: 'settings:create_lm_client',
            VALIDATION: LicenseManagerClientSettingPostModel
        },
        DELETE_METHOD: {
            PERMISSIONS: 'settings:delete_lm_client',
            VALIDATION: LicenseManagerClientSettingDeleteModel
        }
    },
    '/batch_results/': {
        GET_METHOD: {
            PERMISSIONS: 'batch_results:describe',
            VALIDATION: BatchResultsGetModel
        }
    },
    '/batch_results/{batch_results_id}/': {
        GET_METHOD: {
            PERMISSIONS: 'batch_results:describe',
            VALIDATION: SoleBatchResultsGetModel
        }
    },

    '/reports/digests/jobs/{id}/': {
        GET_METHOD: {
            PERMISSIONS: 'report_digests:describe',
            VALIDATION: JobReportGetModel
        }
    },
    '/reports/digests/tenants/jobs/': {
        GET_METHOD: {
            PERMISSIONS: 'report_digests:describe',
            VALIDATION: TimeRangedTenantsReportGetModel
        }
    },
    '/reports/digests/tenants/{tenant_name}/jobs/': {
        GET_METHOD: {
            PERMISSIONS: 'report_digests:describe',
            VALIDATION: TimeRangedTenantReportGetModel
        }
    },
    '/reports/digests/tenants/': {
        GET_METHOD: {
            PERMISSIONS: 'report_digests:describe',
            VALIDATION: TimeRangedTenantsReportGetModel
        }
    },
    '/reports/digests/tenants/{tenant_name}/': {
        GET_METHOD: {
            PERMISSIONS: 'report_digests:describe',
            VALIDATION: TimeRangedTenantReportGetModel
        }
    },
    '/reports/details/jobs/{id}/': {
        GET_METHOD: {
            PERMISSIONS: 'report_details:describe',
            VALIDATION: JobReportGetModel
        }
    },
    '/reports/details/tenants/jobs/': {
        GET_METHOD: {
            PERMISSIONS: 'report_details:describe',
            VALIDATION: TimeRangedTenantsReportGetModel
        }
    },
    '/reports/details/tenants/{tenant_name}/jobs/': {
        GET_METHOD: {
            PERMISSIONS: 'report_details:describe',
            VALIDATION: TimeRangedTenantReportGetModel
        }
    },
    '/reports/details/tenants/': {
        GET_METHOD: {
            PERMISSIONS: 'report_details:describe',
            VALIDATION: TimeRangedTenantsReportGetModel
        }
    },
    '/reports/details/tenants/{tenant_name}/': {
        GET_METHOD: {
            PERMISSIONS: 'report_details:describe',
            VALIDATION: TimeRangedTenantReportGetModel
        }
    },
    '/reports/compliance/jobs/{id}/': {
        GET_METHOD: {
            PERMISSIONS: 'report_compliance:describe',
            VALIDATION: JobComplianceReportGetModel
        }
    },
    '/reports/compliance/tenants/{tenant_name}/': {
        GET_METHOD: {
            PERMISSIONS: 'report_compliance:describe',
            VALIDATION: TenantComplianceReportGetModel
        }
    },
    '/reports/errors/jobs/{id}/': {
        GET_METHOD: {
            PERMISSIONS: 'report_errors:describe',
            VALIDATION: JobErrorReportGetModel
        }
    },
    '/reports/errors/access/jobs/{id}/': {
        GET_METHOD: {
            PERMISSIONS: 'report_errors:describe',
            VALIDATION: JobErrorReportGetModel
        }
    },
    '/reports/errors/core/jobs/{id}/': {
        GET_METHOD: {
            PERMISSIONS: 'report_errors:describe',
            VALIDATION: JobErrorReportGetModel
        }
    },

    '/reports/errors/tenants/': {
        GET_METHOD: {
            PERMISSIONS: 'report_errors:describe',
            VALIDATION: TenantsErrorReportGetModel
        }
    },
    '/reports/errors/access/tenants/': {
        GET_METHOD: {
            PERMISSIONS: 'report_errors:describe',
            VALIDATION: TenantsErrorReportGetModel
        }
    },
    '/reports/errors/core/tenants/': {
        GET_METHOD: {
            PERMISSIONS: 'report_errors:describe',
            VALIDATION: TenantsErrorReportGetModel
        }
    },

    '/reports/errors/tenants/{tenant_name}/': {
        GET_METHOD: {
            PERMISSIONS: 'report_errors:describe',
            VALIDATION: TenantErrorReportGetModel
        }
    },
    '/reports/errors/access/tenants/{tenant_name}/': {
        GET_METHOD: {
            PERMISSIONS: 'report_errors:describe',
            VALIDATION: TenantErrorReportGetModel
        }
    },
    '/reports/errors/core/tenants/{tenant_name}/': {
        GET_METHOD: {
            PERMISSIONS: 'report_errors:describe',
            VALIDATION: TenantErrorReportGetModel
        }
    },
    '/reports/rules/jobs/{id}/': {
        GET_METHOD: {
            PERMISSIONS: 'report_rules:describe',
            VALIDATION: JobRuleReportGetModel
        }
    },
    '/reports/rules/tenants/': {
        GET_METHOD: {
            PERMISSIONS: 'report_rules:describe',
            VALIDATION: TenantsRuleReportGetModel
        }
    },
    '/reports/rules/tenants/{tenant_name}/': {
        GET_METHOD: {
            PERMISSIONS: 'report_rules:describe',
            VALIDATION: TenantRuleReportGetModel
        }
    },
    '/applications/': {
        POST_METHOD: {
            PERMISSIONS: 'application:activate',
            VALIDATION: ApplicationPostModel
        },
        GET_METHOD: {
            PERMISSIONS: 'application:describe',
            VALIDATION: ApplicationListModel
        },
    },
    '/applications/{application_id}/': {
        GET_METHOD: {
            PERMISSIONS: 'application:describe',
            VALIDATION: ApplicationGetModel
        },
        PATCH_METHOD: {
            PERMISSIONS: 'application:update',
            VALIDATION: ApplicationPatchModel
        },
        DELETE_METHOD: {
            PERMISSIONS: 'application:delete',
            VALIDATION: ApplicationDeleteModel
        }
    },
    '/applications/access/{application_id}/': {
        GET_METHOD: {
            PERMISSIONS: 'access_application:describe',
            VALIDATION: AccessApplicationGetModel
        },
        PATCH_METHOD: {
            PERMISSIONS: 'access_application:update',
            VALIDATION: AccessApplicationPatchModel
        },
        DELETE_METHOD: {
            PERMISSIONS: 'access_application:delete',
            VALIDATION: AccessApplicationDeleteModel
        }
    },
    '/applications/access/': {
        POST_METHOD: {
            PERMISSIONS: 'access_application:activate',
            VALIDATION: AccessApplicationPostModel
        },
        GET_METHOD: {
            PERMISSIONS: 'access_application:describe',
            VALIDATION: AccessApplicationListModel
        },
    },
    '/applications/dojo/{application_id}/': {
        GET_METHOD: {
            PERMISSIONS: 'dojo_application:describe',
            VALIDATION: DojoApplicationGetModel
        },
        PATCH_METHOD: {
            PERMISSIONS: 'dojo_application:update',
            VALIDATION: DojoApplicationPatchModel
        },
        DELETE_METHOD: {
            PERMISSIONS: 'dojo_application:delete',
            VALIDATION: DojoApplicationDeleteModel
        }
    },
    '/applications/dojo/': {
        POST_METHOD: {
            PERMISSIONS: 'dojo_application:activate',
            VALIDATION: DojoApplicationPostModel
        },
        GET_METHOD: {
            PERMISSIONS: 'dojo_application:describe',
            VALIDATION: DojoApplicationListModel
        },
    },
    '/parents/': {
        POST_METHOD: {
            PERMISSIONS: 'parent:activate',
            VALIDATION: ParentPostModel
        },
        GET_METHOD: {
            PERMISSIONS: 'parent:describe',
            VALIDATION: ParentListModel
        }
    },
    '/parents/{parent_id}/': {
        GET_METHOD: {
            PERMISSIONS: 'parent:describe',
            VALIDATION: ParentGetModel
        },
        DELETE_METHOD: {
            PERMISSIONS: 'parent:delete',
            VALIDATION: ParentDeleteModel
        },
        PATCH_METHOD: {
            PERMISSIONS: 'parent:update',
            VALIDATION: ParentPatchModel
        }
    },
    '/parents/tenant-link/': {
        POST_METHOD: {
            PERMISSIONS: 'tenant:link_parent',
            VALIDATION: ParentTenantLinkPostModel
        },
        DELETE_METHOD: {
            PERMISSIONS: 'tenant:unlink_parent',
            VALIDATION: ParentTenantLinkDeleteModel
        },
    },
    '/health/': {
        GET_METHOD: {
            PERMISSIONS: None,
            VALIDATION: HealthCheckGetModel
        }
    },
    '/health/{id}/': {
        GET_METHOD: {
            PERMISSIONS: None,
            VALIDATION: SoleHealthCheckGetModel
        }
    },
    '/customers/rabbitmq/': {
        GET_METHOD: {
            PERMISSIONS: 'rabbitmq:describe',
            VALIDATION: RabbitMQGetModel
        },
        POST_METHOD: {
            PERMISSIONS: 'rabbitmq:create',
            VALIDATION: RabbitMQPostModel
        },
        DELETE_METHOD: {
            PERMISSIONS: 'rabbitmq:delete',
            VALIDATION: RabbitMQDeleteModel
        }
    }
}
