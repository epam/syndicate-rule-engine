# Generated controls — do not edit manually


control "ecc_k8s_001_apiserver_anonymous_auth_argument_is_set_to_false" {
  title       = "ecc-k8s-001-apiserver_anonymous_auth_argument_is_set_to_false"
  description = "When enabled, requests that are not rejected by other configured authentication methods are treated as anonymous requests. These requests are then served by the API server. You should rely on authentication to authorize access and disallow anonymous requests."
  query       = query.ecc_k8s_001_apiserver_anonymous_auth_argument_is_set_to_false

  tags = merge(local.common_tags, {
    cis_item_id = "1.2.1"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_001_apiserver_anonymous_auth_argument_is_set_to_false" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-001-apiserver_anonymous_auth_argument_is_set_to_false') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-001-apiserver_anonymous_auth_argument_is_set_to_false'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-001-apiserver_anonymous_auth_argument_is_set_to_false'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-001-apiserver_anonymous_auth_argument_is_set_to_false'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-001-apiserver_anonymous_auth_argument_is_set_to_false'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_002_apiserver_token_auth_file_parameter_is_not_set" {
  title       = "ecc-k8s-002-apiserver_token_auth_file_parameter_is_not_set"
  description = "The token-based authentication utilizes static tokens to authenticate requests to the apiserver. The tokens are stored in clear-text in a file on the apiserver, and cannot be revoked or rotated without restarting the apiserver. Hence, do not use static token-based authentication."
  query       = query.ecc_k8s_002_apiserver_token_auth_file_parameter_is_not_set

  tags = merge(local.common_tags, {
    cis_item_id = "1.2.2"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_002_apiserver_token_auth_file_parameter_is_not_set" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-002-apiserver_token_auth_file_parameter_is_not_set') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-002-apiserver_token_auth_file_parameter_is_not_set'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-002-apiserver_token_auth_file_parameter_is_not_set'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-002-apiserver_token_auth_file_parameter_is_not_set'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-002-apiserver_token_auth_file_parameter_is_not_set'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_003_apiserver_admission_control_plugin_denyserviceexternalips_is_set" {
  title       = "ecc-k8s-003-apiserver_admission_control_plugin_denyserviceexternalips_is_set"
  description = "This admission controller rejects all net-new usage of the 'Service' field 'externalIPs' and mitigates a known security vulnerability CVE-2020-8554. This feature is very powerful (allows network traffic interception) and not well controlled by policy. When enabled, users of the cluster may not create new Services which use 'externalIPs' and may not add new values to 'externalIPs' on existing 'Service' objects. Existing uses of 'externalIPs' are not affected, and users may remove values from 'externalIPs' on existing 'Service' objects."
  query       = query.ecc_k8s_003_apiserver_admission_control_plugin_denyserviceexternalips_is_set

  tags = local.common_tags
}

query "ecc_k8s_003_apiserver_admission_control_plugin_denyserviceexternalips_is_set" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-003-apiserver_admission_control_plugin_denyserviceexternalips_is_set') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-003-apiserver_admission_control_plugin_denyserviceexternalips_is_set'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-003-apiserver_admission_control_plugin_denyserviceexternalips_is_set'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-003-apiserver_admission_control_plugin_denyserviceexternalips_is_set'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-003-apiserver_admission_control_plugin_denyserviceexternalips_is_set'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_004_apiserver_kubelet_client_certificate_and_kubelet_client_key_arguments_are_set" {
  title       = "ecc-k8s-004-apiserver_kubelet_client_certificate_and_kubelet_client_key_arguments_are_set"
  description = "The apiserver, by default, does not authenticate itself to the kubelet's HTTPS endpoints. The requests from the apiserver are treated anonymously. You should set up certificate-based kubelet authentication to ensure that the apiserver authenticates itself to kubelets when submitting requests."
  query       = query.ecc_k8s_004_apiserver_kubelet_client_certificate_and_kubelet_client_key_arguments_are_set

  tags = merge(local.common_tags, {
    cis_item_id = "1.2.4"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_004_apiserver_kubelet_client_certificate_and_kubelet_client_key_arguments_are_set" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-004-apiserver_kubelet_client_certificate_and_kubelet_client_key_arguments_are_set') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-004-apiserver_kubelet_client_certificate_and_kubelet_client_key_arguments_are_set'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-004-apiserver_kubelet_client_certificate_and_kubelet_client_key_arguments_are_set'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-004-apiserver_kubelet_client_certificate_and_kubelet_client_key_arguments_are_set'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-004-apiserver_kubelet_client_certificate_and_kubelet_client_key_arguments_are_set'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_005_apiserver_kubelet_certificate_authority_argument_is_set" {
  title       = "ecc-k8s-005-apiserver_kubelet_certificate_authority_argument_is_set"
  description = "The connections from the API server to the kubelet are used for:"
  query       = query.ecc_k8s_005_apiserver_kubelet_certificate_authority_argument_is_set

  tags = merge(local.common_tags, {
    cis_item_id = "1.2.5"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_005_apiserver_kubelet_certificate_authority_argument_is_set" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-005-apiserver_kubelet_certificate_authority_argument_is_set') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-005-apiserver_kubelet_certificate_authority_argument_is_set'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-005-apiserver_kubelet_certificate_authority_argument_is_set'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-005-apiserver_kubelet_certificate_authority_argument_is_set'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-005-apiserver_kubelet_certificate_authority_argument_is_set'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_006_apiserver_authorization_mode_argument_is_not_set_to_alwaysallow" {
  title       = "ecc-k8s-006-apiserver_authorization_mode_argument_is_not_set_to_alwaysallow"
  description = "Any request that is successfully authenticated (including an anonymous request) is then authorized. This means that every authenticated request to the Kubernetes API will be successfully authorized if authorization mode is set to 'AlwaysAllow'. This mode should not be used on any production cluster."
  query       = query.ecc_k8s_006_apiserver_authorization_mode_argument_is_not_set_to_alwaysallow

  tags = merge(local.common_tags, {
    cis_item_id = "1.2.6"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_006_apiserver_authorization_mode_argument_is_not_set_to_alwaysallow" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-006-apiserver_authorization_mode_argument_is_not_set_to_alwaysallow') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-006-apiserver_authorization_mode_argument_is_not_set_to_alwaysallow'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-006-apiserver_authorization_mode_argument_is_not_set_to_alwaysallow'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-006-apiserver_authorization_mode_argument_is_not_set_to_alwaysallow'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-006-apiserver_authorization_mode_argument_is_not_set_to_alwaysallow'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_007_apiserver_authorization_mode_argument_includes_node" {
  title       = "ecc-k8s-007-apiserver_authorization_mode_argument_includes_node"
  description = "The Node authorization mode only allows kubelets to read Secret, ConfigMap, PersistentVolume, and PersistentVolumeClaim objects associated with their nodes."
  query       = query.ecc_k8s_007_apiserver_authorization_mode_argument_includes_node

  tags = merge(local.common_tags, {
    cis_item_id = "1.2.7"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_007_apiserver_authorization_mode_argument_includes_node" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-007-apiserver_authorization_mode_argument_includes_node') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-007-apiserver_authorization_mode_argument_includes_node'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-007-apiserver_authorization_mode_argument_includes_node'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-007-apiserver_authorization_mode_argument_includes_node'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-007-apiserver_authorization_mode_argument_includes_node'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_008_apiserver_authorization_mode_argument_includes_rbac" {
  title       = "ecc-k8s-008-apiserver_authorization_mode_argument_includes_rbac"
  description = "RBAC is a powerful and flexible authorization mechanism that allows cluster administrators to define granular access policies that control who can access and perform actions on Kubernetes resources."
  query       = query.ecc_k8s_008_apiserver_authorization_mode_argument_includes_rbac

  tags = merge(local.common_tags, {
    cis_item_id = "1.2.8"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_008_apiserver_authorization_mode_argument_includes_rbac" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-008-apiserver_authorization_mode_argument_includes_rbac') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-008-apiserver_authorization_mode_argument_includes_rbac'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-008-apiserver_authorization_mode_argument_includes_rbac'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-008-apiserver_authorization_mode_argument_includes_rbac'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-008-apiserver_authorization_mode_argument_includes_rbac'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_009_apiserver_admission_control_plugin_eventratelimit_is_set" {
  title       = "ecc-k8s-009-apiserver_admission_control_plugin_eventratelimit_is_set"
  description = "Using 'EventRateLimit' admission control enforces a limit on the number of events that the API Server will accept in a given time slice. A misbehaving workload could overwhelm and DoS the API Server, making it unavailable. This particularly applies to a multi-tenant cluster, where there might be a small percentage of misbehaving tenants which could have a significant impact on the performance of the cluster overall. Hence, it is recommended to limit the rate of events that the API server will accept."
  query       = query.ecc_k8s_009_apiserver_admission_control_plugin_eventratelimit_is_set

  tags = merge(local.common_tags, {
    cis_item_id = "1.2.9"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_009_apiserver_admission_control_plugin_eventratelimit_is_set" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-009-apiserver_admission_control_plugin_eventratelimit_is_set') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-009-apiserver_admission_control_plugin_eventratelimit_is_set'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-009-apiserver_admission_control_plugin_eventratelimit_is_set'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-009-apiserver_admission_control_plugin_eventratelimit_is_set'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-009-apiserver_admission_control_plugin_eventratelimit_is_set'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_010_apiserver_admission_control_plugin_alwaysadmit_is_not_set" {
  title       = "ecc-k8s-010-apiserver_admission_control_plugin_alwaysadmit_is_not_set"
  description = "Setting admission control plugin AlwaysAdmit allows all requests and do not filter any requests. The AlwaysAdmit admission controller was deprecated in Kubernetes v1.13. Its behavior was equivalent to turning off all admission controllers."
  query       = query.ecc_k8s_010_apiserver_admission_control_plugin_alwaysadmit_is_not_set

  tags = merge(local.common_tags, {
    cis_item_id = "1.2.10"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_010_apiserver_admission_control_plugin_alwaysadmit_is_not_set" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-010-apiserver_admission_control_plugin_alwaysadmit_is_not_set') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-010-apiserver_admission_control_plugin_alwaysadmit_is_not_set'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-010-apiserver_admission_control_plugin_alwaysadmit_is_not_set'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-010-apiserver_admission_control_plugin_alwaysadmit_is_not_set'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-010-apiserver_admission_control_plugin_alwaysadmit_is_not_set'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_011_apiserver_admission_control_plugin_alwayspullimages_is_set" {
  title       = "ecc-k8s-011-apiserver_admission_control_plugin_alwayspullimages_is_set"
  description = "Setting admission control policy to AlwaysPullImages forces every new pod to pull the required images every time. In a multi-tenant cluster users can be assured that their private images can only be used by those who have the credentials to pull them. Without this admission control policy, once an image has been pulled to a node, any pod from any user can use it simply by knowing the image`s name, without any authorization check against the image ownership. When this plug-in is enabled, images are always pulled prior to starting containers, which means valid credentials are required."
  query       = query.ecc_k8s_011_apiserver_admission_control_plugin_alwayspullimages_is_set

  tags = merge(local.common_tags, {
    cis_item_id = "1.2.11"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_011_apiserver_admission_control_plugin_alwayspullimages_is_set" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-011-apiserver_admission_control_plugin_alwayspullimages_is_set') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-011-apiserver_admission_control_plugin_alwayspullimages_is_set'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-011-apiserver_admission_control_plugin_alwayspullimages_is_set'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-011-apiserver_admission_control_plugin_alwayspullimages_is_set'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-011-apiserver_admission_control_plugin_alwayspullimages_is_set'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_012_apiserver_admission_control_plugin_securitycontextdeny_is_set" {
  title       = "ecc-k8s-012-apiserver_admission_control_plugin_securitycontextdeny_is_set"
  description = "The SecurityContextDeny admission controller can be used to deny pods which make use of some SecurityContext fields which could allow for privilege escalation in the cluster. This should be used where  PodSecurityPolicy is not in place within the cluster."
  query       = query.ecc_k8s_012_apiserver_admission_control_plugin_securitycontextdeny_is_set

  tags = merge(local.common_tags, {
    cis_item_id = "1.2.12"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_012_apiserver_admission_control_plugin_securitycontextdeny_is_set" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-012-apiserver_admission_control_plugin_securitycontextdeny_is_set') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-012-apiserver_admission_control_plugin_securitycontextdeny_is_set'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-012-apiserver_admission_control_plugin_securitycontextdeny_is_set'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-012-apiserver_admission_control_plugin_securitycontextdeny_is_set'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-012-apiserver_admission_control_plugin_securitycontextdeny_is_set'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_013_apiserver_admission_control_plugin_serviceaccount_is_set" {
  title       = "ecc-k8s-013-apiserver_admission_control_plugin_serviceaccount_is_set"
  description = "When you create a pod, if you do not specify a service account, it is automatically assigned the default service account in the same namespace. You should create your own service account and let the API server manage its security tokens."
  query       = query.ecc_k8s_013_apiserver_admission_control_plugin_serviceaccount_is_set

  tags = merge(local.common_tags, {
    cis_item_id = "1.2.13"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_013_apiserver_admission_control_plugin_serviceaccount_is_set" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-013-apiserver_admission_control_plugin_serviceaccount_is_set') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-013-apiserver_admission_control_plugin_serviceaccount_is_set'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-013-apiserver_admission_control_plugin_serviceaccount_is_set'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-013-apiserver_admission_control_plugin_serviceaccount_is_set'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-013-apiserver_admission_control_plugin_serviceaccount_is_set'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_014_apiserver_admission_control_plugin_namespacelifecycle_is_set" {
  title       = "ecc-k8s-014-apiserver_admission_control_plugin_namespacelifecycle_is_set"
  description = "Setting admission control policy to NamespaceLifecycle ensures that objects cannot be created in non-existent namespaces, and that namespaces undergoing termination are not used for creating the new objects. This is recommended to enforce the integrity of the namespace termination process and also for the availability of the newer objects."
  query       = query.ecc_k8s_014_apiserver_admission_control_plugin_namespacelifecycle_is_set

  tags = merge(local.common_tags, {
    cis_item_id = "1.2.14"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_014_apiserver_admission_control_plugin_namespacelifecycle_is_set" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-014-apiserver_admission_control_plugin_namespacelifecycle_is_set') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-014-apiserver_admission_control_plugin_namespacelifecycle_is_set'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-014-apiserver_admission_control_plugin_namespacelifecycle_is_set'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-014-apiserver_admission_control_plugin_namespacelifecycle_is_set'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-014-apiserver_admission_control_plugin_namespacelifecycle_is_set'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_015_apiserver_admission_control_plugin_noderestriction_is_set" {
  title       = "ecc-k8s-015-apiserver_admission_control_plugin_noderestriction_is_set"
  description = "Using the NodeRestriction plug-in ensures that the kubelet is restricted to the Node and Pod objects that it could modify as defined. Such kubelets will only be allowed to modify their own Node API object, and only modify Pod API objects that are bound to their node."
  query       = query.ecc_k8s_015_apiserver_admission_control_plugin_noderestriction_is_set

  tags = merge(local.common_tags, {
    cis_item_id = "1.2.15"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_015_apiserver_admission_control_plugin_noderestriction_is_set" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-015-apiserver_admission_control_plugin_noderestriction_is_set') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-015-apiserver_admission_control_plugin_noderestriction_is_set'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-015-apiserver_admission_control_plugin_noderestriction_is_set'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-015-apiserver_admission_control_plugin_noderestriction_is_set'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-015-apiserver_admission_control_plugin_noderestriction_is_set'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_016_apiserver_profiling_argument_is_set_to_false" {
  title       = "ecc-k8s-016-apiserver_profiling_argument_is_set_to_false"
  description = "Profiling allows for the identification of specific performance bottlenecks. It generates a significant amount of program data that could potentially be exploited to uncover system and program details. If you are not experiencing any bottlenecks and do not need the profiler for troubleshooting purposes, it is recommended to turn it off to reduce the potential attack surface."
  query       = query.ecc_k8s_016_apiserver_profiling_argument_is_set_to_false

  tags = merge(local.common_tags, {
    cis_item_id = "1.2.17"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_016_apiserver_profiling_argument_is_set_to_false" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-016-apiserver_profiling_argument_is_set_to_false') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-016-apiserver_profiling_argument_is_set_to_false'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-016-apiserver_profiling_argument_is_set_to_false'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-016-apiserver_profiling_argument_is_set_to_false'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-016-apiserver_profiling_argument_is_set_to_false'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_017_apiserver_audit_log_path_argument_is_set" {
  title       = "ecc-k8s-017-apiserver_audit_log_path_argument_is_set"
  description = "Auditing the Kubernetes API Server provides a security-relevant chronological set of records documenting the sequence of activities that have affected system by individual users, administrators or other components of the system. Even though currently, Kubernetes provides only basic audit capabilities, it should be enabled. You can enable it by setting an appropriate audit log path."
  query       = query.ecc_k8s_017_apiserver_audit_log_path_argument_is_set

  tags = merge(local.common_tags, {
    cis_item_id = "1.2.18"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_017_apiserver_audit_log_path_argument_is_set" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-017-apiserver_audit_log_path_argument_is_set') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-017-apiserver_audit_log_path_argument_is_set'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-017-apiserver_audit_log_path_argument_is_set'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-017-apiserver_audit_log_path_argument_is_set'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-017-apiserver_audit_log_path_argument_is_set'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_018_apiserver_audit_log_maxage_argument_is_set_to_30" {
  title       = "ecc-k8s-018-apiserver_audit_log_maxage_argument_is_set_to_30"
  description = "Retaining logs for at least 30 days ensures that you can go back in time and investigate or correlate any events."
  query       = query.ecc_k8s_018_apiserver_audit_log_maxage_argument_is_set_to_30

  tags = merge(local.common_tags, {
    cis_item_id = "1.2.19"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_018_apiserver_audit_log_maxage_argument_is_set_to_30" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-018-apiserver_audit_log_maxage_argument_is_set_to_30') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-018-apiserver_audit_log_maxage_argument_is_set_to_30'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-018-apiserver_audit_log_maxage_argument_is_set_to_30'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-018-apiserver_audit_log_maxage_argument_is_set_to_30'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-018-apiserver_audit_log_maxage_argument_is_set_to_30'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_019_apiserver_audit_log_maxbackup_argument_is_set_to_10" {
  title       = "ecc-k8s-019-apiserver_audit_log_maxbackup_argument_is_set_to_10"
  description = "Retaining old log files ensures that one would have sufficient log data available for carrying out any investigation or correlation."
  query       = query.ecc_k8s_019_apiserver_audit_log_maxbackup_argument_is_set_to_10

  tags = merge(local.common_tags, {
    cis_item_id = "1.2.20"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_019_apiserver_audit_log_maxbackup_argument_is_set_to_10" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-019-apiserver_audit_log_maxbackup_argument_is_set_to_10') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-019-apiserver_audit_log_maxbackup_argument_is_set_to_10'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-019-apiserver_audit_log_maxbackup_argument_is_set_to_10'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-019-apiserver_audit_log_maxbackup_argument_is_set_to_10'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-019-apiserver_audit_log_maxbackup_argument_is_set_to_10'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_020_apiserver_audit_log_maxsize_argument_is_set_to_100" {
  title       = "ecc-k8s-020-apiserver_audit_log_maxsize_argument_is_set_to_100"
  description = "The --audit-log-maxsize flag is a configuration option for the Kubernetes API server that specifies the maximum size in megabytes of each audit log file before it gets rotated. When the audit log file reaches the specified maximum size, it is renamed with a timestamp and a new file is created."
  query       = query.ecc_k8s_020_apiserver_audit_log_maxsize_argument_is_set_to_100

  tags = merge(local.common_tags, {
    cis_item_id = "1.2.21"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_020_apiserver_audit_log_maxsize_argument_is_set_to_100" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-020-apiserver_audit_log_maxsize_argument_is_set_to_100') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-020-apiserver_audit_log_maxsize_argument_is_set_to_100'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-020-apiserver_audit_log_maxsize_argument_is_set_to_100'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-020-apiserver_audit_log_maxsize_argument_is_set_to_100'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-020-apiserver_audit_log_maxsize_argument_is_set_to_100'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_021_apiserver_request_timeout_argument_is_set_as_appropriate" {
  title       = "ecc-k8s-021-apiserver_request_timeout_argument_is_set_as_appropriate"
  description = "Setting global request timeout allows extending the API server request timeout limit to a duration appropriate to the user's connection speed. By default, it is set to 60 seconds which might be problematic on slower connections making cluster resources inaccessible once the data volume for requests exceeds what can be transmitted in 60 seconds. But, setting this timeout limit to be too large can exhaust the API server resources making it prone to Denial-of-Service attack. Hence, it is recommended to set this limit as appropriate and change the default limit of 60 seconds only if needed."
  query       = query.ecc_k8s_021_apiserver_request_timeout_argument_is_set_as_appropriate

  tags = merge(local.common_tags, {
    cis_item_id = "1.2.22"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_021_apiserver_request_timeout_argument_is_set_as_appropriate" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-021-apiserver_request_timeout_argument_is_set_as_appropriate') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-021-apiserver_request_timeout_argument_is_set_as_appropriate'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-021-apiserver_request_timeout_argument_is_set_as_appropriate'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-021-apiserver_request_timeout_argument_is_set_as_appropriate'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-021-apiserver_request_timeout_argument_is_set_as_appropriate'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_022_apiserver_service_account_lookup_argument_is_set_to_true" {
  title       = "ecc-k8s-022-apiserver_service_account_lookup_argument_is_set_to_true"
  description = "If Service account lookup is not enabled, the apiserver only verifies that the authentication token is valid, and does not validate that the service account token mentioned in the request is actually present in etcd. This allows using a service account token even after the corresponding service account is deleted."
  query       = query.ecc_k8s_022_apiserver_service_account_lookup_argument_is_set_to_true

  tags = merge(local.common_tags, {
    cis_item_id = "1.2.23"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_022_apiserver_service_account_lookup_argument_is_set_to_true" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-022-apiserver_service_account_lookup_argument_is_set_to_true') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-022-apiserver_service_account_lookup_argument_is_set_to_true'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-022-apiserver_service_account_lookup_argument_is_set_to_true'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-022-apiserver_service_account_lookup_argument_is_set_to_true'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-022-apiserver_service_account_lookup_argument_is_set_to_true'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_023_apiserver_service_account_key_file_argument_is_set" {
  title       = "ecc-k8s-023-apiserver_service_account_key_file_argument_is_set"
  description = "By default, if no --service-account-key-file is specified to the apiserver, it uses the private key from the TLS serving certificate to verify service account tokens. To ensure that the keys for service account tokens could be rotated as needed, a separate public/private key pair should be used for signing service account tokens. Hence, the public key should be specified to the apiserver with --service-account-key-file."
  query       = query.ecc_k8s_023_apiserver_service_account_key_file_argument_is_set

  tags = merge(local.common_tags, {
    cis_item_id = "1.2.24"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_023_apiserver_service_account_key_file_argument_is_set" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-023-apiserver_service_account_key_file_argument_is_set') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-023-apiserver_service_account_key_file_argument_is_set'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-023-apiserver_service_account_key_file_argument_is_set'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-023-apiserver_service_account_key_file_argument_is_set'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-023-apiserver_service_account_key_file_argument_is_set'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_024_apiserver_etcd_certfile_and_etcd_keyfile_arguments_are_set" {
  title       = "ecc-k8s-024-apiserver_etcd_certfile_and_etcd_keyfile_arguments_are_set"
  description = "etcd is a highly-available key value store used by Kubernetes deployments for persistent storage of all of its REST API objects. These objects are sensitive in nature and should be protected by client authentication. This requires the API server to identify itself to the etcd server using a client certificate and key."
  query       = query.ecc_k8s_024_apiserver_etcd_certfile_and_etcd_keyfile_arguments_are_set

  tags = merge(local.common_tags, {
    cis_item_id = "1.2.25"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_024_apiserver_etcd_certfile_and_etcd_keyfile_arguments_are_set" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-024-apiserver_etcd_certfile_and_etcd_keyfile_arguments_are_set') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-024-apiserver_etcd_certfile_and_etcd_keyfile_arguments_are_set'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-024-apiserver_etcd_certfile_and_etcd_keyfile_arguments_are_set'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-024-apiserver_etcd_certfile_and_etcd_keyfile_arguments_are_set'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-024-apiserver_etcd_certfile_and_etcd_keyfile_arguments_are_set'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_025_apiserver_tls_cert_file_and_tls_private_key_file_arguments_are_set" {
  title       = "ecc-k8s-025-apiserver_tls_cert_file_and_tls_private_key_file_arguments_are_set"
  description = "API server communication contains sensitive parameters that should remain encrypted in transit. Configure the API server to serve only HTTPS traffic."
  query       = query.ecc_k8s_025_apiserver_tls_cert_file_and_tls_private_key_file_arguments_are_set

  tags = merge(local.common_tags, {
    cis_item_id = "1.2.26"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_025_apiserver_tls_cert_file_and_tls_private_key_file_arguments_are_set" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-025-apiserver_tls_cert_file_and_tls_private_key_file_arguments_are_set') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-025-apiserver_tls_cert_file_and_tls_private_key_file_arguments_are_set'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-025-apiserver_tls_cert_file_and_tls_private_key_file_arguments_are_set'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-025-apiserver_tls_cert_file_and_tls_private_key_file_arguments_are_set'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-025-apiserver_tls_cert_file_and_tls_private_key_file_arguments_are_set'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_026_apiserver_client_ca_file_argument_is_set" {
  title       = "ecc-k8s-026-apiserver_client_ca_file_argument_is_set"
  description = "API server communication contains sensitive parameters that should remain encrypted in transit. Configure the API server to serve only HTTPS traffic. If --client-ca-file argument is set, any request presenting a client certificate signed by one of the authorities in the client-ca-file is authenticated with an identity corresponding to the CommonName of the client certificate."
  query       = query.ecc_k8s_026_apiserver_client_ca_file_argument_is_set

  tags = merge(local.common_tags, {
    cis_item_id = "1.2.27"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_026_apiserver_client_ca_file_argument_is_set" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-026-apiserver_client_ca_file_argument_is_set') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-026-apiserver_client_ca_file_argument_is_set'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-026-apiserver_client_ca_file_argument_is_set'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-026-apiserver_client_ca_file_argument_is_set'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-026-apiserver_client_ca_file_argument_is_set'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_027_apiserver_etcd_cafile_argument_is_set" {
  title       = "ecc-k8s-027-apiserver_etcd_cafile_argument_is_set"
  description = "etcd is a highly-available key value store used by Kubernetes deployments for persistent storage of all of its REST API objects. These objects are sensitive in nature and should be protected by client authentication. This requires the API server to identify itself to the etcd server using a SSL Certificate Authority file."
  query       = query.ecc_k8s_027_apiserver_etcd_cafile_argument_is_set

  tags = merge(local.common_tags, {
    cis_item_id = "1.2.28"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_027_apiserver_etcd_cafile_argument_is_set" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-027-apiserver_etcd_cafile_argument_is_set') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-027-apiserver_etcd_cafile_argument_is_set'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-027-apiserver_etcd_cafile_argument_is_set'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-027-apiserver_etcd_cafile_argument_is_set'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-027-apiserver_etcd_cafile_argument_is_set'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_028_apiserver_encryption_provider_config_argument_is_set" {
  title       = "ecc-k8s-028-apiserver_encryption_provider_config_argument_is_set"
  description = "etcd is a highly available key-value store used by Kubernetes deployments for persistent storage of all of its REST API objects. These objects are sensitive in nature and should be encrypted at rest to avoid any disclosures."
  query       = query.ecc_k8s_028_apiserver_encryption_provider_config_argument_is_set

  tags = merge(local.common_tags, {
    cis_item_id = "1.2.29"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_028_apiserver_encryption_provider_config_argument_is_set" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-028-apiserver_encryption_provider_config_argument_is_set') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-028-apiserver_encryption_provider_config_argument_is_set'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-028-apiserver_encryption_provider_config_argument_is_set'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-028-apiserver_encryption_provider_config_argument_is_set'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-028-apiserver_encryption_provider_config_argument_is_set'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_030_apiserver_apiserver_only_makes_use_of_strong_cryptographic_ciphers" {
  title       = "ecc-k8s-030-apiserver_apiserver_only_makes_use_of_strong_cryptographic_ciphers"
  description = "TLS ciphers have had a number of known vulnerabilities and weaknesses, which can reduce the protection provided by them. By default Kubernetes supports a number of TLS ciphersuites including some that have security concerns, weakening the protection provided."
  query       = query.ecc_k8s_030_apiserver_apiserver_only_makes_use_of_strong_cryptographic_ciphers

  tags = merge(local.common_tags, {
    cis_item_id = "1.2.31"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_030_apiserver_apiserver_only_makes_use_of_strong_cryptographic_ciphers" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-030-apiserver_apiserver_only_makes_use_of_strong_cryptographic_ciphers') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-030-apiserver_apiserver_only_makes_use_of_strong_cryptographic_ciphers'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-030-apiserver_apiserver_only_makes_use_of_strong_cryptographic_ciphers'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-030-apiserver_apiserver_only_makes_use_of_strong_cryptographic_ciphers'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-030-apiserver_apiserver_only_makes_use_of_strong_cryptographic_ciphers'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_031_controller_manager_terminated_pod_gc_threshold_argument_is_set_as_appropriate" {
  title       = "ecc-k8s-031-controller_manager_terminated_pod_gc_threshold_argument_is_set_as_appropriate"
  description = "Garbage collection is important to ensure sufficient resource availability and avoiding degraded performance and availability. In the worst case, the system might crash or just be unusable for a long period of time. The current setting for garbage collection is 12,500 terminated pods which might be too high for your system to sustain. Based on your system resources and tests, choose an appropriate threshold value to activate garbage collection."
  query       = query.ecc_k8s_031_controller_manager_terminated_pod_gc_threshold_argument_is_set_as_appropriate

  tags = merge(local.common_tags, {
    cis_item_id = "1.3.1"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_031_controller_manager_terminated_pod_gc_threshold_argument_is_set_as_appropriate" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-031-controller_manager_terminated_pod_gc_threshold_argument_is_set_as_appropriate') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-031-controller_manager_terminated_pod_gc_threshold_argument_is_set_as_appropriate'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-031-controller_manager_terminated_pod_gc_threshold_argument_is_set_as_appropriate'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-031-controller_manager_terminated_pod_gc_threshold_argument_is_set_as_appropriate'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-031-controller_manager_terminated_pod_gc_threshold_argument_is_set_as_appropriate'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_032_controller_manager_profiling_argument_is_set_to_false" {
  title       = "ecc-k8s-032-controller_manager_profiling_argument_is_set_to_false"
  description = "Profiling allows for the identification of specific performance bottlenecks. It generates a significant amount of program data that could potentially be exploited to uncover system and program details. If you are not experiencing any bottlenecks and do not need the profiler for troubleshooting purposes, it is recommended to turn it off to reduce the potential attack surface."
  query       = query.ecc_k8s_032_controller_manager_profiling_argument_is_set_to_false

  tags = merge(local.common_tags, {
    cis_item_id = "1.3.2"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_032_controller_manager_profiling_argument_is_set_to_false" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-032-controller_manager_profiling_argument_is_set_to_false') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-032-controller_manager_profiling_argument_is_set_to_false'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-032-controller_manager_profiling_argument_is_set_to_false'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-032-controller_manager_profiling_argument_is_set_to_false'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-032-controller_manager_profiling_argument_is_set_to_false'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_033_controller_manager_use_service_account_credentials_argument_is_set_to_true" {
  title       = "ecc-k8s-033-controller_manager_use_service_account_credentials_argument_is_set_to_true"
  description = "The controller manager creates a service account per controller in the kube-system namespace, generates a credential for it, and builds a dedicated API client with that service account credential for each controller loop to use. Setting the --use-serviceaccount-credentials to true runs each control loop within the controller manager using a separate service account credential. When used in combination with RBAC, this ensures that the control loops run with the minimum permissions required to perform their intended tasks."
  query       = query.ecc_k8s_033_controller_manager_use_service_account_credentials_argument_is_set_to_true

  tags = merge(local.common_tags, {
    cis_item_id = "1.3.3"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_033_controller_manager_use_service_account_credentials_argument_is_set_to_true" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-033-controller_manager_use_service_account_credentials_argument_is_set_to_true') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-033-controller_manager_use_service_account_credentials_argument_is_set_to_true'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-033-controller_manager_use_service_account_credentials_argument_is_set_to_true'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-033-controller_manager_use_service_account_credentials_argument_is_set_to_true'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-033-controller_manager_use_service_account_credentials_argument_is_set_to_true'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_034_controller_manager_service_account_private_key_file_argument_is_set" {
  title       = "ecc-k8s-034-controller_manager_service_account_private_key_file_argument_is_set"
  description = "To ensure that keys for service account tokens can be rotated as needed, a separate public/private key pair should be used for signing service account tokens. The private key should be specified to the controller manager with --service-account-private-key-file as appropriate."
  query       = query.ecc_k8s_034_controller_manager_service_account_private_key_file_argument_is_set

  tags = merge(local.common_tags, {
    cis_item_id = "1.3.4"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_034_controller_manager_service_account_private_key_file_argument_is_set" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-034-controller_manager_service_account_private_key_file_argument_is_set') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-034-controller_manager_service_account_private_key_file_argument_is_set'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-034-controller_manager_service_account_private_key_file_argument_is_set'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-034-controller_manager_service_account_private_key_file_argument_is_set'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-034-controller_manager_service_account_private_key_file_argument_is_set'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_035_controller_manager_root_ca_file_argument_is_set" {
  title       = "ecc-k8s-035-controller_manager_root_ca_file_argument_is_set"
  description = "Processes running within pods that need to contact the API server must verify the API server's serving certificate. Failing to do so could be a subject to man-in-the-middle attacks."
  query       = query.ecc_k8s_035_controller_manager_root_ca_file_argument_is_set

  tags = merge(local.common_tags, {
    cis_item_id = "1.3.5"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_035_controller_manager_root_ca_file_argument_is_set" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-035-controller_manager_root_ca_file_argument_is_set') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-035-controller_manager_root_ca_file_argument_is_set'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-035-controller_manager_root_ca_file_argument_is_set'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-035-controller_manager_root_ca_file_argument_is_set'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-035-controller_manager_root_ca_file_argument_is_set'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_036_controller_manager_rotatekubeletservercertificate_argument_is_set_to_true" {
  title       = "ecc-k8s-036-controller_manager_rotatekubeletservercertificate_argument_is_set_to_true"
  description = "RotateKubeletServerCertificate causes the kubelet to both request a serving certificate after bootstrapping its client credentials and rotate the certificate as its existing credentials expire. This automated periodic rotation ensures that the there are no downtimes due to expired certificates and thus addressing availability in the CIA security triad."
  query       = query.ecc_k8s_036_controller_manager_rotatekubeletservercertificate_argument_is_set_to_true

  tags = merge(local.common_tags, {
    cis_item_id = "1.3.6"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_036_controller_manager_rotatekubeletservercertificate_argument_is_set_to_true" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-036-controller_manager_rotatekubeletservercertificate_argument_is_set_to_true') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-036-controller_manager_rotatekubeletservercertificate_argument_is_set_to_true'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-036-controller_manager_rotatekubeletservercertificate_argument_is_set_to_true'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-036-controller_manager_rotatekubeletservercertificate_argument_is_set_to_true'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-036-controller_manager_rotatekubeletservercertificate_argument_is_set_to_true'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_037_controller_manager_bind_address_argument_is_set_to_127_0_0_1" {
  title       = "ecc-k8s-037-controller_manager_bind_address_argument_is_set_to_127_0_0_1"
  description = "Do not bind the Controller Manager service to non-loopback insecure addresses."
  query       = query.ecc_k8s_037_controller_manager_bind_address_argument_is_set_to_127_0_0_1

  tags = merge(local.common_tags, {
    cis_item_id = "1.3.7"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_037_controller_manager_bind_address_argument_is_set_to_127_0_0_1" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-037-controller_manager_bind_address_argument_is_set_to_127_0_0_1') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-037-controller_manager_bind_address_argument_is_set_to_127_0_0_1'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-037-controller_manager_bind_address_argument_is_set_to_127_0_0_1'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-037-controller_manager_bind_address_argument_is_set_to_127_0_0_1'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-037-controller_manager_bind_address_argument_is_set_to_127_0_0_1'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_038_scheduler_profiling_argument_is_set_to_false" {
  title       = "ecc-k8s-038-scheduler_profiling_argument_is_set_to_false"
  description = "Profiling allows for the identification of specific performance bottlenecks via web interface 'host:port/debug/pprof/'. It generates a significant amount of program data that could potentially be exploited to uncover system and program details. If you are not experiencing any bottlenecks and do not need the profiler for troubleshooting purposes, it is recommended to turn it off to reduce the potential attack surface."
  query       = query.ecc_k8s_038_scheduler_profiling_argument_is_set_to_false

  tags = merge(local.common_tags, {
    cis_item_id = "1.4.1"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_038_scheduler_profiling_argument_is_set_to_false" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-038-scheduler_profiling_argument_is_set_to_false') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-038-scheduler_profiling_argument_is_set_to_false'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-038-scheduler_profiling_argument_is_set_to_false'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-038-scheduler_profiling_argument_is_set_to_false'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-038-scheduler_profiling_argument_is_set_to_false'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_039_scheduler_bind_address_argument_is_set_to_127_0_0_1" {
  title       = "ecc-k8s-039-scheduler_bind_address_argument_is_set_to_127_0_0_1"
  description = "Do not bind the scheduler service to non-loopback insecure addresses."
  query       = query.ecc_k8s_039_scheduler_bind_address_argument_is_set_to_127_0_0_1

  tags = merge(local.common_tags, {
    cis_item_id = "1.4.2"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_039_scheduler_bind_address_argument_is_set_to_127_0_0_1" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-039-scheduler_bind_address_argument_is_set_to_127_0_0_1') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-039-scheduler_bind_address_argument_is_set_to_127_0_0_1'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-039-scheduler_bind_address_argument_is_set_to_127_0_0_1'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-039-scheduler_bind_address_argument_is_set_to_127_0_0_1'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-039-scheduler_bind_address_argument_is_set_to_127_0_0_1'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_040_etcd_cert_file_and_key_file_arguments_are_set_as_appropriate" {
  title       = "ecc-k8s-040-etcd_cert_file_and_key_file_arguments_are_set_as_appropriate"
  description = "etcd is a highly-available key value store used by Kubernetes deployments for persistent storage of all of its REST API objects. These objects are sensitive in nature and should be encrypted in transit."
  query       = query.ecc_k8s_040_etcd_cert_file_and_key_file_arguments_are_set_as_appropriate

  tags = merge(local.common_tags, {
    cis_item_id = "2.1"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_040_etcd_cert_file_and_key_file_arguments_are_set_as_appropriate" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-040-etcd_cert_file_and_key_file_arguments_are_set_as_appropriate') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-040-etcd_cert_file_and_key_file_arguments_are_set_as_appropriate'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-040-etcd_cert_file_and_key_file_arguments_are_set_as_appropriate'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-040-etcd_cert_file_and_key_file_arguments_are_set_as_appropriate'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-040-etcd_cert_file_and_key_file_arguments_are_set_as_appropriate'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_041_etcd_client_cert_auth_argument_is_set_to_true" {
  title       = "ecc-k8s-041-etcd_client_cert_auth_argument_is_set_to_true"
  description = "etcd is a highly-available key value store used by Kubernetes deployments for persistent storage of all of its REST API objects. These objects are sensitive in nature and should not be available to unauthenticated clients. You should enable the client authentication via valid certificates to secure the access to the etcd service."
  query       = query.ecc_k8s_041_etcd_client_cert_auth_argument_is_set_to_true

  tags = merge(local.common_tags, {
    cis_item_id = "2.2"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_041_etcd_client_cert_auth_argument_is_set_to_true" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-041-etcd_client_cert_auth_argument_is_set_to_true') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-041-etcd_client_cert_auth_argument_is_set_to_true'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-041-etcd_client_cert_auth_argument_is_set_to_true'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-041-etcd_client_cert_auth_argument_is_set_to_true'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-041-etcd_client_cert_auth_argument_is_set_to_true'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_042_etcd_auto_tls_argument_is_not_set_to_true" {
  title       = "ecc-k8s-042-etcd_auto_tls_argument_is_not_set_to_true"
  description = "etcd is a highly-available key value store used by Kubernetes deployments for persistent storage of all of its REST API objects. These objects are sensitive in nature and should not be available to unauthenticated clients. You should enable the client authentication via valid certificates to secure the access to the etcd service."
  query       = query.ecc_k8s_042_etcd_auto_tls_argument_is_not_set_to_true

  tags = merge(local.common_tags, {
    cis_item_id = "2.3"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_042_etcd_auto_tls_argument_is_not_set_to_true" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-042-etcd_auto_tls_argument_is_not_set_to_true') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-042-etcd_auto_tls_argument_is_not_set_to_true'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-042-etcd_auto_tls_argument_is_not_set_to_true'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-042-etcd_auto_tls_argument_is_not_set_to_true'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-042-etcd_auto_tls_argument_is_not_set_to_true'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_043_etcd_cluster_peer_cert_file_and_peer_key_file_arguments_are_set_as_appropriate" {
  title       = "ecc-k8s-043-etcd_cluster_peer_cert_file_and_peer_key_file_arguments_are_set_as_appropriate"
  description = "Note: This recommendation is applicable only for etcd clusters. If you are using only one etcd server in your environment then this recommendation is not applicable."
  query       = query.ecc_k8s_043_etcd_cluster_peer_cert_file_and_peer_key_file_arguments_are_set_as_appropriate

  tags = merge(local.common_tags, {
    cis_item_id = "2.4"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_043_etcd_cluster_peer_cert_file_and_peer_key_file_arguments_are_set_as_appropriate" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-043-etcd_cluster_peer_cert_file_and_peer_key_file_arguments_are_set_as_appropriate') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-043-etcd_cluster_peer_cert_file_and_peer_key_file_arguments_are_set_as_appropriate'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-043-etcd_cluster_peer_cert_file_and_peer_key_file_arguments_are_set_as_appropriate'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-043-etcd_cluster_peer_cert_file_and_peer_key_file_arguments_are_set_as_appropriate'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-043-etcd_cluster_peer_cert_file_and_peer_key_file_arguments_are_set_as_appropriate'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_044_etcd_cluster_peer_client_cert_auth_argument_is_set_to_true" {
  title       = "ecc-k8s-044-etcd_cluster_peer_client_cert_auth_argument_is_set_to_true"
  description = "Note: This recommendation is applicable only for etcd clusters. If you are using only one etcd server in your environment then this recommendation is not applicable."
  query       = query.ecc_k8s_044_etcd_cluster_peer_client_cert_auth_argument_is_set_to_true

  tags = merge(local.common_tags, {
    cis_item_id = "2.5"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_044_etcd_cluster_peer_client_cert_auth_argument_is_set_to_true" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-044-etcd_cluster_peer_client_cert_auth_argument_is_set_to_true') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-044-etcd_cluster_peer_client_cert_auth_argument_is_set_to_true'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-044-etcd_cluster_peer_client_cert_auth_argument_is_set_to_true'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-044-etcd_cluster_peer_client_cert_auth_argument_is_set_to_true'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-044-etcd_cluster_peer_client_cert_auth_argument_is_set_to_true'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_045_etcd_cluster_peer_auto_tls_argument_is_not_set_to_true" {
  title       = "ecc-k8s-045-etcd_cluster_peer_auto_tls_argument_is_not_set_to_true"
  description = "etcd is a highly-available key value store used by Kubernetes deployments for persistent storage of all of its REST API objects. These objects are sensitive in nature and should be accessible only by authenticated etcd peers in the etcd cluster. Hence, do not use automatically generated certificates for TLS connections between peers."
  query       = query.ecc_k8s_045_etcd_cluster_peer_auto_tls_argument_is_not_set_to_true

  tags = merge(local.common_tags, {
    cis_item_id = "2.6"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_045_etcd_cluster_peer_auto_tls_argument_is_not_set_to_true" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-045-etcd_cluster_peer_auto_tls_argument_is_not_set_to_true') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-045-etcd_cluster_peer_auto_tls_argument_is_not_set_to_true'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-045-etcd_cluster_peer_auto_tls_argument_is_not_set_to_true'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-045-etcd_cluster_peer_auto_tls_argument_is_not_set_to_true'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-045-etcd_cluster_peer_auto_tls_argument_is_not_set_to_true'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_047_minimize_wildcard_use_in_roles" {
  title       = "ecc-k8s-047-minimize_wildcard_use_in_roles"
  description = "Kubernetes Roles provide access to resources based on sets of objects and actions that can be taken on those objects. It is possible to set it to be the wildcard \"*\" which matches all items."
  query       = query.ecc_k8s_047_minimize_wildcard_use_in_roles

  tags = merge(local.common_tags, {
    cis_item_id = "5.1.3"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_047_minimize_wildcard_use_in_roles" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-047-minimize_wildcard_use_in_roles') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-047-minimize_wildcard_use_in_roles'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-047-minimize_wildcard_use_in_roles'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-047-minimize_wildcard_use_in_roles'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_048_minimize_wildcard_use_in_clusterroles" {
  title       = "ecc-k8s-048-minimize_wildcard_use_in_clusterroles"
  description = "Kubernetes ClusterRoles provide access to resources based on sets of objects and actions that can be taken on those objects. It is possible to set it to be the wildcard \"*\" which matches all items."
  query       = query.ecc_k8s_048_minimize_wildcard_use_in_clusterroles

  tags = merge(local.common_tags, {
    cis_item_id = "5.1.3"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_048_minimize_wildcard_use_in_clusterroles" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-048-minimize_wildcard_use_in_clusterroles') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-048-minimize_wildcard_use_in_clusterroles'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-048-minimize_wildcard_use_in_clusterroles'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-048-minimize_wildcard_use_in_clusterroles'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_049_seccomp_profile_is_set_to_docker_default_in_pod_definitions" {
  title       = "ecc-k8s-049-seccomp_profile_is_set_to_docker_default_in_pod_definitions"
  description = "Seccomp stands for secure computing mode and has been a feature of the Linux kernel since version 2.6.12. It can be used to sandbox the privileges of a process, restricting the calls it is able to make from userspace into the kernel. Kubernetes lets you automatically apply seccomp profiles loaded onto a node to your Pods and containers."
  query       = query.ecc_k8s_049_seccomp_profile_is_set_to_docker_default_in_pod_definitions

  tags = merge(local.common_tags, {
    cis_item_id = "5.7.2"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_049_seccomp_profile_is_set_to_docker_default_in_pod_definitions" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-049-seccomp_profile_is_set_to_docker_default_in_pod_definitions') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-049-seccomp_profile_is_set_to_docker_default_in_pod_definitions'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-049-seccomp_profile_is_set_to_docker_default_in_pod_definitions'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-049-seccomp_profile_is_set_to_docker_default_in_pod_definitions'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_050_default_namespace_should_not_be_used_for_pods" {
  title       = "ecc-k8s-050-default_namespace_should_not_be_used_for_pods"
  description = "Kubernetes provides a default namespace, where objects are placed if no namespace is specified for them. Placing objects in this namespace makes application of RBAC and other controls more difficult. Resources in a Kubernetes cluster should be segregated by namespace, to allow for security controls to be applied at that level and to make it easier to manage resources."
  query       = query.ecc_k8s_050_default_namespace_should_not_be_used_for_pods

  tags = merge(local.common_tags, {
    cis_item_id = "5.7.4"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_050_default_namespace_should_not_be_used_for_pods" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-050-default_namespace_should_not_be_used_for_pods') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-050-default_namespace_should_not_be_used_for_pods'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-050-default_namespace_should_not_be_used_for_pods'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-050-default_namespace_should_not_be_used_for_pods'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_051_default_namespace_should_not_be_used_for_configmap" {
  title       = "ecc-k8s-051-default_namespace_should_not_be_used_for_configmap"
  description = "Kubernetes provides a default namespace, where objects are placed if no namespace is specified for them. Placing objects in this namespace makes application of RBAC and other controls more difficult. Resources in a Kubernetes cluster should be segregated by namespace, to allow for security controls to be applied at that level and to make it easier to manage resources."
  query       = query.ecc_k8s_051_default_namespace_should_not_be_used_for_configmap

  tags = merge(local.common_tags, {
    cis_item_id = "5.7.4"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_051_default_namespace_should_not_be_used_for_configmap" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-051-default_namespace_should_not_be_used_for_configmap') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-051-default_namespace_should_not_be_used_for_configmap'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-051-default_namespace_should_not_be_used_for_configmap'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-051-default_namespace_should_not_be_used_for_configmap'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_052_default_namespace_should_not_be_used_for_deployment" {
  title       = "ecc-k8s-052-default_namespace_should_not_be_used_for_deployment"
  description = "Kubernetes provides a default namespace, where objects are placed if no namespace is specified for them. Placing objects in this namespace makes application of RBAC and other controls more difficult. Resources in a Kubernetes cluster should be segregated by namespace, to allow for security controls to be applied at that level and to make it easier to manage resources."
  query       = query.ecc_k8s_052_default_namespace_should_not_be_used_for_deployment

  tags = merge(local.common_tags, {
    cis_item_id = "5.7.4"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_052_default_namespace_should_not_be_used_for_deployment" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-052-default_namespace_should_not_be_used_for_deployment') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-052-default_namespace_should_not_be_used_for_deployment'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-052-default_namespace_should_not_be_used_for_deployment'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-052-default_namespace_should_not_be_used_for_deployment'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_053_default_namespace_should_not_be_used_for_role" {
  title       = "ecc-k8s-053-default_namespace_should_not_be_used_for_role"
  description = "Kubernetes provides a default namespace, where objects are placed if no namespace is specified for them. Placing objects in this namespace makes application of RBAC and other controls more difficult. Resources in a Kubernetes cluster should be segregated by namespace, to allow for security controls to be applied at that level and to make it easier to manage resources."
  query       = query.ecc_k8s_053_default_namespace_should_not_be_used_for_role

  tags = merge(local.common_tags, {
    cis_item_id = "5.7.4"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_053_default_namespace_should_not_be_used_for_role" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-053-default_namespace_should_not_be_used_for_role') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-053-default_namespace_should_not_be_used_for_role'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-053-default_namespace_should_not_be_used_for_role'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-053-default_namespace_should_not_be_used_for_role'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_054_minimize_the_admission_of_containers_which_use_hostports" {
  title       = "ecc-k8s-054-minimize_the_admission_of_containers_which_use_hostports"
  description = "Host ports connect containers directly to the host's network."
  query       = query.ecc_k8s_054_minimize_the_admission_of_containers_which_use_hostports

  tags = merge(local.common_tags, {
    cis_item_id = "5.2.13"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_054_minimize_the_admission_of_containers_which_use_hostports" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-054-minimize_the_admission_of_containers_which_use_hostports') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-054-minimize_the_admission_of_containers_which_use_hostports'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-054-minimize_the_admission_of_containers_which_use_hostports'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-054-minimize_the_admission_of_containers_which_use_hostports'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_056_minimize_the_admission_of_privileged_containers" {
  title       = "ecc-k8s-056-minimize_the_admission_of_privileged_containers"
  description = "Do not generally permit containers to be run with the securityContext.privileged flag set to true. There should be at least one admission control policy defined which does not permit privileged containers.If you need to run privileged containers, this should be defined in a separate policy and you should carefully check to ensure that only limited service accounts and users are given permission to use that policy."
  query       = query.ecc_k8s_056_minimize_the_admission_of_privileged_containers

  tags = merge(local.common_tags, {
    cis_item_id = "5.2.2"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_056_minimize_the_admission_of_privileged_containers" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-056-minimize_the_admission_of_privileged_containers') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-056-minimize_the_admission_of_privileged_containers'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-056-minimize_the_admission_of_privileged_containers'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-056-minimize_the_admission_of_privileged_containers'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_057_at_least_baseline_pod_security_level_policy_enforced_for_namespaces" {
  title       = "ecc-k8s-057-at_least_baseline_pod_security_level_policy_enforced_for_namespaces"
  description = "To control pod security Kubernetes provided Pod Security Standards, they specify a set of security settings that Pods must meet before they can be created or updated in a cluster."
  query       = query.ecc_k8s_057_at_least_baseline_pod_security_level_policy_enforced_for_namespaces

  tags = local.common_tags
}

query "ecc_k8s_057_at_least_baseline_pod_security_level_policy_enforced_for_namespaces" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-057-at_least_baseline_pod_security_level_policy_enforced_for_namespaces') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-057-at_least_baseline_pod_security_level_policy_enforced_for_namespaces'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-057-at_least_baseline_pod_security_level_policy_enforced_for_namespaces'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-057-at_least_baseline_pod_security_level_policy_enforced_for_namespaces'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_058_sa_tokens_are_only_mounted_where_necessary" {
  title       = "ecc-k8s-058-sa_tokens_are_only_mounted_where_necessary"
  description = "Mounting service account tokens inside pods can provide an avenue for privilege escalation attacks where an attacker is able to compromise a single pod in the cluster. Avoiding mounting these tokens removes this attack avenue."
  query       = query.ecc_k8s_058_sa_tokens_are_only_mounted_where_necessary

  tags = merge(local.common_tags, {
    cis_item_id = "5.1.6"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_058_sa_tokens_are_only_mounted_where_necessary" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-058-sa_tokens_are_only_mounted_where_necessary') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-058-sa_tokens_are_only_mounted_where_necessary'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-058-sa_tokens_are_only_mounted_where_necessary'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-058-sa_tokens_are_only_mounted_where_necessary'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_059_service_account_tokens_are_only_mounted_where_necessary_in_pods" {
  title       = "ecc-k8s-059-service_account_tokens_are_only_mounted_where_necessary_in_pods"
  description = "Mounting service account tokens inside pods can provide an avenue for privilege escalation attacks where an attacker is able to compromise a single pod in the cluster. Avoiding mounting these tokens removes this attack avenue."
  query       = query.ecc_k8s_059_service_account_tokens_are_only_mounted_where_necessary_in_pods

  tags = merge(local.common_tags, {
    cis_item_id = "5.1.6"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_059_service_account_tokens_are_only_mounted_where_necessary_in_pods" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-059-service_account_tokens_are_only_mounted_where_necessary_in_pods') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-059-service_account_tokens_are_only_mounted_where_necessary_in_pods'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-059-service_account_tokens_are_only_mounted_where_necessary_in_pods'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-059-service_account_tokens_are_only_mounted_where_necessary_in_pods'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_060_minimize_the_admission_of_hostpath_volumes" {
  title       = "ecc-k8s-060-minimize_the_admission_of_hostpath_volumes"
  description = "A container which mounts a hostPath volume as part of its specification will have access to the filesystem of the underlying cluster node. The use of hostPath volumes may allow containers access to privileged areas of the node filesystem."
  query       = query.ecc_k8s_060_minimize_the_admission_of_hostpath_volumes

  tags = merge(local.common_tags, {
    cis_item_id = "5.2.12"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_060_minimize_the_admission_of_hostpath_volumes" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-060-minimize_the_admission_of_hostpath_volumes') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-060-minimize_the_admission_of_hostpath_volumes'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-060-minimize_the_admission_of_hostpath_volumes'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-060-minimize_the_admission_of_hostpath_volumes'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_061_minimize_the_admission_of_windows_hostprocess_containers" {
  title       = "ecc-k8s-061-minimize_the_admission_of_windows_hostprocess_containers"
  description = "A Windows container making use of the 'hostProcess' flag can interact with the underlying Windows cluster node. As per the Kubernetes documentation, this provides \"privileged access\" to the Windows node."
  query       = query.ecc_k8s_061_minimize_the_admission_of_windows_hostprocess_containers

  tags = merge(local.common_tags, {
    cis_item_id = "5.2.11"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_061_minimize_the_admission_of_windows_hostprocess_containers" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-061-minimize_the_admission_of_windows_hostprocess_containers') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-061-minimize_the_admission_of_windows_hostprocess_containers'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-061-minimize_the_admission_of_windows_hostprocess_containers'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-061-minimize_the_admission_of_windows_hostprocess_containers'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_062_minimize_the_admission_of_containers_wishing_to_share_the_host_ipc_namespace" {
  title       = "ecc-k8s-062-minimize_the_admission_of_containers_wishing_to_share_the_host_ipc_namespace"
  description = "A container running in the host's IPC namespace can use IPC to interact with processes outside the container."
  query       = query.ecc_k8s_062_minimize_the_admission_of_containers_wishing_to_share_the_host_ipc_namespace

  tags = merge(local.common_tags, {
    cis_item_id = "5.2.4"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_062_minimize_the_admission_of_containers_wishing_to_share_the_host_ipc_namespace" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-062-minimize_the_admission_of_containers_wishing_to_share_the_host_ipc_namespace') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-062-minimize_the_admission_of_containers_wishing_to_share_the_host_ipc_namespace'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-062-minimize_the_admission_of_containers_wishing_to_share_the_host_ipc_namespace'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-062-minimize_the_admission_of_containers_wishing_to_share_the_host_ipc_namespace'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_063_minimize_the_admission_of_containers_with_allowprivilegeescalation" {
  title       = "ecc-k8s-063-minimize_the_admission_of_containers_with_allowprivilegeescalation"
  description = "Do not generally permit containers to be run with the allowPrivilegeEscalation flag set to true. Allowing this right can lead to a process running a container getting more rights than it started with. It's important to note that these rights are still constrained by the overall container sandbox, and this setting does not relate to the use of privileged containers."
  query       = query.ecc_k8s_063_minimize_the_admission_of_containers_with_allowprivilegeescalation

  tags = merge(local.common_tags, {
    cis_item_id = "5.2.6"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_063_minimize_the_admission_of_containers_with_allowprivilegeescalation" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-063-minimize_the_admission_of_containers_with_allowprivilegeescalation') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-063-minimize_the_admission_of_containers_with_allowprivilegeescalation'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-063-minimize_the_admission_of_containers_with_allowprivilegeescalation'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-063-minimize_the_admission_of_containers_with_allowprivilegeescalation'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_064_minimize_the_admission_of_containers_wishing_to_share_the_host_network_namespace" {
  title       = "ecc-k8s-064-minimize_the_admission_of_containers_wishing_to_share_the_host_network_namespace"
  description = "A container running in the host's network namespace could access the local loopback device, and could access network traffic to and from other pods."
  query       = query.ecc_k8s_064_minimize_the_admission_of_containers_wishing_to_share_the_host_network_namespace

  tags = merge(local.common_tags, {
    cis_item_id = "5.2.5"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_064_minimize_the_admission_of_containers_wishing_to_share_the_host_network_namespace" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-064-minimize_the_admission_of_containers_wishing_to_share_the_host_network_namespace') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-064-minimize_the_admission_of_containers_wishing_to_share_the_host_network_namespace'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-064-minimize_the_admission_of_containers_wishing_to_share_the_host_network_namespace'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-064-minimize_the_admission_of_containers_wishing_to_share_the_host_network_namespace'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_065_minimize_the_admission_of_containers_wishing_to_share_the_host_process_id_namespace" {
  title       = "ecc-k8s-065-minimize_the_admission_of_containers_wishing_to_share_the_host_process_id_namespace"
  description = "A container running in the host's PID namespace can inspect processes running outside the container. If the container also has access to ptrace capabilities this can be used to escalate privileges outside of the container."
  query       = query.ecc_k8s_065_minimize_the_admission_of_containers_wishing_to_share_the_host_process_id_namespace

  tags = merge(local.common_tags, {
    cis_item_id = "5.2.3"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_065_minimize_the_admission_of_containers_wishing_to_share_the_host_process_id_namespace" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-065-minimize_the_admission_of_containers_wishing_to_share_the_host_process_id_namespace') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-065-minimize_the_admission_of_containers_wishing_to_share_the_host_process_id_namespace'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-065-minimize_the_admission_of_containers_wishing_to_share_the_host_process_id_namespace'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-065-minimize_the_admission_of_containers_wishing_to_share_the_host_process_id_namespace'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_066_minimize_the_admission_of_containers_with_the_net_raw_capability" {
  title       = "ecc-k8s-066-minimize_the_admission_of_containers_with_the_net_raw_capability"
  description = "Containers run with a default set of capabilities as assigned by the Container Runtime. By default this can include potentially dangerous capabilities. With Docker as the container runtime the NET_RAW capability is enabled which may be misused by malicious containers. Ideally, all containers should drop this capability."
  query       = query.ecc_k8s_066_minimize_the_admission_of_containers_with_the_net_raw_capability

  tags = merge(local.common_tags, {
    cis_item_id = "5.2.8"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_066_minimize_the_admission_of_containers_with_the_net_raw_capability" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-066-minimize_the_admission_of_containers_with_the_net_raw_capability') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-066-minimize_the_admission_of_containers_with_the_net_raw_capability'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-066-minimize_the_admission_of_containers_with_the_net_raw_capability'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-066-minimize_the_admission_of_containers_with_the_net_raw_capability'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_067_minimize_the_admission_of_containers_with_added_capabilities" {
  title       = "ecc-k8s-067-minimize_the_admission_of_containers_with_added_capabilities"
  description = "Containers run with a default set of capabilities as assigned by the Container Runtime. Capabilities outside this set can be added to containers which could expose them to risks of container breakout attacks."
  query       = query.ecc_k8s_067_minimize_the_admission_of_containers_with_added_capabilities

  tags = merge(local.common_tags, {
    cis_item_id = "5.2.9"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_067_minimize_the_admission_of_containers_with_added_capabilities" {
  sql = <<-EOQ
select
  'alarm' as status,5.2.6
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-067-minimize_the_admission_of_containers_with_added_capabilities') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-067-minimize_the_admission_of_containers_with_added_capabilities'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-067-minimize_the_admission_of_containers_with_added_capabilities'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-067-minimize_the_admission_of_containers_with_added_capabilities'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_068_liveness_probe_is_configured" {
  title       = "ecc-k8s-068-liveness_probe_is_configured"
  description = "The kubelet uses liveness probes to know when to schedule restarts for containers. Restarting a container in a deadlock state can help to make the application more available, despite bugs."
  query       = query.ecc_k8s_068_liveness_probe_is_configured

  tags = local.common_tags
}

query "ecc_k8s_068_liveness_probe_is_configured" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-068-liveness_probe_is_configured') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-068-liveness_probe_is_configured'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-068-liveness_probe_is_configured'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-068-liveness_probe_is_configured'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_069_readiness_probe_is_configured" {
  title       = "ecc-k8s-069-readiness_probe_is_configured"
  description = "Readiness Probe is a Kubernetes capability that enables teams to make their applications more reliable and robust. This probe regulates under what circumstances the pod should be taken out of the list of service endpoints so that it no longer responds to requests. In defined circumstances the probe can remove the pod from the list of available service endpoints."
  query       = query.ecc_k8s_069_readiness_probe_is_configured

  tags = local.common_tags
}

query "ecc_k8s_069_readiness_probe_is_configured" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-069-readiness_probe_is_configured') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-069-readiness_probe_is_configured'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-069-readiness_probe_is_configured'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-069-readiness_probe_is_configured'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_070_minimize_the_admission_of_root_containers" {
  title       = "ecc-k8s-070-minimize-the-admission-of-root-containers"
  description = "Containers may run as any Linux user. Containers which run as the root user, whilst constrained by Container Runtime security features still have a escalated likelihood of container breakout. Ideally, all containers should run as a defined non-UID 0 user."
  query       = query.ecc_k8s_070_minimize_the_admission_of_root_containers

  tags = merge(local.common_tags, {
    cis_item_id = "5.2.7"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_070_minimize_the_admission_of_root_containers" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-070-minimize-the-admission-of-root-containers') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-070-minimize-the-admission-of-root-containers'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-070-minimize-the-admission-of-root-containers'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-070-minimize-the-admission-of-root-containers'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_071_minimize_the_admission_of_containers_with_capabilities_assigned" {
  title       = "ecc-k8s-071-minimize_the_admission_of_containers_with_capabilities_assigned"
  description = "Do not generally permit containers with capabilities."
  query       = query.ecc_k8s_071_minimize_the_admission_of_containers_with_capabilities_assigned

  tags = merge(local.common_tags, {
    cis_item_id = "5.2.10"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_071_minimize_the_admission_of_containers_with_capabilities_assigned" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-071-minimize_the_admission_of_containers_with_capabilities_assigned') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-071-minimize_the_admission_of_containers_with_capabilities_assigned'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-071-minimize_the_admission_of_containers_with_capabilities_assigned'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-071-minimize_the_admission_of_containers_with_capabilities_assigned'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_072_readonly_filesystem_is_configured" {
  title       = "ecc-k8s-072-readonly_filesystem_is_configured"
  description = "Using a read-only root file system for Kubernetes pods provides several benefits. It prevents any process from writing to the file system, which can help mitigate certain types of security attacks."
  query       = query.ecc_k8s_072_readonly_filesystem_is_configured

  tags = local.common_tags
}

query "ecc_k8s_072_readonly_filesystem_is_configured" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-072-readonly_filesystem_is_configured') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-072-readonly_filesystem_is_configured'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-072-readonly_filesystem_is_configured'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-072-readonly_filesystem_is_configured'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_074_prefer_using_secrets_as_files_over_secrets_as_environment_variables_for_pod" {
  title       = "ecc-k8s-074-prefer_using_secrets_as_files_over_secrets_as_environment_variables_for_pod"
  description = "Kubernetes supports mounting secrets as data volumes or as environment variables. Minimize the use of environment variable secrets."
  query       = query.ecc_k8s_074_prefer_using_secrets_as_files_over_secrets_as_environment_variables_for_pod

  tags = merge(local.common_tags, {
    cis_item_id = "5.4.1"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_074_prefer_using_secrets_as_files_over_secrets_as_environment_variables_for_pod" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-074-prefer_using_secrets_as_files_over_secrets_as_environment_variables_for_pod') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-074-prefer_using_secrets_as_files_over_secrets_as_environment_variables_for_pod'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-074-prefer_using_secrets_as_files_over_secrets_as_environment_variables_for_pod'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-074-prefer_using_secrets_as_files_over_secrets_as_environment_variables_for_pod'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_075_default_namespace_should_not_be_used_for_secret" {
  title       = "ecc-k8s-075-default_namespace_should_not_be_used_for_secret"
  description = "Kubernetes provides a default namespace, where objects are placed if no namespace is specified for them. Placing objects in this namespace makes application of RBAC and other controls more difficult. Resources in a Kubernetes cluster should be segregated by namespace, to allow for security controls to be applied at that level and to make it easier to manage resources."
  query       = query.ecc_k8s_075_default_namespace_should_not_be_used_for_secret

  tags = merge(local.common_tags, {
    cis_item_id = "5.7.4"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_075_default_namespace_should_not_be_used_for_secret" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-075-default_namespace_should_not_be_used_for_secret') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-075-default_namespace_should_not_be_used_for_secret'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-075-default_namespace_should_not_be_used_for_secret'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-075-default_namespace_should_not_be_used_for_secret'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_076_cpu_request_is_set" {
  title       = "ecc-k8s-076-cpu_request_is_set"
  description = "When specifying the resource request for containers in a pod, the scheduler uses this information to decide which node to place the pod on. When setting resource limit for a container, the kubelet enforces those limits so that the running container is not allowed to use more of that resource than the limit you set."
  query       = query.ecc_k8s_076_cpu_request_is_set

  tags = local.common_tags
}

query "ecc_k8s_076_cpu_request_is_set" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-076-cpu_request_is_set') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-076-cpu_request_is_set'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-076-cpu_request_is_set'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-076-cpu_request_is_set'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_077_limit_use_of_bind_impersonate_escalate_role" {
  title       = "ecc-k8s-077-limit_use_of_bind_impersonate_escalate_role"
  description = "Roles with the impersonate, bind or escalate permissions should not be granted unless strictly required."
  query       = query.ecc_k8s_077_limit_use_of_bind_impersonate_escalate_role

  tags = local.common_tags
}

query "ecc_k8s_077_limit_use_of_bind_impersonate_escalate_role" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-077-limit_use_of_bind_impersonate_escalate_role') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-077-limit_use_of_bind_impersonate_escalate_role'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-077-limit_use_of_bind_impersonate_escalate_role'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-077-limit_use_of_bind_impersonate_escalate_role'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_078_cpu_limits_are_set" {
  title       = "ecc-k8s-078-cpu_limits_are_set"
  description = "Kubernetes allows administrators to set CPU quotas in namespaces, as hard limits for resource usage. Containers cannot use more CPU than the configured limit. Provided the system has CPU time free, a container is guaranteed to be allocated as much CPU as it requests."
  query       = query.ecc_k8s_078_cpu_limits_are_set

  tags = local.common_tags
}

query "ecc_k8s_078_cpu_limits_are_set" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-078-cpu_limits_are_set') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-078-cpu_limits_are_set'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-078-cpu_limits_are_set'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-078-cpu_limits_are_set'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_079_memory_requests_are_set" {
  title       = "ecc-k8s-079-memory_requests_are_set"
  description = "Memory resources can be defined using values from bytes to petabytes, it is common to use mebibytes. If you configure a memory request that is larger than the amount of memory on your nodes, the pod will never be scheduled. When specifying a memory request for a container, include the resources:requests field in the container`s resource manifest. To specify a memory limit, include resources:limits."
  query       = query.ecc_k8s_079_memory_requests_are_set

  tags = local.common_tags
}

query "ecc_k8s_079_memory_requests_are_set" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-079-memory_requests_are_set') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-079-memory_requests_are_set'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-079-memory_requests_are_set'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-079-memory_requests_are_set'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_080_memory_limits_are_set" {
  title       = "ecc-k8s-080-memory_limits_are_set"
  description = "The scheduler uses resource request information for containers in a pod to decide which node to place the pod on. The kubelet enforces the resource limits set, so that the running container is not allowed to use more resource than the limit set."
  query       = query.ecc_k8s_080_memory_limits_are_set

  tags = local.common_tags
}

query "ecc_k8s_080_memory_limits_are_set" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-080-memory_limits_are_set') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-080-memory_limits_are_set'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-080-memory_limits_are_set'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-080-memory_limits_are_set'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_081_sys_admin_capability_is_not_used" {
  title       = "ecc-k8s-081-sys_admin_capability_is_not_used"
  description = "Capabilities permit certain named root actions without giving full root access and are considered a fine-grained permissions model."
  query       = query.ecc_k8s_081_sys_admin_capability_is_not_used

  tags = local.common_tags
}

query "ecc_k8s_081_sys_admin_capability_is_not_used" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-081-sys_admin_capability_is_not_used') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-081-sys_admin_capability_is_not_used'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-081-sys_admin_capability_is_not_used'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-081-sys_admin_capability_is_not_used'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_082_limit_use_of_bind_impersonate_escalate_cluster_role" {
  title       = "ecc-k8s-082-limit_use_of_bind_impersonate_escalate_cluster_role"
  description = "Cluster roles with the impersonate, bind or escalate permissions should not be granted unless strictly required."
  query       = query.ecc_k8s_082_limit_use_of_bind_impersonate_escalate_cluster_role

  tags = local.common_tags
}

query "ecc_k8s_082_limit_use_of_bind_impersonate_escalate_cluster_role" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-082-limit_use_of_bind_impersonate_escalate_cluster_role') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-082-limit_use_of_bind_impersonate_escalate_cluster_role'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-082-limit_use_of_bind_impersonate_escalate_cluster_role'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-082-limit_use_of_bind_impersonate_escalate_cluster_role'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_086_apply_security_context_to_your_pods_and_containers" {
  title       = "ecc-k8s-086-apply_security_context_to_your_pods_and_containers"
  description = "A security context defines the operating system security settings (uid, gid, capabilities, SELinux role, etc.) applied to a container. When designing your containers and pods, make sure that you configure the security context for your pods, containers, and volumes. A security context is a property defined in the deployment yaml. It controls the security parameters that will be assigned to the pod/container/volume. There are two levels of security context: pod level security context, and container level security context."
  query       = query.ecc_k8s_086_apply_security_context_to_your_pods_and_containers

  tags = merge(local.common_tags, {
    cis_item_id = "5.7.3"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_086_apply_security_context_to_your_pods_and_containers" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-086-apply_security_context_to_your_pods_and_containers') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-086-apply_security_context_to_your_pods_and_containers'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-086-apply_security_context_to_your_pods_and_containers'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-086-apply_security_context_to_your_pods_and_containers'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_087_minimize_access_to_secrets_in_roles" {
  title       = "ecc-k8s-087-minimize_access_to_secrets_in_roles"
  description = "The Kubernetes API stores secrets, which may be service account tokens for the Kubernetes API or credentials used by workloads in the cluster. Access to these secrets should be restricted to the smallest possible group of users to reduce the risk of privilege escalation. Inappropriate access to secrets stored within the Kubernetes cluster can allow for an attacker to gain additional access to the Kubernetes cluster or external resources whose credentials are stored as secrets."
  query       = query.ecc_k8s_087_minimize_access_to_secrets_in_roles

  tags = merge(local.common_tags, {
    cis_item_id = "5.1.2"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_087_minimize_access_to_secrets_in_roles" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-087-minimize_access_to_secrets_in_roles') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-087-minimize_access_to_secrets_in_roles'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-087-minimize_access_to_secrets_in_roles'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-087-minimize_access_to_secrets_in_roles'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_088_minimize_access_to_secrets_in_clusterroles" {
  title       = "ecc-k8s-088-minimize_access_to_secrets_in_clusterroles"
  description = "The Kubernetes API stores secrets, which may be service account tokens for the Kubernetes API or credentials used by workloads in the cluster. Access to these secrets should be restricted to the smallest possible group of users to reduce the risk of privilege escalation. Inappropriate access to secrets stored within the Kubernetes cluster can allow for an attacker to gain additional access to the Kubernetes cluster or external resources whose credentials are stored as secrets."
  query       = query.ecc_k8s_088_minimize_access_to_secrets_in_clusterroles

  tags = merge(local.common_tags, {
    cis_item_id = "5.1.2"
    cis_version = "v1.7.0"
  })
}

query "ecc_k8s_088_minimize_access_to_secrets_in_clusterroles" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'resource') as resource,
  coalesce(description, 'ecc-k8s-088-minimize_access_to_secrets_in_clusterroles') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-088-minimize_access_to_secrets_in_clusterroles'

union all

select
  'ok' as status,
  'passed-resource-' || gs.n::text as resource,
  'Compliant' as reason,
  null as namespace
from generate_series(
  1,
  greatest(
    0,
    coalesce(
      (
        select sum(coalesce(scanned_resources, 0)) - sum(coalesce(failed_resources, 0))
        from sre_rule_result
        where job_id = '${var.job_id}'
          and policy = 'ecc-k8s-088-minimize_access_to_secrets_in_clusterroles'
      ),
      0
    )
  )
) as gs(n)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-088-minimize_access_to_secrets_in_clusterroles'
  and nullif(error_type, '') is not null
EOQ
}


control "ecc_k8s_092_basic_auth_file_argument_not_set_openshift_kube_apiserver" {
  title       = "ecc-k8s-092-basic_auth_file_argument_not_set_openshift_kube_apiserver"
  description = "Basic authentication uses plaintext credentials for authentication. Currently, the basic authentication credentials last indefinitely, and the password cannot be changed without restarting the API server. The basic authentication is currently supported for convenience. Hence, basic authentication should not be used."
  query       = query.ecc_k8s_092_basic_auth_file_argument_not_set_openshift_kube_apiserver

  tags = local.common_tags
}

query "ecc_k8s_092_basic_auth_file_argument_not_set_openshift_kube_apiserver" {
  sql = <<-EOQ
select
  'alarm' as status,
  coalesce(nullif(resource_name, ''), nullif(resource_id, ''), 'cluster') as resource,
  coalesce(description, 'ecc-k8s-092-basic_auth_file_argument_not_set_openshift_kube_apiserver') as reason,
  namespace
from sre_finding
where job_id = '${var.job_id}'
  and rule_name = 'ecc-k8s-092-basic_auth_file_argument_not_set_openshift_kube_apiserver'

union all

select
  'ok' as status,
  'cluster' as resource,
  'Compliant' as reason,
  null as namespace
where not exists (
  select 1
  from sre_finding
  where job_id = '${var.job_id}'
    and rule_name = 'ecc-k8s-092-basic_auth_file_argument_not_set_openshift_kube_apiserver'
)
and not exists (
  select 1
  from sre_rule_result
  where job_id = '${var.job_id}'
    and policy = 'ecc-k8s-092-basic_auth_file_argument_not_set_openshift_kube_apiserver'
    and nullif(error_type, '') is not null
)

union all

select
  'error' as status,
  policy as resource,
  coalesce(nullif(reason, ''), error_type, 'execution failed') as reason,
  null as namespace
from sre_rule_result
where job_id = '${var.job_id}'
  and policy = 'ecc-k8s-092-basic_auth_file_argument_not_set_openshift_kube_apiserver'
  and nullif(error_type, '') is not null
EOQ
}
