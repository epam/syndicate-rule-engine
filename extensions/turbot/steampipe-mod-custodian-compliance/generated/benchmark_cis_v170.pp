# Generated CIS v1.7.0 benchmark

benchmark "cis_v170" {
  title       = "CIS v1.7.0"
  description = "CIS Kubernetes Benchmark v1.7.0 compliance from SRE Custodian scan results."
  children = [
    benchmark.cis_v170_1_2,
    benchmark.cis_v170_1_3,
    benchmark.cis_v170_1_4,
    benchmark.cis_v170_2_1,
    benchmark.cis_v170_2_2,
    benchmark.cis_v170_2_3,
    benchmark.cis_v170_2_4,
    benchmark.cis_v170_2_5,
    benchmark.cis_v170_2_6,
    benchmark.cis_v170_5_1,
    benchmark.cis_v170_5_2,
    benchmark.cis_v170_5_4,
    benchmark.cis_v170_5_7
  ]
  tags = merge(local.common_tags, { type = "Benchmark" }, { cis = "true", cis_version = "v1.7.0", category = "Compliance", service = "Kubernetes" })
}

benchmark "cis_v170_1_2" {
  title       = "1.2 API Server"
  description = "CIS Kubernetes Benchmark v1.7.0 section 1.2."
  children = [
    control.ecc_k8s_001_apiserver_anonymous_auth_argument_is_set_to_false,
    control.ecc_k8s_010_apiserver_admission_control_plugin_alwaysadmit_is_not_set,
    control.ecc_k8s_011_apiserver_admission_control_plugin_alwayspullimages_is_set,
    control.ecc_k8s_012_apiserver_admission_control_plugin_securitycontextdeny_is_set,
    control.ecc_k8s_013_apiserver_admission_control_plugin_serviceaccount_is_set,
    control.ecc_k8s_014_apiserver_admission_control_plugin_namespacelifecycle_is_set,
    control.ecc_k8s_015_apiserver_admission_control_plugin_noderestriction_is_set,
    control.ecc_k8s_016_apiserver_profiling_argument_is_set_to_false,
    control.ecc_k8s_017_apiserver_audit_log_path_argument_is_set,
    control.ecc_k8s_018_apiserver_audit_log_maxage_argument_is_set_to_30,
    control.ecc_k8s_002_apiserver_token_auth_file_parameter_is_not_set,
    control.ecc_k8s_019_apiserver_audit_log_maxbackup_argument_is_set_to_10,
    control.ecc_k8s_020_apiserver_audit_log_maxsize_argument_is_set_to_100,
    control.ecc_k8s_021_apiserver_request_timeout_argument_is_set_as_appropriate,
    control.ecc_k8s_022_apiserver_service_account_lookup_argument_is_set_to_true,
    control.ecc_k8s_023_apiserver_service_account_key_file_argument_is_set,
    control.ecc_k8s_024_apiserver_etcd_certfile_and_etcd_keyfile_arguments_are_set,
    control.ecc_k8s_025_apiserver_tls_cert_file_and_tls_private_key_file_arguments_are_set,
    control.ecc_k8s_026_apiserver_client_ca_file_argument_is_set,
    control.ecc_k8s_027_apiserver_etcd_cafile_argument_is_set,
    control.ecc_k8s_028_apiserver_encryption_provider_config_argument_is_set,
    control.ecc_k8s_030_apiserver_apiserver_only_makes_use_of_strong_cryptographic_ciphers,
    control.ecc_k8s_004_apiserver_kubelet_client_certificate_and_kubelet_client_key_arguments_are_set,
    control.ecc_k8s_005_apiserver_kubelet_certificate_authority_argument_is_set,
    control.ecc_k8s_006_apiserver_authorization_mode_argument_is_not_set_to_alwaysallow,
    control.ecc_k8s_007_apiserver_authorization_mode_argument_includes_node,
    control.ecc_k8s_008_apiserver_authorization_mode_argument_includes_rbac,
    control.ecc_k8s_009_apiserver_admission_control_plugin_eventratelimit_is_set
  ]
  tags = merge(local.common_tags, { type = "Benchmark" }, { cis = "true", cis_version = "v1.7.0" })
}


benchmark "cis_v170_1_3" {
  title       = "1.3 CIS Controls"
  description = "CIS Kubernetes Benchmark v1.7.0 section 1.3."
  children = [
    control.ecc_k8s_031_controller_manager_terminated_pod_gc_threshold_argument_is_set_as_appropriate,
    control.ecc_k8s_032_controller_manager_profiling_argument_is_set_to_false,
    control.ecc_k8s_033_controller_manager_use_service_account_credentials_argument_is_set_to_true,
    control.ecc_k8s_034_controller_manager_service_account_private_key_file_argument_is_set,
    control.ecc_k8s_035_controller_manager_root_ca_file_argument_is_set,
    control.ecc_k8s_036_controller_manager_rotatekubeletservercertificate_argument_is_set_to_true,
    control.ecc_k8s_037_controller_manager_bind_address_argument_is_set_to_127_0_0_1
  ]
  tags = merge(local.common_tags, { type = "Benchmark" }, { cis = "true", cis_version = "v1.7.0" })
}


benchmark "cis_v170_1_4" {
  title       = "1.4 CIS Controls"
  description = "CIS Kubernetes Benchmark v1.7.0 section 1.4."
  children = [
    control.ecc_k8s_038_scheduler_profiling_argument_is_set_to_false,
    control.ecc_k8s_039_scheduler_bind_address_argument_is_set_to_127_0_0_1
  ]
  tags = merge(local.common_tags, { type = "Benchmark" }, { cis = "true", cis_version = "v1.7.0" })
}


benchmark "cis_v170_2_1" {
  title       = "2.1 CIS Controls"
  description = "CIS Kubernetes Benchmark v1.7.0 section 2.1."
  children = [
    control.ecc_k8s_040_etcd_cert_file_and_key_file_arguments_are_set_as_appropriate
  ]
  tags = merge(local.common_tags, { type = "Benchmark" }, { cis = "true", cis_version = "v1.7.0" })
}


benchmark "cis_v170_2_2" {
  title       = "2.2 CIS Controls"
  description = "CIS Kubernetes Benchmark v1.7.0 section 2.2."
  children = [
    control.ecc_k8s_041_etcd_client_cert_auth_argument_is_set_to_true
  ]
  tags = merge(local.common_tags, { type = "Benchmark" }, { cis = "true", cis_version = "v1.7.0" })
}


benchmark "cis_v170_2_3" {
  title       = "2.3 CIS Controls"
  description = "CIS Kubernetes Benchmark v1.7.0 section 2.3."
  children = [
    control.ecc_k8s_042_etcd_auto_tls_argument_is_not_set_to_true
  ]
  tags = merge(local.common_tags, { type = "Benchmark" }, { cis = "true", cis_version = "v1.7.0" })
}


benchmark "cis_v170_2_4" {
  title       = "2.4 CIS Controls"
  description = "CIS Kubernetes Benchmark v1.7.0 section 2.4."
  children = [
    control.ecc_k8s_043_etcd_cluster_peer_cert_file_and_peer_key_file_arguments_are_set_as_appropriate
  ]
  tags = merge(local.common_tags, { type = "Benchmark" }, { cis = "true", cis_version = "v1.7.0" })
}


benchmark "cis_v170_2_5" {
  title       = "2.5 CIS Controls"
  description = "CIS Kubernetes Benchmark v1.7.0 section 2.5."
  children = [
    control.ecc_k8s_044_etcd_cluster_peer_client_cert_auth_argument_is_set_to_true
  ]
  tags = merge(local.common_tags, { type = "Benchmark" }, { cis = "true", cis_version = "v1.7.0" })
}


benchmark "cis_v170_2_6" {
  title       = "2.6 CIS Controls"
  description = "CIS Kubernetes Benchmark v1.7.0 section 2.6."
  children = [
    control.ecc_k8s_045_etcd_cluster_peer_auto_tls_argument_is_not_set_to_true
  ]
  tags = merge(local.common_tags, { type = "Benchmark" }, { cis = "true", cis_version = "v1.7.0" })
}


benchmark "cis_v170_5_1" {
  title       = "5.1 CIS Controls"
  description = "CIS Kubernetes Benchmark v1.7.0 section 5.1."
  children = [
    control.ecc_k8s_087_minimize_access_to_secrets_in_roles,
    control.ecc_k8s_088_minimize_access_to_secrets_in_clusterroles,
    control.ecc_k8s_047_minimize_wildcard_use_in_roles,
    control.ecc_k8s_048_minimize_wildcard_use_in_clusterroles,
    control.ecc_k8s_058_sa_tokens_are_only_mounted_where_necessary,
    control.ecc_k8s_059_service_account_tokens_are_only_mounted_where_necessary_in_pods
  ]
  tags = merge(local.common_tags, { type = "Benchmark" }, { cis = "true", cis_version = "v1.7.0" })
}


benchmark "cis_v170_5_2" {
  title       = "5.2 Pod Security Standards"
  description = "CIS Kubernetes Benchmark v1.7.0 section 5.2."
  children = [
    control.ecc_k8s_071_minimize_the_admission_of_containers_with_capabilities_assigned,
    control.ecc_k8s_061_minimize_the_admission_of_windows_hostprocess_containers,
    control.ecc_k8s_060_minimize_the_admission_of_hostpath_volumes,
    control.ecc_k8s_054_minimize_the_admission_of_containers_which_use_hostports,
    control.ecc_k8s_056_minimize_the_admission_of_privileged_containers,
    control.ecc_k8s_065_minimize_the_admission_of_containers_wishing_to_share_the_host_process_id_namespace,
    control.ecc_k8s_062_minimize_the_admission_of_containers_wishing_to_share_the_host_ipc_namespace,
    control.ecc_k8s_064_minimize_the_admission_of_containers_wishing_to_share_the_host_network_namespace,
    control.ecc_k8s_063_minimize_the_admission_of_containers_with_allowprivilegeescalation,
    control.ecc_k8s_070_minimize_the_admission_of_root_containers,
    control.ecc_k8s_066_minimize_the_admission_of_containers_with_the_net_raw_capability,
    control.ecc_k8s_067_minimize_the_admission_of_containers_with_added_capabilities
  ]
  tags = merge(local.common_tags, { type = "Benchmark" }, { cis = "true", cis_version = "v1.7.0" })
}


benchmark "cis_v170_5_4" {
  title       = "5.4 CIS Controls"
  description = "CIS Kubernetes Benchmark v1.7.0 section 5.4."
  children = [
    control.ecc_k8s_074_prefer_using_secrets_as_files_over_secrets_as_environment_variables_for_pod
  ]
  tags = merge(local.common_tags, { type = "Benchmark" }, { cis = "true", cis_version = "v1.7.0" })
}


benchmark "cis_v170_5_7" {
  title       = "5.7 CIS Controls"
  description = "CIS Kubernetes Benchmark v1.7.0 section 5.7."
  children = [
    control.ecc_k8s_049_seccomp_profile_is_set_to_docker_default_in_pod_definitions,
    control.ecc_k8s_086_apply_security_context_to_your_pods_and_containers,
    control.ecc_k8s_050_default_namespace_should_not_be_used_for_pods,
    control.ecc_k8s_051_default_namespace_should_not_be_used_for_configmap,
    control.ecc_k8s_052_default_namespace_should_not_be_used_for_deployment,
    control.ecc_k8s_053_default_namespace_should_not_be_used_for_role,
    control.ecc_k8s_075_default_namespace_should_not_be_used_for_secret
  ]
  tags = merge(local.common_tags, { type = "Benchmark" }, { cis = "true", cis_version = "v1.7.0" })
}
