# Generated CIS v1.20 benchmark

benchmark "cis_v120" {
  title       = "CIS Kubernetes v1.20"
  description = "CIS Kubernetes Benchmark v1.2.0 compliance from SRE Custodian scan results."
  children = [
    benchmark.cis_v120_4_1,
    benchmark.cis_v120_4_2,
    benchmark.cis_v120_4_4,
    benchmark.cis_v120_4_6
  ]
  tags = merge(local.common_tags, { type = "Benchmark" }, { cis = "true", cis_version = "v1.20", category = "Compliance", service = "Kubernetes" })
}

benchmark "cis_v120_4_1" {
  title       = "4.1 CIS Controls"
  description = "CIS Kubernetes Benchmark v1.2.0 (Kubernetes v1.20) section 4.1."
  children = [
    control.ecc_k8s_087_minimize_access_to_secrets_in_roles,
    control.ecc_k8s_088_minimize_access_to_secrets_in_clusterroles,
    control.ecc_k8s_047_minimize_wildcard_use_in_roles,
    control.ecc_k8s_048_minimize_wildcard_use_in_clusterroles,
    control.ecc_k8s_058_sa_tokens_are_only_mounted_where_necessary,
    control.ecc_k8s_059_service_account_tokens_are_only_mounted_where_necessary_in_pods
  ]
  tags = merge(local.common_tags, { type = "Benchmark" }, { cis = "true", cis_version = "v1.20" })
}


benchmark "cis_v120_4_2" {
  title       = "4.2 CIS Controls"
  description = "CIS Kubernetes Benchmark v1.2.0 (Kubernetes v1.20) section 4.2."
  children = [
    control.ecc_k8s_065_minimize_the_admission_of_containers_wishing_to_share_the_host_process_id_namespace,
    control.ecc_k8s_062_minimize_the_admission_of_containers_wishing_to_share_the_host_ipc_namespace,
    control.ecc_k8s_064_minimize_the_admission_of_containers_wishing_to_share_the_host_network_namespace,
    control.ecc_k8s_063_minimize_the_admission_of_containers_with_allowprivilegeescalation,
    control.ecc_k8s_070_minimize_the_admission_of_root_containers,
    control.ecc_k8s_066_minimize_the_admission_of_containers_with_the_net_raw_capability,
    control.ecc_k8s_067_minimize_the_admission_of_containers_with_added_capabilities,
    control.ecc_k8s_071_minimize_the_admission_of_containers_with_capabilities_assigned
  ]
  tags = merge(local.common_tags, { type = "Benchmark" }, { cis = "true", cis_version = "v1.20" })
}


benchmark "cis_v120_4_4" {
  title       = "4.4 CIS Controls"
  description = "CIS Kubernetes Benchmark v1.2.0 (Kubernetes v1.20) section 4.4."
  children = [
    control.ecc_k8s_074_prefer_using_secrets_as_files_over_secrets_as_environment_variables_for_pod
  ]
  tags = merge(local.common_tags, { type = "Benchmark" }, { cis = "true", cis_version = "v1.20" })
}


benchmark "cis_v120_4_6" {
  title       = "4.6 CIS Controls"
  description = "CIS Kubernetes Benchmark v1.2.0 (Kubernetes v1.20) section 4.6."
  children = [
    control.ecc_k8s_086_apply_security_context_to_your_pods_and_containers,
    control.ecc_k8s_050_default_namespace_should_not_be_used_for_pods,
    control.ecc_k8s_051_default_namespace_should_not_be_used_for_configmap,
    control.ecc_k8s_052_default_namespace_should_not_be_used_for_deployment,
    control.ecc_k8s_053_default_namespace_should_not_be_used_for_role,
    control.ecc_k8s_075_default_namespace_should_not_be_used_for_secret
  ]
  tags = merge(local.common_tags, { type = "Benchmark" }, { cis = "true", cis_version = "v1.20" })
}
