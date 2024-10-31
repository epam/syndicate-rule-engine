from pathlib import Path

import pytest
from modular_sdk.models.tenant import Tenant

from executor.helpers.constants import Cloud
from executor.services.report_service import JobResult
from helpers.constants import PolicyErrorType
from ..commons import DATA


@pytest.fixture
def aws_scan_result() -> Path:
    return DATA / "cloud_custodian" / "aws"


@pytest.fixture
def azure_scan_result() -> Path:
    return DATA / "cloud_custodian" / "azure"


@pytest.fixture
def google_scan_result() -> Path:
    return DATA / "cloud_custodian" / "google"


@pytest.fixture
def aws_tenant() -> Tenant:
    return Tenant(
        name="TEST-TENANT",
        display_name="Test tenant",
        is_active=True,
        customer_name="TEST-CUSTOMER",
        cloud="AWS",
        project="123456789012",
    )


def test_adjust_resource_type():
    item = JobResult(Path(), Cloud.AWS)
    assert item.adjust_resource_type("s3") == "aws.s3"
    assert item.adjust_resource_type("aws.s3") == "aws.s3"
    assert item.adjust_resource_type("gcp.vpc") == "aws.vpc"

    item = JobResult(Path(), Cloud.AZURE)
    assert item.adjust_resource_type("vm") == "azure.vm"
    assert item.adjust_resource_type("azure.vm") == "azure.vm"

    item = JobResult(Path(), Cloud.GOOGLE)
    assert item.adjust_resource_type("vpc") == "gcp.vpc"
    assert item.adjust_resource_type("gcp.vpc") == "gcp.vpc"


def test_rules_meta_aws(aws_scan_result: Path):
    item = JobResult(aws_scan_result, Cloud.AWS)
    meta = item.rules_meta()
    assert (
            len(meta) == 14
    )  # that is a number of unique rules in our stubs
    assert meta == {
        "ecc-aws-427-rds_cluster_without_tag_information": {
            "resource": "aws.rds-cluster",
            "description": "Description for ecc-aws-427-rds_cluster_without_tag_information",
            "comment": "010010062000",
        },
        "ecc-aws-149-rds_public_access_disabled": {
            "resource": "aws.rds",
            "description": "Description for ecc-aws-149-rds_public_access_disabled",
            "comment": "010040062900",
        },
        "ecc-aws-167-security_group_ingress_is_restricted_traffic_to_port_143": {
            "resource": "aws.security-group",
            "description": "Description for ecc-aws-167-security_group_ingress_is_restricted_traffic_to_port_143",
            "comment": "010042022000",
        },
        "ecc-aws-575-ebs_volumes_attached_to_stopped_ec2_instances": {
            "resource": "aws.ebs",
            "description": "Description for ecc-aws-575-ebs_volumes_attached_to_stopped_ec2_instances",
            "comment": "010002042000",
        },
        "ecc-aws-490-ec2_token_hop_limit_check": {
            "resource": "aws.ec2",
            "description": "Description for ecc-aws-490-ec2_token_hop_limit_check",
            "comment": "010024032010",
        },
        "ecc-aws-229-ecr_repository_kms_encryption_enabled": {
            "resource": "aws.ecr",
            "description": "Description for ecc-aws-229-ecr_repository_kms_encryption_enabled",
            "comment": "010043082000",
        },
        "ecc-aws-374-cloudtrail_logs_data_events": {
            "resource": "aws.cloudtrail",
            "description": "Description for ecc-aws-374-cloudtrail_logs_data_events",
            "comment": "010019012000",
        },
        "ecc-aws-150-api_gateway_rest_api_encryption_at_rest": {
            "resource": "aws.rest-stage",
            "description": "Description for ecc-aws-150-api_gateway_rest_api_encryption_at_rest",
            "comment": "010043022000",
        },
        "ecc-aws-070-unused_ec2_security_groups": {
            "resource": "aws.security-group",
            "description": "Description for ecc-aws-070-unused_ec2_security_groups",
            "comment": "010018022000",
        },
        "ecc-aws-221-sns_kms_encryption_enabled": {
            "resource": "aws.sns",
            "description": "Description for ecc-aws-221-sns_kms_encryption_enabled",
            "comment": "010042022000",
        },
        "ecc-aws-112-s3_bucket_versioning_mfa_delete_enabled": {
            "resource": "aws.s3",
            "description": "Description for ecc-aws-112-s3_bucket_versioning_mfa_delete_enabled",
            "comment": "010047042901",
        },
        "ecc-aws-499-iam_group_has_users_check": {
            "resource": "aws.iam-group",
            "description": "Description for ecc-aws-499-iam_group_has_users_check",
            "comment": "010018002001",
        },
        "ecc-aws-516-s3_event_notifications_enabled": {
            "resource": "aws.s3",
            "description": "Description for ecc-aws-516-s3_event_notifications_enabled",
            "comment": "010019042001",
        },
        "ecc-aws-527-waf_global_webacl_not_empty": {
            "resource": "aws.waf",
            "description": "Description for ecc-aws-527-waf_global_webacl_not_empty",
            "comment": "010002092001",
        },
    }


def test_statistics_aws(aws_tenant, aws_scan_result):
    item = JobResult(aws_scan_result, Cloud.AWS)
    failed = {
        ("eu-west-1", "ecc-aws-427-rds_cluster_without_tag_information"): (
            PolicyErrorType.ACCESS,
            "AccessDenied Exception",
            [],
        ),
        ("global", "ecc-aws-527-waf_global_webacl_not_empty"): (
            PolicyErrorType.ACCESS,
            "AccessDenied Exception",
            [],
        ),
    }
    stats = item.statistics(aws_tenant, failed)
    assert len(stats) == 15
    assert stats == [
        {
            "policy": "ecc-aws-427-rds_cluster_without_tag_information",
            "region": "eu-west-1",
            "tenant_name": "TEST-TENANT",
            "customer_name": "TEST-CUSTOMER",
            "start_time": 1729253941.704202,
            "end_time": 1729253941.712029,
            "api_calls": {},
            "error_type": PolicyErrorType.ACCESS,
            "reason": "AccessDenied Exception",
            "traceback": [],
        },
        {
            "policy": "ecc-aws-149-rds_public_access_disabled",
            "region": "eu-west-1",
            "tenant_name": "TEST-TENANT",
            "customer_name": "TEST-CUSTOMER",
            "start_time": 1729253903.4745028,
            "end_time": 1729253903.48386,
            "api_calls": {},
            "scanned_resources": None,
            "failed_resources": 0,
        },
        {
            "policy": "ecc-aws-167-security_group_ingress_is_restricted_traffic_to_port_143",
            "region": "eu-west-1",
            "tenant_name": "TEST-TENANT",
            "customer_name": "TEST-CUSTOMER",
            "start_time": 1729253907.7343721,
            "end_time": 1729253907.736023,
            "api_calls": {},
            "scanned_resources": None,
            "failed_resources": 0,
        },
        {
            "policy": "ecc-aws-575-ebs_volumes_attached_to_stopped_ec2_instances",
            "region": "eu-west-1",
            "tenant_name": "TEST-TENANT",
            "customer_name": "TEST-CUSTOMER",
            "start_time": 1729253989.469185,
            "end_time": 1729253990.192673,
            "api_calls": {"ec2.DescribeInstances": 1},
            "scanned_resources": None,
            "failed_resources": 2,
        },
        {
            "policy": "ecc-aws-490-ec2_token_hop_limit_check",
            "region": "eu-west-1",
            "tenant_name": "TEST-TENANT",
            "customer_name": "TEST-CUSTOMER",
            "start_time": 1729253946.7202518,
            "end_time": 1729253946.722402,
            "api_calls": {},
            "scanned_resources": None,
            "failed_resources": 2,
        },
        {
            "policy": "ecc-aws-229-ecr_repository_kms_encryption_enabled",
            "region": "eu-west-1",
            "tenant_name": "TEST-TENANT",
            "customer_name": "TEST-CUSTOMER",
            "start_time": 1729253920.7959292,
            "end_time": 1729253920.7997608,
            "api_calls": {},
            "scanned_resources": None,
            "failed_resources": 2,
        },
        {
            "policy": "ecc-aws-374-cloudtrail_logs_data_events",
            "region": "eu-west-1",
            "tenant_name": "TEST-TENANT",
            "customer_name": "TEST-CUSTOMER",
            "start_time": 1729253935.386652,
            "end_time": 1729253935.660345,
            "api_calls": {"cloudtrail.GetEventSelectors": 1},
            "scanned_resources": None,
            "failed_resources": 1,
        },
        {
            "policy": "ecc-aws-150-api_gateway_rest_api_encryption_at_rest",
            "region": "eu-central-1",
            "tenant_name": "TEST-TENANT",
            "customer_name": "TEST-CUSTOMER",
            "start_time": 1729253764.842481,
            "end_time": 1729253765.068435,
            "api_calls": {"apigateway.GetStages": 1},
            "scanned_resources": None,
            "failed_resources": 0,
        },
        {
            "policy": "ecc-aws-070-unused_ec2_security_groups",
            "region": "eu-central-1",
            "tenant_name": "TEST-TENANT",
            "customer_name": "TEST-CUSTOMER",
            "start_time": 1729253751.923907,
            "end_time": 1729253757.393474,
            "api_calls": {
                "ec2.DescribeNetworkInterfaces": 1,
                "lambda.ListFunctions": 1,
                "autoscaling.DescribeLaunchConfigurations": 1,
                "events.ListRules": 1,
                "events.ListTargetsByRule": 22,
                "codebuild.ListProjects": 1,
                "codebuild.BatchGetProjects": 1,
                "tagging.GetResources": 1,
                "batch.DescribeComputeEnvironments": 1,
                "ec2.DescribeSecurityGroupReferences": 1,
            },
            "scanned_resources": None,
            "failed_resources": 3,
        },
        {
            "policy": "ecc-aws-374-cloudtrail_logs_data_events",
            "region": "eu-central-1",
            "tenant_name": "TEST-TENANT",
            "customer_name": "TEST-CUSTOMER",
            "start_time": 1729253779.865362,
            "end_time": 1729253780.092237,
            "api_calls": {"cloudtrail.GetEventSelectors": 1},
            "scanned_resources": None,
            "failed_resources": 1,
        },
        {
            "policy": "ecc-aws-221-sns_kms_encryption_enabled",
            "region": "eu-central-1",
            "tenant_name": "TEST-TENANT",
            "customer_name": "TEST-CUSTOMER",
            "start_time": 1729253768.087648,
            "end_time": 1729253769.274071,
            "api_calls": {
                "sns.ListTopics": 1,
                "sns.GetTopicAttributes": 7,
                "tagging.GetResources": 1,
            },
            "scanned_resources": None,
            "failed_resources": 3,
        },
        {
            "policy": "ecc-aws-112-s3_bucket_versioning_mfa_delete_enabled",
            "region": "global",
            "tenant_name": "TEST-TENANT",
            "customer_name": "TEST-CUSTOMER",
            "start_time": 1729253612.897642,
            "end_time": 1729253612.904913,
            "api_calls": {},
            "scanned_resources": None,
            "failed_resources": 6,
        },
        {
            "policy": "ecc-aws-499-iam_group_has_users_check",
            "region": "global",
            "tenant_name": "TEST-TENANT",
            "customer_name": "TEST-CUSTOMER",
            "start_time": 1729253693.737499,
            "end_time": 1729253695.934546,
            "api_calls": {"iam.GetGroup": 9},
            "scanned_resources": None,
            "failed_resources": 3,
        },
        {
            "policy": "ecc-aws-516-s3_event_notifications_enabled",
            "region": "global",
            "tenant_name": "TEST-TENANT",
            "customer_name": "TEST-CUSTOMER",
            "start_time": 1729253632.1882932,
            "end_time": 1729253632.1969962,
            "api_calls": {},
            "scanned_resources": None,
            "failed_resources": 4,
        },
        {
            "policy": "ecc-aws-527-waf_global_webacl_not_empty",
            "region": "global",
            "tenant_name": "TEST-TENANT",
            "customer_name": "TEST-CUSTOMER",
            "start_time": 1729253695.95329,
            "end_time": 1729253695.95428,
            "api_calls": {},
            "error_type": PolicyErrorType.ACCESS,
            "reason": "AccessDenied Exception",
            "traceback": [],
        },
    ]


def test_iter_shard_parts_aws(aws_scan_result):
    item = JobResult(aws_scan_result, Cloud.AWS)
    parts = tuple(item.iter_shard_parts())
    assert len(parts) == 17
    dct = {}
    for part in parts:
        dct[(part.location, part.policy)] = part
    # test s3 location resolving
    assert len(dct[('eu-west-1', 'ecc-aws-112-s3_bucket_versioning_mfa_delete_enabled')].resources) == 3
    assert len(dct[('eu-west-3', 'ecc-aws-112-s3_bucket_versioning_mfa_delete_enabled')].resources) == 1
    assert len(dct[('eu-central-1', 'ecc-aws-112-s3_bucket_versioning_mfa_delete_enabled')].resources) == 1
    assert len(dct[('eu-north-1', 'ecc-aws-112-s3_bucket_versioning_mfa_delete_enabled')].resources) == 1

    assert len(dct[('global', 'ecc-aws-499-iam_group_has_users_check')].resources) == 3
    with pytest.raises(KeyError):
        _ = dct[('global', 'ecc-aws-527-waf_global_webacl_not_empty')]

    assert len(dct[('eu-central-1', 'ecc-aws-070-unused_ec2_security_groups')].resources) == 3


def test_iter_shard_parts_azure(azure_scan_result):
    item = JobResult(azure_scan_result, Cloud.AZURE)
    parts = tuple(item.iter_shard_parts())
    assert len(parts) == 8
    dct = {}
    for part in parts:
        dct[(part.location, part.policy)] = part
    assert len(dct[('global', 'ecc-azure-096-cis_sec_defender_azure_sql')].resources) == 1
    # todo add location to azure stubs
