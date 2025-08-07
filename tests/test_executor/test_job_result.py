from pathlib import Path

import pytest

from executor.helpers.constants import Cloud
from executor.services.report_service import JobResult
from helpers.constants import PolicyErrorType


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


def test_rules_meta_aws(aws_scan_result: Path, load_expected):
    item = JobResult(aws_scan_result, Cloud.AWS)
    meta = item.rules_meta()
    assert len(meta) == 15  # that is a number of unique rules in our stubs
    assert meta == load_expected('aws_rules_meta')


def test_statistics_aws(aws_tenant, aws_scan_result, load_expected):
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
    assert len(stats) == 16
    def k(i):
        return i['policy'], i['region']
    print(sorted(stats, key=k))
    assert sorted(stats, key=k) == sorted(load_expected('aws_job_statistics'), key=k)


def test_iter_shard_parts_aws(aws_scan_result):
    item = JobResult(aws_scan_result, Cloud.AWS)
    parts = tuple(item.iter_shard_parts({}))
    assert len(parts) == 23
    dct = {}
    for part in parts:
        dct[(part.location, part.policy)] = part
    # test s3 location resolving
    assert len(dct[('eu-west-1', 'ecc-aws-112-s3_bucket_versioning_mfa_delete_enabled')].resources) == 3
    assert len(dct[('eu-west-3', 'ecc-aws-112-s3_bucket_versioning_mfa_delete_enabled')].resources) == 1
    assert len(dct[('eu-central-1', 'ecc-aws-112-s3_bucket_versioning_mfa_delete_enabled')].resources) == 1
    assert len(dct[('eu-north-1', 'ecc-aws-112-s3_bucket_versioning_mfa_delete_enabled')].resources) == 1

    assert len(dct[('global', 'ecc-aws-499-iam_group_has_users_check')].resources) == 3
    assert dct[('global', 'ecc-aws-527-waf_global_webacl_not_empty')].error is not None

    assert len(dct[('eu-central-1', 'ecc-aws-070-unused_ec2_security_groups')].resources) == 3


def test_iter_shard_parts_azure(azure_scan_result):
    item = JobResult(azure_scan_result, Cloud.AZURE)
    parts = tuple(item.iter_shard_parts({}))
    assert len(parts) == 10
    dct = {}
    for part in parts:
        dct[(part.location, part.policy)] = part
    assert len(dct[('global', 'ecc-azure-096-cis_sec_defender_azure_sql')].resources) == 1
    # todo add location to azure stubs
