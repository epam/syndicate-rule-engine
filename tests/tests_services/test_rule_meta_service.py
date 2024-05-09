from helpers.constants import RuleDomain
from services.rule_meta_service import RuleName


def test_rule_name():
    rule = RuleName('ecc-aws-001-blabla')
    assert rule.vendor == 'ecc'
    assert rule.cloud_raw == 'aws'
    assert rule.cloud == RuleDomain.AWS
    assert rule.number == '001'
    assert rule.human_name == 'blabla'
    assert rule.raw == 'ecc-aws-001-blabla'

    rule = RuleName('ecc')
    assert rule.vendor == 'ecc'
    assert rule.cloud_raw is None
    assert rule.cloud is None
    assert rule.number is None
    assert rule.human_name is None

    rule = RuleName('ecc-azure')
    assert rule.vendor == 'ecc'
    assert rule.cloud_raw == 'azure'
    assert rule.cloud == RuleDomain.AZURE
    assert rule.number is None
    assert rule.human_name is None

    rule = RuleName('ecc-gcp')
    assert rule.cloud == RuleDomain.GCP

    rule = RuleName('ecc-k8s')
    assert rule.cloud == RuleDomain.KUBERNETES

    assert RuleName('').vendor == ''
