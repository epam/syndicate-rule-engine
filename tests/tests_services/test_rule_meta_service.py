from helpers.constants import RuleDomain
from models.rule import Rule
from services.rule_meta_service import RuleName
from services.rule_meta_service import RuleService


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

def test_rule_without_duplicates():
    rules = [
        Rule(id='customer#AWS#name_1#000001.000002.000003'),
        Rule(id='customer#AWS#name_1#000001.000005.000012'),
        Rule(id='customer#AWS#name_1#000001.000012.000003'),
        Rule(id='customer#AWS#name_2#000003.000002.000003'),
        Rule(id='customer#AWS#name_2#000012.000002.000004'),
    ]
    
    distinct_rules = list(RuleService.without_duplicates(rules, '1.2.3'))
    assert len(distinct_rules) == 2
    assert {rule.id for rule in distinct_rules} == \
        {'customer#AWS#name_1#000001.000002.000003', 'customer#AWS#name_2#000012.000002.000004'}
    
    distinct_rules = list(RuleService.without_duplicates(rules))
    assert len(distinct_rules) == 2
    assert {rule.id for rule in distinct_rules} == \
        {'customer#AWS#name_1#000001.000012.000003', 'customer#AWS#name_2#000012.000002.000004'}