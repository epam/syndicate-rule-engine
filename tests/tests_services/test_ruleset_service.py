import pytest
from services.ruleset_service import RulesetName


def test_ruleset_name():
    assert RulesetName('FULL_AWS').name == 'FULL_AWS'

    name = RulesetName('FULL_AWS:2.4.5')
    assert name.name == 'FULL_AWS'
    assert name.version.to_str() == '2.4.5'
    assert name.license_key is None

    name = RulesetName('f197a215-9934-4950-81f0-f7e0bdb14485:FULL_AWS:2.4.5')
    assert name.name == 'FULL_AWS'
    assert name.version.to_str() == '2.4.5'
    assert name.license_key == 'f197a215-9934-4950-81f0-f7e0bdb14485'

    name = RulesetName('f197a215-9934-4950-81f0-f7e0bdb14485:FULL_AWS')
    assert name.name == 'FULL_AWS'
    assert name.version is None
    assert name.license_key == 'f197a215-9934-4950-81f0-f7e0bdb14485'

    name = RulesetName('f197a215-9934-4950-81f0-f7e0bdb14485:FULL_K8S')
    assert name.name == 'FULL_K8S'
    assert name.version is None
    assert name.license_key == 'f197a215-9934-4950-81f0-f7e0bdb14485'

    with pytest.raises(ValueError):
        RulesetName('f197a215-9934-4950-81f0-f7e0bdb14485:FULL_AWS:invalid_version')

    assert hash(RulesetName('FULL_AWS:1.0.0')) == hash(RulesetName('FULL_AWS:1.0.0'))

    assert hash(RulesetName('FULL_AWS')) == hash(RulesetName('FULL_AWS'))
