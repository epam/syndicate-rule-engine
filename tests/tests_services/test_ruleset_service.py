import pytest
from services.ruleset_service import RulesetName


def test_ruleset_name():
    assert RulesetName('FULL_AWS').name == 'FULL_AWS'

    name = RulesetName('FULL_AWS:2.4.5')
    assert name.name == 'FULL_AWS'
    assert name.version.to_str() == '2.4.5'
    assert name.license_key is None

    name = RulesetName('license_key:FULL_AWS:2.4.5')
    assert name.name == 'FULL_AWS'
    assert name.version.to_str() == '2.4.5'
    assert name.license_key == 'license_key'

    name = RulesetName('license_key:FULL_AWS')
    assert name.name == 'FULL_AWS'
    assert name.version is None
    assert name.license_key == 'license_key'

    with pytest.raises(ValueError):
        RulesetName('license_key:FULL_AWS:invalid_version')

    assert hash(RulesetName('FULL_AWS:1.0.0')) == hash(RulesetName('FULL_AWS:1.0.0'))

    assert hash(RulesetName('FULL_AWS')) == hash(RulesetName('FULL_AWS'))
