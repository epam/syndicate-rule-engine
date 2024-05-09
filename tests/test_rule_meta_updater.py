import os
import pytest

from lambdas.custodian_rule_meta_updater.handler import (
    RuleMetaUpdaterLambdaHandler)


@pytest.mark.skipif(os.name == 'nt', reason='The test is for Posix OS')
def test_to_rule_name_posix():
    pattern_to_test = [
        '/root/path/name_metadata.yml',
        'path/name_metadata.yml',
        'name_metadata.yml',
        'name_metadata.yaml',
        'name.yml',
        'name.yaml',
        'name'
    ]
    for pattern in pattern_to_test:
        assert RuleMetaUpdaterLambdaHandler.to_rule_name(pattern) == 'name'


@pytest.mark.skipif(os.name == 'posix', reason='The test is for NT OS')
def test_to_rule_name_nt():
    pattern_to_test = [
        'path\\name_metadata.yml',
        'C:\\Documents\\path\\name_metadata.yml',
        'name_metadata.yml',
        'name_metadata.yaml',
        'name.yml',
        'name.yaml',
        'name'
    ]
    for pattern in pattern_to_test:
        assert RuleMetaUpdaterLambdaHandler.to_rule_name(pattern) == 'name'
