import os
from unittest.mock import patch

from helpers.constants import CustodianEndpoint, EnvEnum


def test_custodian_endpoint_match():
    assert CustodianEndpoint.match('jobs') == CustodianEndpoint.JOBS
    assert CustodianEndpoint.match('jobs/') == CustodianEndpoint.JOBS
    assert CustodianEndpoint.match('/jobs/') == CustodianEndpoint.JOBS
    assert CustodianEndpoint.match(
        '/jobs/{job_id}') == CustodianEndpoint.JOBS_JOB
    assert CustodianEndpoint.match(
        '/jobs/{job_id}/') == CustodianEndpoint.JOBS_JOB
    assert CustodianEndpoint.match(
        'jobs/{job_id}/') == CustodianEndpoint.JOBS_JOB


def test_env_enum():
    class MyEnvs(EnvEnum):
        FOO = 'MY_FOO'
        BAR = 'MY_BAR', 'bar_default'

    with patch('os.environ', {}):
        assert MyEnvs.FOO.get() is None
        assert MyEnvs.FOO.get('default') == 'default'
        assert MyEnvs.BAR.get() == 'bar_default'
        assert MyEnvs.BAR.get(None) is None
        assert MyEnvs.BAR.get('') == ''
        assert MyEnvs.BAR.get('default') == 'default'

    with patch('os.environ', {'MY_FOO': 'foo', 'MY_BAR': ''}):
        assert MyEnvs.FOO.get() == 'foo'
        assert MyEnvs.BAR.get() == ''
        assert MyEnvs.BAR.get('default') == ''

    with patch('os.environ', {}):
        assert MyEnvs.FOO.get() is None
        MyEnvs.FOO.set('foo')
        assert MyEnvs.FOO.get() == 'foo'
        MyEnvs.FOO.set(12)
        assert MyEnvs.FOO.get() == '12'
        MyEnvs.FOO.set(None)
        assert MyEnvs.FOO.get() is None
        assert MyEnvs.FOO not in os.environ


def test_envs_enum_compatibility():
    class MyEnvs(EnvEnum):
        FOO = 'MY_FOO'
        BAR = 'MY_BAR'
    with patch('os.environ', {}):
        os.environ[MyEnvs.FOO] = 'foo'
        assert os.environ[MyEnvs.FOO] == 'foo'
        assert os.environ[MyEnvs.FOO.value] == 'foo'
        assert os.environ['MY_FOO'] == 'foo'

    with patch('os.environ', {'MY_FOO': 'foo'}):
        assert MyEnvs.FOO in os.environ
        assert MyEnvs.FOO.value in os.environ
        assert MyEnvs.FOO.get() == 'foo'
        assert os.environ.get('MY_FOO') == 'foo'
        assert os.environ.get(MyEnvs.FOO) == 'foo'
        assert os.environ.get(MyEnvs.FOO.value) == 'foo'
