"""
Oh, yeah
"""
from helpers.constants import CustodianEndpoint


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
