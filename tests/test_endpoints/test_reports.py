import uuid
from datetime import datetime, timezone, timedelta

import pytest
from modular_sdk.models.tenant import Tenant
from modular_sdk.models.customer import Customer

from helpers.constants import JobState
from services import SP


@pytest.fixture()
def main_customer(mocked_mongo_client) -> Customer:
    customer = Customer(
        name='TEST_CUSTOMER',
        display_name='test customer',
        admins=[],
        is_active=True
    )
    customer.save()
    return customer


@pytest.fixture()
def aws_tenant(main_customer):
    tenant = Tenant(
        name='AWS-TESTING',
        display_name='aws testing',
        display_name_to_lower='aws testing',
        read_only=False,
        is_active=True,
        customer_name=main_customer.name,
        cloud='AWS',
        project='123456789012',
        contacts={},
        activation_date=datetime.now(timezone.utc)
    )
    tenant.save()
    return tenant


@pytest.fixture()
def aws_job(aws_tenant):
    dt = datetime.now(timezone.utc)
    job = SP.job_service.create(
        customer_name=aws_tenant.customer_name,
        tenant_name=aws_tenant.name,
        regions=['eu-central-1'],
        rulesets=['testing'],
    )
    job.save()
    SP.job_service.update(
        job=job,
        batch_job_id=str(uuid.uuid4()),
        status=JobState.SUCCEEDED,
        created_at=dt.isoformat(),
        started_at=(dt + timedelta(minutes=2)).isoformat(),
        stopped_at=(dt + timedelta(minutes=2)).isoformat(),
    )
    return job


@pytest.fixture()
def aws_job_results(s3_buckets):
    pass


def test_digest_report_job(system_user_token, sre_client, aws_job,
                           aws_job_results):
    resp = sre_client.request(
        f'/reports/digests/jobs/{aws_job.id}',
        auth=system_user_token,
        data={'customer_id': aws_job.customer_name}  # on behalf because system
    )
    # todo finish

