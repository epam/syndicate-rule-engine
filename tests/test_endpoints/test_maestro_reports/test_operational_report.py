import pytest

from datetime import timedelta
from services import SP
from helpers.time_helper import utc_datetime, utc_iso
from modular_sdk.commons.constants import ParentType


@pytest.fixture()
def aws_metrics(aws_tenant, load_expected, report_bounds):
    _, end = report_bounds
    SP.s3.gz_put_json(
        bucket='metrics',
        key=f'TEST_CUSTOMER/accounts/{end.date().isoformat()}/{aws_tenant.project}.json',
        obj=load_expected('aws_account_metrics')
    )


@pytest.fixture()
def azure_metrics(azure_tenant, load_expected, report_bounds):
    _, end = report_bounds
    SP.s3.gz_put_json(
        bucket='metrics',
        key=f'TEST_CUSTOMER/accounts/{end.date().isoformat()}/{azure_tenant.project}.json',
        obj=load_expected('azure_account_metrics')
    )


@pytest.fixture()
def google_metrics(google_tenant, load_expected, report_bounds):
    _, end = report_bounds
    SP.s3.gz_put_json(
        bucket='metrics',
        key=f'TEST_CUSTOMER/accounts/{end.date().isoformat()}/{google_tenant.project}.json',
        obj=load_expected('google_account_metrics')
    )


@pytest.fixture(autouse=True)
def activate_all_tenants(main_customer):
    lic = SP.license_service.create(
        license_key='license_key',
        customer=main_customer.name,
        created_by='testing',
        expiration=utc_iso(utc_datetime() + timedelta(days=1)),
        description='Testing license',
        ruleset_ids=['TESTING'],
    )
    SP.license_service.save(lic)
    SP.modular_client.parent_service().create_all_scope(
        application_id=lic.license_key,
        customer_id=main_customer.name,
        type_=ParentType.CUSTODIAN_LICENSES,
        created_by='testing'
    ).save()


def test_operational_report(system_user_token, sre_client,
                            aws_metrics, azure_metrics,
                            google_metrics, reports_marker):
    resp = sre_client.request(
        "/reports/operational",
        "POST",
        auth=system_user_token,
        data={
            "customer_id": "TEST_CUSTOMER",
            "tenant_names": ['AWS-TESTING', 'AZURE-TESTING', 'GOOGLE-TESTING']
        }
    )
    assert resp.status_int == 503
