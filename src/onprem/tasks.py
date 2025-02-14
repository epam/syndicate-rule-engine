from executor.job import main
from helpers import RequestContext
from lambdas.custodian_license_updater.handler import LicenseUpdater
from lambdas.custodian_metrics_updater.handler import MetricsUpdater
from lambdas.custodian_metrics_updater.processors.findings_processor import \
    FindingsUpdater
from onprem.celery import app


@app.task
def run_executor(environment: dict[str, str]):
    return main(environment)


@app.task
def make_findings_snapshot():
    FindingsUpdater.build().__call__()


@app.task
def sync_license(license_keys: list[str] | None = None):
    event = {}
    if license_keys:
        event['license_key'] = license_keys
    LicenseUpdater.build().lambda_handler(event=event, context=RequestContext())


@app.task
def collect_metrics():
    MetricsUpdater.build().lambda_handler(event={'data_type': 'metrics'},
                                          context=RequestContext())
