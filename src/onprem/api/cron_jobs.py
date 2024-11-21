import importlib
from typing import Callable

from apscheduler.triggers.interval import IntervalTrigger

from helpers import RequestContext
from helpers.log_helper import get_logger
from services import SERVICE_PROVIDER

_LOG = get_logger(__name__)


def sync_license():
    """
    This is a scheduled job itself. It must be in a separate module
    from __main__ in case we have multiple possible __main__ (s). And we do
    """
    license_module = importlib.import_module(
        'lambdas.custodian_license_updater.handler'
    )
    license_module.lambda_handler(event={}, context=RequestContext())


def make_findings_snapshot():
    module = importlib.import_module(
        'lambdas.custodian_metrics_updater.processors.findings_processor'
    )
    module.FINDINGS_UPDATER.process_data(
        event={'data_type': 'findings'},
    )


def metrics_pipeline():
    module = importlib.import_module(
        'lambdas.custodian_metrics_updater.handler'
    )
    module.HANDLER.lambda_handler(event={'data_type': 'metrics'}, context=RequestContext())


# def diagnostic_pipeline():
#     module = importlib.import_module(
#         'lambdas.custodian_metrics_updater.handler'
#     )
#     module.HANDLER.lambda_handler(event={'data_type': 'diagnostic'}, context=RequestContext())

# TODO: maybe rewrite to use celery or something
def ensure_job(name: str, method: Callable, hours: int):
    """
    Recreates a job because the server is not restarted often but changes
    in case can cause the old job to throw exceptions
    """
    from helpers.system_customer import SYSTEM_CUSTOMER
    from models.scheduled_job import ScheduledJob

    scheduler = SERVICE_PROVIDER.ap_job_scheduler.scheduler
    _LOG.info(f'Registering {name} job')
    scheduler.add_job(method, id=name,
                      trigger=IntervalTrigger(hours=hours))
    item = ScheduledJob(id=name)
    item.update(actions=[
        ScheduledJob.customer_name.set(SYSTEM_CUSTOMER),
        ScheduledJob.context.schedule.set(
            f'rate({hours} {"hour" if hours == 1 else "hours"})'
        )
    ])


def ensure_all():
    from helpers.system_customer import SYSTEM_CUSTOMER
    from models.scheduled_job import ScheduledJob
    for item in ScheduledJob.customer_name_principal_index.query(hash_key=SYSTEM_CUSTOMER):
        item.delete()
    jobs = {
        'custodian-system-license-sync-job': (sync_license, 3),
        'custodian-system-snapshot-findings': (make_findings_snapshot, 4),
        'custodian-system-metrics-update': (metrics_pipeline, 12),
        # 'custodian-system-diagnostic-update': (diagnostic_pipeline, 12)  # todo use cron for these
    }
    for name, data in jobs.items():
        ensure_job(name, data[0], data[1])
