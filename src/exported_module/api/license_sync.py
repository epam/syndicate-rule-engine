import importlib

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from helpers import RequestContext
from helpers.log_helper import get_logger
from services import SERVICE_PROVIDER

LICENSE_SYNC_SCHEDULED_JOB_NAME = 'custodian-system-license-sync-job'

_LOG = get_logger(__name__)


def sync_license():
    """
    This is a scheduled job itself. It must be in a separate module
    from __main__ in case we have multiple possible __main__ (s). And we do
    """
    license_module = importlib.import_module(
        'lambdas.custodian_license_updater.handler')
    license_module.lambda_handler(event={}, context=RequestContext())


def ensure_license_sync_job(schedule_hours: int = 3):
    """
    Make sure you've started the scheduler and set envs before invoking
    this function
    """
    from helpers.system_customer import SYSTEM_CUSTOMER

    scheduler: BackgroundScheduler = SERVICE_PROVIDER.ap_job_scheduler(). \
        scheduler
    _job = scheduler.get_job(LICENSE_SYNC_SCHEDULED_JOB_NAME)
    if not _job:
        _LOG.info('License sync scheduled job not found, registering one')
        scheduler.add_job(sync_license, id=LICENSE_SYNC_SCHEDULED_JOB_NAME,
                          trigger=IntervalTrigger(hours=schedule_hours))
    else:  # exists
        _LOG.info('License sync scheduled job has already been registered. '
                  'Schedule hours cli param won`t be used to reschedule the '
                  'job. Remove the job and restart the server if you want '
                  'to reschedule it.')
    from models.scheduled_job import ScheduledJob

    _item = ScheduledJob.get(LICENSE_SYNC_SCHEDULED_JOB_NAME)
    _item.update_with(
        customer=SYSTEM_CUSTOMER,
        schedule=f'rate({schedule_hours} '
                 f'{"hour" if schedule_hours == 1 else "hours"})'
    )
    _item.save()
