from celery import Celery
from celery.schedules import crontab
from kombu import Queue

from helpers.constants import Env
from helpers.log_helper import get_logger

_LOG = get_logger(__name__)

def crontab_from_string(ct: str) -> crontab:
    """
    Taken from the newer version of celery
    """
    return crontab(*ct.split(' '))


def schedule_from_string(sch: str) -> int | crontab:
    if sch.isdigit():
        return int(sch)
    return crontab_from_string(sch)


def prepare_beat_schedule() -> dict[str, dict]:
    schedule = {
        'make-findings-snapshots': {
            'task': 'onprem.tasks.make_findings_snapshot',
            'schedule': Env.CELERY_MAKE_FINDINGS_SNAPSHOTS_SCHEDULE,
            'args': (),
        },
        'sync-license': {
            'task': 'onprem.tasks.sync_license',
            'schedule': Env.CELERY_SYNC_LICENSE_SCHEDULE,
            'args': (),
        },
        'collect-metrics': {
            'task': 'onprem.tasks.collect_metrics',
            'schedule': Env.CELERY_COLLECT_METRICS_SCHEDULE,
            'args': (),
        },
        'remove-expired-metrics': {
            'task': 'onprem.tasks.delete_expired_metrics',
            'schedule': Env.CELERY_REMOVE_EXPIRED_METRICS_SCHEDULE,
            'args': (),
        },
        'scan-resources': {
            'task': 'onprem.tasks.collect_resources',
            'schedule': Env.CELERY_SCAN_RESOURCES_SCHEDULE,
            'args': (),
        },
        'assemble-events': {
            'task': 'onprem.tasks.assemble_events',
            'schedule': Env.CELERY_ASSEMBLE_EVENTS_SCHEDULE,
            'args': (),
        },
        'clear-events': {
            'task': 'onprem.tasks.clear_events',
            'schedule': Env.CELERY_CLEAR_EVENTS_SCHEDULE,
            'args': (),
        },
    }
    disabled = []
    for name, inner in schedule.items():
        s = inner['schedule']
        if not isinstance(s, Env):
            continue
        val = s.get()
        _LOG.debug(f'Schedule {name}: env={s.name}, value={val!r}')
        if not val:  # can be forced empty from outside
            disabled.append(name)
            continue
        inner['schedule'] = schedule_from_string(val)
    for name in disabled:
        schedule.pop(name)
    _LOG.debug(f'Celery beat schedule: {schedule}')
    if disabled:
        _LOG.warning(f'Disabled tasks: {disabled}')
    return schedule


redis = Env.CELERY_BROKER_URL.get()

app = Celery(broker=redis, include=['onprem.tasks'])


app.conf.beat_schedule = prepare_beat_schedule()
app.conf.beat_scheduler = 'onprem.scheduler:MongoScheduler'

# TODO: celery docs are abstruse but this seems to work. Anyway, we should
#  pay attention
app.conf.task_create_missing_queues = True
app.conf.task_queues = (Queue('a-jobs'), Queue('b-scheduled'))
app.conf.task_routes = {
    'onprem.tasks.make_findings_snapshot': {'queue': 'b-scheduled'},
    'onprem.tasks.sync_license': {'queue': 'b-scheduled'},
    'onprem.tasks.sync_rulesource': {'queue': 'b-scheduled'},
    'onprem.tasks.collect_metrics': {'queue': 'b-scheduled'},
    'onprem.tasks.run_standard_job': {'queue': 'a-jobs'},
    'onprem.tasks.run_scheduled_job': {'queue': 'a-jobs'},
    'onprem.tasks.push_to_dojo': {'queue': 'a-jobs'},
    'onprem.tasks.delete_expired_metrics': {'queue': 'b-scheduled'},
    'onprem.tasks.collect_resources': {'queue': 'b-scheduled'},
    'onprem.tasks.run_update_metadata': {'queue': 'a-jobs'},
    'onprem.tasks.assemble_events': {'queue': 'b-scheduled'},
    'onprem.tasks.clear_events': {'queue': 'b-scheduled'},
}
app.conf.timezone = Env.CELERY_TIMEZONE.as_str()
app.conf.broker_connection_retry_on_startup = True
app.conf.worker_prefetch_multiplier = Env.CELERY_WORKER_PREFETCH_MULTIPLIER.as_int()  # https://docs.celeryq.dev/en/stable/userguide/optimizing.html#prefetch-limits
app.conf.task_compression = Env.CELERY_TASK_COMPRESSION.as_str()
app.conf.worker_max_tasks_per_child = Env.CELERY_WORKER_MAX_TASK_PER_CHILD.as_int()
app.conf.worker_log_color = False
app.conf.worker_send_task_event = False
app.conf.task_ignore_result = True  # custom results logic
app.conf.broker_transport_options = {
    'queue_order_strategy': 'sorted',
    'visibility_timeout': 3600 * 4
    + 300,  # more than hard task limit because we cannot afford to deliver one task twice
}

# app.conf.task_annotations = {
#     'onprem.tasks.run_executor': {
#         'rate_limit': '10/h'
#     }
# }
