from celery import Celery
from celery.schedules import crontab
from dotenv import load_dotenv

from helpers.constants import CAASEnv

load_dotenv(verbose=True)

redis = CAASEnv.CELERY_BROKER_URL.get()

app = Celery(broker=redis,
             include=['onprem.tasks'])

app.conf.beat_schedule = {
    'make-findings-snapshots-every-4-hours': {
        'task': 'onprem.tasks.make_findings_snapshot',
        'schedule': crontab(minute='0', hour='*/4'),
        'args': ()
    },
    'sync-license-every-4-hours': {
        'task': 'onprem.tasks.sync_license',
        'schedule': 3600 * 4,
        'args': ()
    },
    'collect-metrics-twice-a-day': {
        'task': 'onprem.tasks.collect_metrics',
        'schedule': crontab(minute='0', hour='1,13'),
        'args': ()
    }
}
app.conf.timezone = 'UTC'
app.conf.broker_connection_retry_on_startup = True
app.conf.worker_prefetch_multiplier = 1  # https://docs.celeryq.dev/en/stable/userguide/optimizing.html#prefetch-limits
app.conf.task_compression = 'gzip'
app.conf.worker_max_tasks_per_child = 8
app.conf.worker_log_color = False
app.conf.worker_send_task_event = False
app.conf.task_ignore_result = True  # custom results logic

# app.conf.task_annotations = {
#     'onprem.tasks.run_executor': {
#         'rate_limit': '10/h'
#     }
# }
