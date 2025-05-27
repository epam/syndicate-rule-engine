from celery import Celery
from celery.schedules import crontab

from helpers.constants import CAASEnv
from kombu import Queue

redis = CAASEnv.CELERY_BROKER_URL.get()

app = Celery(broker=redis,
             include=['onprem.tasks'])

app.conf.beat_schedule = {
    'make-findings-snapshots': {
        'task': 'onprem.tasks.make_findings_snapshot',
        'schedule': crontab(minute='0', hour='*/4'),
        'args': ()
    },
    'sync-license': {
        'task': 'onprem.tasks.sync_license',
        'schedule': 3600 * 4,
        'args': ()
    },
    'collect-metrics': {
        'task': 'onprem.tasks.collect_metrics',
        'schedule': crontab(minute='0', hour='3,15'),
        'args': ()
    },
    'remove-expired-metrics': {
        'task': 'onprem.tasks.delete_expired_metrics',
        'schedule': 20,
        'args': ()
    }
}
app.conf.beat_scheduler = 'onprem.scheduler:MongoScheduler'

# TODO: celery docs are abstruse but this seems to work. Anyway, we should
#  pay attention
app.conf.task_create_missing_queues = True
app.conf.task_queues = (
    Queue("a-jobs"),
    Queue("b-scheduled"),
)
app.conf.task_routes = {
    'onprem.tasks.make_findings_snapshot': {
        'queue': 'b-scheduled'
    },
    'onprem.tasks.sync_license': {
        'queue': 'b-scheduled'
    },
    'onprem.tasks.collect_metrics': {
        'queue': 'b-scheduled'
    },
    'onprem.tasks.run_standard_job': {
        'queue': 'a-jobs'
    },
    'onprem.tasks.run_scheduled_job': {
        'queue': 'a-jobs'
    },
    'onprem.tasks.delete_expired_metrics': {
        'queue': 'a-jobs'
    }
}
app.conf.timezone = 'UTC'
app.conf.broker_connection_retry_on_startup = True
app.conf.worker_prefetch_multiplier = 1  # https://docs.celeryq.dev/en/stable/userguide/optimizing.html#prefetch-limits
app.conf.task_compression = 'gzip'
app.conf.worker_max_tasks_per_child = 16
app.conf.worker_log_color = False
app.conf.worker_send_task_event = False
app.conf.task_ignore_result = True  # custom results logic
app.conf.broker_transport_options = {
    "queue_order_strategy": "sorted",
    "visibility_timeout": 3600 * 4 + 300  # more than hard task limit because we cannot afford to deliver one task twice
}

# app.conf.task_annotations = {
#     'onprem.tasks.run_executor': {
#         'rate_limit': '10/h'
#     }
# }
