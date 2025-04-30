from celery.schedules import crontab, schedule

from services import SP


def test_celery_schedule_to_str_rate():
    sjs = SP.scheduled_job_service
    assert sjs.celery_schedule_to_str(schedule(7200)) == 'rate(2 hours)'
    assert sjs.celery_schedule_to_str(schedule(300)) == 'rate(5 minutes)'
    assert sjs.celery_schedule_to_str(schedule(3600 * 48)) == 'rate(2 days)'
    assert sjs.celery_schedule_to_str(schedule(3600 * 24)) == 'rate(1 day)'
    assert sjs.celery_schedule_to_str(schedule(60)) == 'rate(1 minute)'


def test_celery_schedule_to_str_cron():
    sjs = SP.scheduled_job_service
    assert sjs.celery_schedule_to_str(crontab()) == 'cron(* * * * *)'
    assert sjs.celery_schedule_to_str(
        crontab(minute=0, hour='*/3')) == 'cron(0 */3 * * *)'
    assert sjs.celery_schedule_to_str(
        crontab(0, 0, day_of_month='2')) == 'cron(0 0 2 * *)'
