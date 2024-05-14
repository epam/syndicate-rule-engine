from helpers import get_logger

_LOG = get_logger(__name__)
LIMIT = 30
RETRY_CRON_NAME_PATTERN = 'report_retry_'


# def create_retry_job(event):
#     report_module = importlib.import_module(
#         'lambdas.custodian_report_generation_handler.handler')
#     report_module.lambda_handler(event=event, context=RequestContext())


# def ensure_retry_job():
#     """
#     Make sure you've started the scheduler and set envs before invoking
#     this function
#     """
#     scheduler: BackgroundScheduler = SERVICE_PROVIDER.ap_job_scheduler(). \
#         scheduler
#     _settings_service = SERVICE_PROVIDER.settings_service()
#     _report_statistics_service = SERVICE_PROVIDER.report_statistics_service()
#     _entity_schedule_mapping = {}
#
#     if not _settings_service.get_send_reports():
#         _LOG.info('Send report setting is disabled. Cannot run retry jobs.')
#         return
#
#     _item, cursor = _report_statistics_service.get_all_not_sent_retries(
#         limit=LIMIT, only_without_status=True)
#     while cursor:
#         new_items, cursor = _report_statistics_service.get_all_not_sent_retries(
#             limit=LIMIT, last_evaluated_key=cursor, only_without_status=True)
#         _item.append(new_items)
#     if not _item:
#         _LOG.info('No need to run retry jobs.')
#     else:
#         for i in _item:
#             entity = f'{i.customer}_{i.tenant}_{i.report_type}'
#             if scheduler.get_job(job_id=i.id):
#                 _LOG.debug(f'Schedule job `{i.id}` already exists')
#                 _entity_schedule_mapping[entity] = i.id
#             else:
#                 if entity in _entity_schedule_mapping:
#                     _LOG.debug(f'There is already a cron for entity {entity}')
#                 elif _settings_service.max_cron_number() > len(scheduler.get_jobs()):
#                     _LOG.debug('Maximum number of crons reached!')
#                     break
#                 else:
#                     hours = (i.attempt * 15) // 60
#                     params = {
#                         'kwargs': {'event': i.get_json().get('event', {})},
#                         'id': i.id, 'name': RETRY_CRON_NAME_PATTERN + i.id,
#                         'trigger': 'interval',
#                         'minutes': (i.attempt * 15) % 60
#                         }
#                     if hours > 0:
#                         params.update(hours=hours)
#                     scheduler.add_job(create_retry_job, **params)
#                     _entity_schedule_mapping[entity] = i.id
