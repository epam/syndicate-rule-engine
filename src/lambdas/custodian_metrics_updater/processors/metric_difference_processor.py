import json
from datetime import datetime, timedelta

from dateutil.relativedelta import SU, relativedelta

from helpers import get_logger
from helpers.constants import ATTACK_VECTOR_TYPE, START_DATE, DATA_TYPE
from helpers.time_helper import utc_datetime
from helpers.utils import get_last_element
from helpers.difference import calculate_dict_diff
from services import SERVICE_PROVIDER
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService
from services.setting_service import SettingsService

_LOG = get_logger(__name__)

TENANT_METRICS_PATH = '{customer}/tenants/{date}'
DELIMITER = '/'
NEXT_STEP = 'tenants'


class TenantMetricsDifference:
    def __init__(self, s3_client: S3Client,
                 environment_service: EnvironmentService,
                 settings_service: SettingsService):
        self.s3_client = s3_client
        self.environment_service = environment_service
        self.settings_service = settings_service

        self.TO_UPDATE_MARKER = False

        self.today_date = datetime.utcnow().date()
        self.today_midnight = datetime.combine(self.today_date,
                                               datetime.min.time())
        self.yesterday = self.today_date - timedelta(days=1)

        self._date_marker = self.settings_service.get_report_date_marker()
        self.current_week_date = self._date_marker.get('current_week_date')
        self.last_week_date = self._date_marker.get('last_week_date')
        self.last_week_date = utc_datetime(
            self.last_week_date if self.last_week_date else (
                self.today_date - relativedelta(weekday=SU(-1))
            ).date().isoformat(), utc=False)

    def process_data(self, event):
        today = datetime.utcnow().today()
        s3_object_date = self.current_week_date
        if self.current_week_date <= self.yesterday.isoformat():
            self.TO_UPDATE_MARKER = True

        date = self.settings_service.get_report_date_marker()
        last_week_date = date.get('last_week_date')
        if not last_week_date:
            _LOG.error('Cannot get \'last_week_date\' param from report date '
                       'market setting. Resolving...')
            sun_offset = (today.weekday() - 6) % 7
            last_week_date = str((today - timedelta(days=sun_offset)).date().
                                 isoformat())

        _LOG.debug(f'Last week date: {last_week_date}')
        metrics_bucket = self.environment_service.get_metrics_bucket_name()

        customers = set(
            customer.split('/')[0] for customer in
            self.s3_client.list_dir(bucket_name=metrics_bucket)
        )

        _LOG.debug(f'Received customers: {customers}')
        for customer in customers:
            _LOG.debug(f'Processing customer {customer}')
            current_metric_filenames = list(self.s3_client.list_dir(
                bucket_name=metrics_bucket,
                key=TENANT_METRICS_PATH.format(customer=customer,
                                               date=s3_object_date)))
            if not current_metric_filenames:
                _LOG.warning(
                    f'There is no tenant metrics for customer {customer}; '
                    f'date: {s3_object_date}')
                continue

            previous_metric_full_filenames = list(self.s3_client.list_dir(
                bucket_name=metrics_bucket,
                key=TENANT_METRICS_PATH.format(customer=customer,
                                               date=last_week_date)))
            previous_metric_filenames = [get_last_element(
                f, DELIMITER) for f in previous_metric_full_filenames]
            for file in current_metric_filenames:
                if (tenant_file := get_last_element(file, DELIMITER)) in \
                        previous_metric_filenames:
                    index = previous_metric_filenames.index(tenant_file)
                    previous = self.s3_client.get_json_file_content(
                        bucket_name=metrics_bucket,
                        full_file_name=previous_metric_full_filenames[index])
                    previous_metric_filenames.pop(index)
                else:
                    _LOG.debug(f'Previous metrics for tenant {tenant_file} '
                               f'were not found')
                    previous = {}
                current = self.s3_client.get_json_file_content(
                    bucket_name=metrics_bucket, full_file_name=file)
                diff = calculate_dict_diff(current, previous,
                                           exclude=ATTACK_VECTOR_TYPE)
                self.s3_client.put_object(bucket_name=metrics_bucket,
                                          object_name=file,
                                          body=json.dumps(diff))
                if self.TO_UPDATE_MARKER:
                    obj_name = TENANT_METRICS_PATH.format(
                        customer=customer, date=last_week_date)
                    self.s3_client.put_object(
                        bucket_name=metrics_bucket,
                        object_name=f'{obj_name}/{tenant_file}',
                        body=json.dumps(diff))

        # Monday
        if self.TO_UPDATE_MARKER:
            new_week_date = (self.today_date + relativedelta(
                weekday=SU(0))).isoformat()
            _LOG.debug(f'Rewriting report date marker setting. New date: '
                       f'{new_week_date}')
            self.settings_service.set_report_date_marker(
                    current_week_date=new_week_date,
                    last_week_date=self.current_week_date)

        next_step = {}
        continuously_update = event.get('continuously')
        if continuously_update:
            start_date = event.get(START_DATE)
            start_date = (utc_datetime(start_date, utc=False) + timedelta(days=1)).date()
            if self.today_date <= start_date:
                next_step = {DATA_TYPE: NEXT_STEP, START_DATE: start_date,
                             'continuously': True}
        return next_step


TENANT_METRICS_DIFF = TenantMetricsDifference(
    s3_client=SERVICE_PROVIDER.s3(),
    environment_service=SERVICE_PROVIDER.environment_service(),
    settings_service=SERVICE_PROVIDER.settings_service()
)
