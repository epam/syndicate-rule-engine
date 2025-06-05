from datetime import timedelta

from modular_sdk.modular import ModularServiceProvider

from helpers import get_logger
from helpers.constants import CAASEnv
from helpers.time_helper import utc_datetime
from models.metrics import ReportMetrics
from services import SP
from services.clients.s3 import S3Client, S3Url
from services.reports import ReportMetricsService

_LOG = get_logger(__name__)


class ExpiredMetricsCleaner:
    def __init__(
        self,
        mc: ModularServiceProvider,
        s3_client: S3Client,
        rms: ReportMetricsService,
    ):
        self._mc = mc
        self._s3_client = s3_client
        self._rms = rms

    @classmethod
    def build(cls) -> 'ExpiredMetricsCleaner':
        return cls(
            mc=SP.modular_client,
            s3_client=SP.s3,
            rms=SP.report_metrics_service,
        )

    def __call__(self, *args, **kwargs):
        """
        When this processor is executed all metrics
        and coresponding files in s3 is deleted
        if they older then  specified limit
        """
        expiration = CAASEnv.METRICS_EXPIRATION_DAYS.get()
        if expiration is None or not expiration.isalnum():
            _LOG.info('Expiration env is not set. Metrics won`t be removed')
            return
        till = utc_datetime() - timedelta(days=int(expiration))

        _LOG.info(f'Cleaning expired metrics till: {till.isoformat()}')

        deleted_metrics = 0
        deleted_obj = 0
        for customer in self._mc.customer_service().i_get_customer():
            _LOG.info(f'Cleaning metrics from {customer}')
            metrics = self._rms.query_all_by_customer(
                customer=customer,
                till=till,
                attributes_to_get=[ReportMetrics.s3_url],
            )
            to_remove = []
            for metric in metrics:
                if metric.s3_url:
                    u = S3Url(metric.s3_url)
                    self._s3_client.delete_object(bucket=u.bucket, key=u.key)
                    deleted_obj += 1
                to_remove.append(metric)

            self._rms.batch_delete(to_remove)
            deleted_metrics += len(to_remove)
        _LOG.info(
            f'Cleaning finished. '
            f'Deleted metrics: {deleted_metrics}. '
            f'Deleted objects: {deleted_obj}'
        )
