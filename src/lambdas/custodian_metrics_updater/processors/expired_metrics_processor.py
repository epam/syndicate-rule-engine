from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse

from modular_sdk.modular import Modular

from helpers import get_logger
from helpers.constants import CAASEnv
from services import SP
from services.clients.s3 import S3Client
from services.reports import ReportMetricsService

_LOG = get_logger(__name__)

class ExpiredMetricsCleaner:
    def __init__(self, mc: Modular, s3_client: S3Client, rms: ReportMetricsService):
        self._mc = mc
        self._s3_client = s3_client
        self._rms = rms

    @classmethod
    def build(cls) -> 'ExpiredMetricsCleaner':
        return cls(
            mc = SP.modular_client,
            s3_client = SP.s3,
            rms = SP.report_metrics_service
        )
    
    @staticmethod
    def _get_bucket_and_key_from_url(url: str) -> tuple[str, str]:
        parsed_url = urlparse(url)
        
        bucket_name, key = parsed_url.path.lstrip('/').split('/', 1)

        return bucket_name, key
    
    def __call__(self, *args, **kwargs):
        """
        When this processor is executed all metrics 
        and coresponding files in s3 is deleted 
        if they older then  specified limit
        """
        try:
            till = datetime.now(timezone.utc) - timedelta(days=CAASEnv.METRICS_EXPIRATION_DAYS.as_int())

            _LOG.info(f'Cleaning expired metrics till: {till.isoformat()}')

            deleted_metrics = 0
            deleted_obj = 0
            for customer in self._mc.customer_service().i_get_customer():
                _LOG.info(f'Cleaning metrics from {customer}')
                metrics = self._rms.query_all_by_customer(
                    customer=customer,
                    till=till,
                    attributes_to_get = ['l']
                )
                if metrics is None:
                    continue
                for metric in metrics:
                    if metric.s3_url:
                        bucket, key = self._get_bucket_and_key_from_url(metric.s3_url)
                        
                        self._s3_client.delete_object(bucket=bucket, key=key)
                        deleted_obj+=1
                    self._rms.delete(metric)
                    deleted_metrics+=1
            _LOG.info(
                f'Cleaning finished. '\
                f'Deleted metrics: {deleted_metrics}. '\
                f'Deleted objects: {deleted_obj}'
            )
        except Exception as e:
            _LOG.exception(f'Exception during cleaning metrics: {e}')
        return {}