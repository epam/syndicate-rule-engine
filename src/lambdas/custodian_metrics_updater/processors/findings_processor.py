from pathlib import PurePosixPath

from helpers import get_logger
from helpers.constants import DATA_TYPE, START_DATE
from services import SERVICE_PROVIDER
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService
from services.reports_bucket import ReportsBucketKeysBuilder

_LOG = get_logger(__name__)
NEXT_STEP = 'recommendations'


class FindingsUpdater:
    def __init__(self, s3_client: S3Client,
                 environment_service: EnvironmentService):
        self._s3_client = s3_client
        self._environment_service = environment_service

    def process_data(self, event: dict):
        """
        When this processor is executed we make a snapshot of existing
        findings for specified tenants.
        :param event:
        :return:
        """
        bucket = self._environment_service.default_reports_bucket_name()
        prefixes = self._s3_client.common_prefixes(
            bucket=bucket,
            delimiter=ReportsBucketKeysBuilder.latest,
            prefix=ReportsBucketKeysBuilder.prefix
        )
        for prefix in prefixes:
            _LOG.debug(f'Processing key: {prefix}')
            objects = self._s3_client.list_objects(
                bucket=bucket,
                prefix=prefix,
            )
            for obj in objects:
                # prefix: /bla/bla/latest
                # key: /bla/bla/latest/1.json.gz
                # destination: /bla/bla/snapshots/2023/10/10/10/1.json.gz
                key = obj.key
                path = PurePosixPath(key)
                destination = ReportsBucketKeysBuilder.urljoin(
                    str(path.parent.parent),
                    ReportsBucketKeysBuilder.snapshots,
                    ReportsBucketKeysBuilder.datetime()
                )
                destination += path.name
                _LOG.debug(f'Copying {key} to {destination}')
                self._s3_client.copy(
                    bucket=bucket,
                    key=key,
                    destination_bucket=bucket,
                    destination_key=destination
                )
        return {DATA_TYPE: NEXT_STEP, START_DATE: event.get(START_DATE),
                'continuously': event.get('continuously')}


FINDINGS_UPDATER = FindingsUpdater(
    s3_client=SERVICE_PROVIDER.s3,
    environment_service=SERVICE_PROVIDER.environment_service
)
