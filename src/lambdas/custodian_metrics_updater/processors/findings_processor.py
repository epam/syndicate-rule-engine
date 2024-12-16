from pathlib import PurePosixPath

from helpers import get_logger
from services import SP
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService
from services.reports_bucket import ReportsBucketKeysBuilder

_LOG = get_logger(__name__)


class FindingsUpdater:
    def __init__(self, s3_client: S3Client,
                 environment_service: EnvironmentService):
        self._s3_client = s3_client
        self._environment_service = environment_service

    @classmethod
    def build(cls) -> 'FindingsUpdater':
        return cls(
            s3_client=SP.s3,
            environment_service=SP.environment_service
        )

    def __call__(self, *args, **kwargs):
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
        return {}
