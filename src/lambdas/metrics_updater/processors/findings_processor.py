from pathlib import PurePosixPath

from helpers import get_logger
from helpers.constants import Env
from services import SP
from services.clients.s3 import S3Client
from services.reports_bucket import ReportsBucketKeysBuilder

_LOG = get_logger(__name__)


class FindingsUpdater:
    def __init__(self, s3_client: S3Client):
        self._s3_client = s3_client

    @classmethod
    def build(cls) -> 'FindingsUpdater':
        return cls(s3_client=SP.s3)

    def __call__(self, *args, **kwargs):
        """
        When this processor is executed we make a snapshot of existing
        findings for specified tenants.
        :param event:
        :return:
        """
        bucket = Env.REPORTS_BUCKET_NAME.as_str()
        prefixes = self._s3_client.common_prefixes(
            bucket=bucket,
            delimiter=ReportsBucketKeysBuilder.latest,
            prefix=ReportsBucketKeysBuilder.prefix,
        )
        for prefix in prefixes:
            _LOG.debug(f'Processing key: {prefix}')
            objects = self._s3_client.list_objects(
                bucket=bucket, prefix=prefix
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
                    ReportsBucketKeysBuilder.datetime(),
                )
                destination += path.name
                _LOG.debug(f'Copying {key} to {destination}')
                self._s3_client.copy(
                    bucket=bucket,
                    key=key,
                    destination_bucket=bucket,
                    destination_key=destination,
                    destination_tags={'Type': 'DataSnapshot'},
                )
        return {}
