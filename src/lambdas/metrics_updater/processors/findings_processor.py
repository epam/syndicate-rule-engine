from pathlib import PurePosixPath
from typing import MutableMapping, Optional

from typing_extensions import Self

from helpers import RequestContext, get_logger
from helpers.constants import START_DATE, Env
from lambdas.metrics_updater.processors.base import (
    BaseProcessor,
    NextLambdaEvent,
)
from services import SP
from services.clients.s3 import S3Client
from services.reports_bucket import ReportsBucketKeysBuilder


NEXT_DATA_TYPE = "recommendations"

_LOG = get_logger(__name__)


class FindingsUpdater(BaseProcessor):
    processor_name = "findings"

    def __init__(self, s3_client: S3Client):
        self._s3_client = s3_client

    @classmethod
    def build(cls) -> Self:
        return cls(s3_client=SP.s3)

    def __call__(
        self,
        event: Optional[MutableMapping] = None,
        context: Optional[RequestContext] = None,
    ) -> Optional[NextLambdaEvent]:
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

        if event:
            return self._return_next_event(
                current_event=event,
                next_processor_name=NEXT_DATA_TYPE,
            )