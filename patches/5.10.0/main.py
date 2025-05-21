import logging
import os
import re
import sys

from boto3.resources.base import ServiceResource
from boto3.session import Session
from botocore.config import Config

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(name)s.%(funcName)s:%(lineno)d %(message)s',
    level=logging.INFO,
)
_LOG = logging.getLogger(__name__)

TAGS = [{'Key': 'Type', 'Value': 'DataSnapshot'}]


def _init_minio() -> ServiceResource:
    endpoint = os.environ.get('SRE_MINIO_ENDPOINT')
    access_key = os.environ.get('SRE_MINIO_ACCESS_KEY_ID')
    secret_key = os.environ.get('SRE_MINIO_SECRET_ACCESS_KEY')
    assert endpoint, 'minio endpoint is required'
    assert access_key, 'minio access key is required'
    assert secret_key, 'minio secret key is required'
    resource = Session().resource(
        service_name='s3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint,
        config=Config(
            s3={'signature_version': 's3v4', 'addressing_style': 'path'}
        ),
    )
    _LOG.info('Minio connection was successfully initialized')
    return resource.meta.client


def patch() -> None:
    """
    This patch tags all the files with `snapshots/*` or `*/snapshots/*` prefixes as `Type: DataSnapshot`
    """
    _LOG.info('Starting patch')

    client = _init_minio()
    params = {
        'Bucket': os.environ.get('SRE_REPORTS_BUCKET_NAME'),
        'MaxKeys': 1000,
    }
    assert params.get('Bucket')

    _LOG.debug(f'Bucket: {params["Bucket"]}')

    count = 0
    while True:
        response = client.list_objects_v2(**params)

        for item in response['Contents']:
            if re.match(r'(^|.*/)snapshots/.*', item['Key']):
                client.put_object_tagging(
                    Bucket=params['Bucket'],
                    Key=item['Key'],
                    Tagging={'TagSet': TAGS},
                )
                count += 1

        if response.get('IsTruncated'):
            params['ContinuationToken'] = response['NextContinuationToken']
        else:
            break

    _LOG.info(f'Patch has finished. Tagged: {count} files')


def main() -> int:
    try:
        patch()
        return 0
    except Exception:
        _LOG.exception('Unexpected exception')
        return 1


if __name__ == '__main__':
    sys.exit(main())
