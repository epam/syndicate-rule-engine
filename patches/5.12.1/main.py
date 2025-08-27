import logging
from itertools import chain
from typing import Generator
import sys
from helpers.constants import Env, Cloud, GLOBAL_REGION
from services.sharding import ShardsCollectionFactory, ShardsS3IO, ShardsCollection, ShardPart
from services import SP
from services.reports_bucket import ReportsBucketKeysBuilder


logging.basicConfig(
    format='%(asctime)s %(levelname)s %(name)s.%(funcName)s:%(lineno)d %(message)s',
    level=logging.INFO,
)
_LOG = logging.getLogger(__name__)

def iter_shard_prefixes() -> Generator[str, None, None]:
    return SP.s3.common_prefixes(
        bucket=Env.REPORTS_BUCKET_NAME.as_str(),
        delimiter=ReportsBucketKeysBuilder.latest,
        prefix=ReportsBucketKeysBuilder.prefix
    )


def get_prefix_cloud(root: str) -> Cloud:
    if '/AWS/' in root:
        return Cloud.AWS
    if '/AZURE/' in root:
        return Cloud.AZURE
    if '/GOOGLE/' in root:
        return Cloud.GOOGLE
    if '/KUBERNETES/' in root:
        return Cloud.KUBERNETES
    raise RuntimeError(f'Not supported prefix: {root}')


def patch_aws_collection(collection: ShardsCollection) -> ShardsCollection | None:
    new = ShardsCollectionFactory.from_cloud(Cloud.AWS)
    new.io = collection.io
    mapping = {}
    changed = False
    for part in collection.iter_all_parts():
        if collection.meta[part.policy]['resource'] not in ('s3', 'aws.s3'):
            new.put_part(part)
            _LOG.debug('Skipping non s3 part')
            continue
        # s3 policy, need a mapping
        mapping.setdefault(part.policy, {})[part.location] = part
    for policy in mapping:
        if len(mapping[policy]) == 1 and GLOBAL_REGION in mapping[policy]:
            new.put_part(mapping[policy][GLOBAL_REGION])
            continue
        # need to merge
        changed = True
        parts = tuple(mapping[policy].values())
        _LOG.debug(f'Going to merge {len(parts)} parts for policy {policy}')
        new.put_part(ShardPart(
            policy=policy,
            location=GLOBAL_REGION,
            timestamp=max(p.timestamp for p in parts),
            resources=list(chain.from_iterable(p.resources for p in parts)),
        ))
    if changed:
        return new
    return None


def patch_azure_google_collection(collection, cloud: Cloud) -> ShardsCollection | None:
    mapping = {}
    # {'ecc-azure-1': {'us-east-1': part, 'us-west-2': part}}
    for part in collection.iter_all_parts():
        mapping.setdefault(part.policy, {})[part.location] = part
    if all([tuple(v) == (GLOBAL_REGION, ) for v in mapping.values()]):
        # No patch needed, all parts have only global region
        return None

    new = ShardsCollectionFactory.from_cloud(cloud)
    new.io = collection.io
    for policy in mapping:
        if len(mapping[policy]) == 1 and GLOBAL_REGION in mapping[policy]:
            new.put_part(mapping[policy][GLOBAL_REGION])
            continue
        # need to merge multiple parts into one with global region
        parts = tuple(mapping[policy].values())
        new.put_part(ShardPart(
            policy=policy,
            location=GLOBAL_REGION,
            timestamp=max([p.timestamp for p in parts]),
            resources=list(chain.from_iterable([p.resources for p in parts])),
        ))
    return new


def patch_shards():
    for root in iter_shard_prefixes():
        _LOG.info(f'Going to patch {root}')
        cloud = get_prefix_cloud(root)
        if cloud is Cloud.KUBERNETES or cloud is Cloud.K8S:
            _LOG.info('Skipping kubernetes cloud')
            continue
        collection = ShardsCollectionFactory.from_cloud(cloud)
        collection.io = ShardsS3IO(
            bucket=Env.REPORTS_BUCKET_NAME.as_str(),
            key=root,
            client=SP.s3
        )
        _LOG.info('Fetching collection data')
        collection.fetch_all()
        collection.fetch_meta()

        match cloud:
            case Cloud.AWS:
                new = patch_aws_collection(collection)
            case Cloud.AZURE | Cloud.GOOGLE | Cloud.GCP:
                new = patch_azure_google_collection(collection, cloud)
            case _:
                raise RuntimeError(f'Unsupported cloud: {cloud}')
        if new is None:
            _LOG.info('No patch is needed')
            continue
        _LOG.info('Going to save patches collection')
        new.write_all()


def main() -> int:
    try:
        patch_shards()
        return 0
    except Exception:
        _LOG.exception('Unexpected exception')
        return 1


if __name__ == '__main__':
    sys.exit(main())

