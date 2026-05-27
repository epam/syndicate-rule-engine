from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Generator, TypedDict

import msgspec
from modular_sdk.models.tenant import Tenant

from helpers.constants import Cloud, PolicyErrorType
from helpers.log_helper import get_logger
from services.sharding import RuleMeta, ShardPart, ShardsCollection

if TYPE_CHECKING:
    from executor.job.scan.types import FailedPoliciesMap

_LOG = get_logger(__name__)


class StatisticsItem(TypedDict):
    policy: str
    region: str
    tenant_name: str
    customer_name: str
    start_time: float
    end_time: float
    api_calls: dict


class StatisticsItemFail(StatisticsItem):
    reason: str | None
    traceback: list[str]
    error_type: PolicyErrorType


class StatisticsItemSuccess(StatisticsItem):
    scanned_resources: int
    failed_resources: int


class RuleRawMetadata:
    """
    Simple wrapper over metadata.json that is returned by Cloud Custodian.
    Allows to extract some data
    """

    __slots__ = ('data',)

    class MetricItem(TypedDict):
        MetricName: str
        Timestamp: str
        Value: float | int
        Unit: str  # Count | Seconds

    def __init__(self, data: dict):
        self.data = data

    @property
    def policy(self) -> dict:
        return self.data['policy']

    @property
    def name(self) -> str:
        return self.policy['name']

    @property
    def resource_type(self) -> str:
        return self.policy['resource']

    @property
    def start_time(self) -> float:
        return self.data['execution']['start']

    @property
    def end_time(self) -> float:
        return self.data['execution']['end_time']

    @property
    def api_calls(self) -> dict:
        return self.data.get('api-stats') or {}

    def metric_by_name(self, name: str) -> MetricItem | None:
        it = iter(self.data.get('metrics') or [])
        it = filter(lambda m: m['MetricName'] == name, it)
        return next(it, None)

    @property
    def all_resources_count(self) -> int | None:
        metric = self.metric_by_name('AllScannedResourcesCount')
        if not metric:
            return
        return metric['Value']

    @property
    def failed_resources_count(self) -> int | None:
        metric = self.metric_by_name('ResourceCount')
        if not metric:
            return
        return metric['Value']


def statistics_from_shards_collection(
    tenant: Tenant,
    failed: FailedPoliciesMap | dict,
    collection: ShardsCollection,
) -> list[dict]:
    """
    Build per-rule statistics from an in-memory shard collection (e.g. S3
    scan partial after all regions completed). Used when the local work_dir
    does not contain every region (job resume).
    """
    failed = failed or {}
    by_key: dict[tuple[str, str], dict] = {}

    for part in collection.iter_all_parts():
        key = (part.location, part.policy)
        item: dict = {
            'policy': part.policy,
            'region': part.location,
            'tenant_name': tenant.name,
            'customer_name': tenant.customer_name,
            'start_time': part.timestamp,
            'end_time': part.timestamp,
            'api_calls': {},
        }
        if part.error is None:
            item['scanned_resources'] = len(part.resources)
            item['failed_resources'] = 0
        elif fb := failed.get(key):
            item['error_type'] = fb[0]
            item['reason'] = fb[1]
            item['traceback'] = fb[2]
        else:
            et_str, _, msg = (part.error or '').partition(':')
            try:
                item['error_type'] = PolicyErrorType(et_str)
            except ValueError:
                item['error_type'] = PolicyErrorType.INTERNAL
            item['reason'] = msg or None
            item['traceback'] = []
        by_key[key] = item

    for key, fb in failed.items():
        if key not in by_key:
            region, policy = key
            by_key[key] = {
                'policy': policy,
                'region': region,
                'tenant_name': tenant.name,
                'customer_name': tenant.customer_name,
                'start_time': 0.0,
                'end_time': 0.0,
                'api_calls': {},
                'error_type': fb[0],
                'reason': fb[1],
                'traceback': fb[2],
            }

    return list(by_key.values())


class JobResult:
    RegionRuleOutput = tuple[str, str, RuleRawMetadata, list[dict] | None]

    def __init__(self, work_dir: Path, cloud: Cloud):
        self._work_dir = work_dir
        self._cloud = cloud

        self._metadata_decoder = msgspec.json.Decoder(type=dict)
        self._res_decoded = msgspec.json.Decoder(type=list[dict])

    @staticmethod
    def cloud_to_resource_type_prefix() -> dict[Cloud, str]:
        return {
            Cloud.AWS: 'aws',
            Cloud.AZURE: 'azure',
            Cloud.GOOGLE: 'gcp',
            Cloud.KUBERNETES: 'k8s',
        }

    def adjust_resource_type(self, rt: str) -> str:
        rt = rt.split('.', maxsplit=1)[-1]
        return '.'.join(
            (self.cloud_to_resource_type_prefix()[self._cloud], rt)
        )

    @staticmethod
    def _resources_exist(root: Path) -> bool:
        return (root / 'resources.json').exists()

    def _load_resources(self, root: Path) -> list[dict] | None:
        resources = root / 'resources.json'
        if not resources.exists():
            return
        with open(resources, 'rb') as fp:
            return self._res_decoded.decode(fp.read())

    def _load_metadata(self, root: Path) -> RuleRawMetadata:
        with open(root / 'metadata.json', 'rb') as fp:
            return RuleRawMetadata(self._metadata_decoder.decode(fp.read()))

    def iter_raw(
        self, with_resources: bool = False
    ) -> Generator[RegionRuleOutput, None, None]:
        dirs = filter(Path.is_dir, self._work_dir.iterdir())
        for region in dirs:
            for rule in filter(Path.is_dir, region.iterdir()):
                metadata = self._load_metadata(rule)
                if with_resources:
                    resources = self._load_resources(rule)
                else:
                    resources = [] if self._resources_exist(rule) else None
                yield region.name, rule.name, metadata, resources

    def statistics(self, tenant: Tenant, failed: FailedPoliciesMap | dict) -> list[dict]:
        """
        :param tenant:
        :param failed:
        :return: Ready statistics dict
        :rtype: StatisticsItemFail | StatisticsItemSuccess
        """
        failed = failed or {}
        res = []
        for region, rule, metadata, resources in self.iter_raw(
            with_resources=False
        ):
            item = {
                'policy': rule,
                'region': region,
                'tenant_name': tenant.name,
                'customer_name': tenant.customer_name,
                'start_time': metadata.start_time,
                'end_time': metadata.end_time,
                'api_calls': metadata.api_calls,
            }
            if resources is not None:
                item['scanned_resources'] = metadata.all_resources_count
                item['failed_resources'] = metadata.failed_resources_count
            elif _failed := failed.get((region, rule)):
                item['error_type'] = _failed[0]
                item['reason'] = _failed[1]
                item['traceback'] = _failed[2]
            else:
                _LOG.warning(
                    f'Rule {rule}:{region} has was not executed but '
                    f'failed map does not contain its info'
                )
                item['error_type'] = PolicyErrorType.INTERNAL
            res.append(item)
        return res

    def iter_shard_parts(
        self, failed: FailedPoliciesMap | dict
    ) -> Generator[ShardPart, None, None]:
        for region, rule, metadata, resources in self.iter_raw(with_resources=True):
            if resources is None:
                # policy error occurred
                if er := failed.get((region, rule)):
                    error = er[0], er[1]
                else:
                    error = PolicyErrorType.INTERNAL, 'Unknown policy error'

                yield ShardPart(
                    policy=rule,
                    location=region,
                    timestamp=metadata.end_time,
                    error=':'.join(error),
                )
            else:
                yield ShardPart(
                    policy=rule,
                    location=region,
                    timestamp=metadata.end_time,
                    resources=resources,
                )

    def iter_shard_parts_for_region(
        self,
        region: str,
        failed: FailedPoliciesMap | dict,
    ) -> Generator[ShardPart, None, None]:
        for reg, rule, metadata, resources in self.iter_raw(with_resources=True):
            if reg != region:
                continue
            if resources is None:
                if er := failed.get((reg, rule)):
                    error = er[0], er[1]
                else:
                    error = PolicyErrorType.INTERNAL, 'Unknown policy error'

                yield ShardPart(
                    policy=rule,
                    location=reg,
                    timestamp=metadata.end_time,
                    error=':'.join(error),
                )
            else:
                yield ShardPart(
                    policy=rule,
                    location=reg,
                    timestamp=metadata.end_time,
                    resources=resources,
                )

    def rules_meta_for_region(self, region: str) -> dict[str, RuleMeta]:
        result: dict[str, RuleMeta] = {}
        for reg, rule, metadata, _ in self.iter_raw(with_resources=False):
            if reg != region:
                continue
            meta = {
                k: v
                for k, v in metadata.policy.items()
                if k not in ('filters', 'name')
            }
            if 'resource' in meta:
                meta['resource'] = self.adjust_resource_type(meta['resource'])
            result.setdefault(rule, {}).update(meta)
        return result

    def rules_meta(self) -> dict[str, RuleMeta]:
        """
        Collect some meta for each policy, currently it's everything that
        policy has except filters
        :return:
        """
        result = {}
        for _, rule, metadata, _ in self.iter_raw(with_resources=False):
            meta = {
                k: v
                for k, v in metadata.policy.items()
                if k not in ('filters', 'name')
            }
            if 'resource' in meta:
                meta['resource'] = self.adjust_resource_type(meta['resource'])
            result.setdefault(rule, {}).update(meta)
        return result
