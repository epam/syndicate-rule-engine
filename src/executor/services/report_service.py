from pathlib import Path
from typing import Generator, Iterable, TypedDict

import msgspec
from modular_sdk.models.tenant import Tenant

from executor.helpers.constants import AWS_DEFAULT_REGION
from helpers.constants import GLOBAL_REGION, Cloud, PolicyErrorType
from helpers.log_helper import get_logger
from services.sharding import RuleMeta, ShardPart

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

    @staticmethod
    def resolve_azure_locations(
        it: Iterable[RegionRuleOutput],
    ) -> Generator[RegionRuleOutput, None, None]:
        """
        The thing is: Custodian Custom Core cannot scan Azure
        region-dependently. A rule covers the whole subscription and then
        each found resource has 'location' field with its real location.
        In order to adhere to AWS logic, when a user wants to receive
        reports only for regions he activated, we need to filter out only
        appropriate resources.
        """
        for _, rule, metadata, resources in it:
            if resources is None or not resources:
                yield GLOBAL_REGION, rule, metadata, resources
                continue
            # resources exist
            _loc_res = {}
            for res in resources:
                loc = res.get('location') or GLOBAL_REGION
                _loc_res.setdefault(loc, []).append(res)
            for loc, res in _loc_res.items():
                yield loc, rule, metadata, res

    @staticmethod
    def resolve_aws_s3_location_constraint(
        it: Iterable[RegionRuleOutput],
    ) -> Generator[RegionRuleOutput, None, None]:
        for region, rule, metadata, resources in it:
            if resources is None or not resources:
                yield region, rule, metadata, resources
                continue
            if metadata.resource_type not in ('s3', 'aws.s3'):
                yield region, rule, metadata, resources
                continue
            _region_buckets = {}
            for bucket in resources:
                # LocationConstraint is None if region is us-east-1
                r = (
                    bucket.get('Location', {}).get('LocationConstraint')
                    or AWS_DEFAULT_REGION
                )
                _region_buckets.setdefault(r, []).append(bucket)
            for r, buckets in _region_buckets.items():
                yield r, rule, metadata, buckets

    @staticmethod
    def resolve_google_location_id(
        it: Iterable[RegionRuleOutput],
    ) -> Generator[RegionRuleOutput, None, None]:
        for region, rule, metadata, resources in it:
            if resources is None or not resources:
                yield region, rule, metadata, resources
                continue
            _loc_res = {}
            for res in resources:
                loc = res.get('locationId') or GLOBAL_REGION
                _loc_res.setdefault(loc, []).append(res)
            for loc, res in _loc_res.items():
                yield loc, rule, metadata, res

    def build_default_iterator(self) -> Iterable[RegionRuleOutput]:
        it = self.iter_raw(with_resources=True)
        if self._cloud == Cloud.AZURE:
            it = self.resolve_azure_locations(it)
        elif self._cloud == Cloud.AWS:
            it = self.resolve_aws_s3_location_constraint(it)
        # can be handled here or later, but will require a path of here
        # elif self._cloud == Cloud.GOOGLE:
        #     it = self.resolve_google_location_id(it)
        return it

    def statistics(self, tenant: Tenant, failed: dict) -> list[dict]:
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
        self, failed: dict
    ) -> Generator[ShardPart, None, None]:
        for region, rule, metadata, resources in self.build_default_iterator():
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
