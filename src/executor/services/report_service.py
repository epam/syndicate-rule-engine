from pathlib import Path
from typing import Generator, TypedDict, Iterable, cast

import msgspec
from c7n.provider import get_resource_class
from c7n.resources import load_resources
from modular_sdk.models.tenant import Tenant

from helpers import json_path_get
from helpers.constants import (Cloud, GLOBAL_REGION, PolicyErrorType)
from helpers.log_helper import get_logger
from services.sharding import LazyPickleShardPart

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


class ReportFieldsLoader:
    """
    What you need to understand is that each resource type has its unique
    json representation of its instances. We cannot know what field inside
    that json is considered to be a logical ID, name (or arn in case AWS) of
    that resource. Fortunately, this information is present inside Cloud
    Custodian and we get get it.
    For k8s name is always inside "metadata.name", id - "metadata.uid",
    namespace - "metadata.namespace".
    For azure they are also always the same due to consistent api.
    For AWS, GOOGLE we must retrieve these values for each resource type
    """
    class Fields(TypedDict, total=False):
        id: str
        name: str
        arn: str | None
        namespace: str | None

    _mapping: dict[str, Fields] = {}

    @classmethod
    def _load_for_resource_type(cls, rt: str) -> Fields | None:
        """
        Updates mapping for the given resource type.
        It must be loaded beforehand
        :param rt:
        :return:
        """
        _LOG.debug(f'Loading meta for resource type: {rt}')
        try:
            factory = get_resource_class(rt)
        except (KeyError, AssertionError):
            _LOG.warning(f'Could not load resource type: {rt}')
            return
        resource_type = getattr(factory, 'resource_type')
        _id = getattr(resource_type, 'id', None)
        _name = getattr(resource_type, 'name', None)
        if not _id:
            _LOG.warning('Resource type has no id')
        if not _name:
            _LOG.warning('Resource type has no name')

        return {
            'id': cast(str, _id),
            'name': cast(str, _name),
            'arn': getattr(resource_type, 'arn', None),
            'namespace': 'metadata.namespace'  # for k8s always this way
        }

    @classmethod
    def get(cls, rt: str) -> Fields:
        if rt not in cls._mapping:
            fields = cls._load_for_resource_type(rt)
            if not isinstance(fields, dict):
                return {}
            cls._mapping[rt] = fields
        return cls._mapping[rt]

    @classmethod
    def load(cls, resource_types: tuple = ('*',)):
        """
        Loads all the modules. In theory, we must use this class after
        performing scan. Till that moment all the necessary resources must be
        already loaded
        :param resource_types:
        :return:
        """
        load_resources(set(resource_types))


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
    def description(self) -> str | None:
        return self.policy.get('description')

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


class RuleRawOutput:
    __slots__ = ('metadata', 'resources')

    def __init__(self, metadata: RuleRawMetadata,
                 resources: list[dict] | None):
        self.metadata = metadata
        self.resources = resources  # if None, the rule wasn't executed at all
        # custodian_run: str

    @property
    def was_executed(self) -> bool:
        """
        Tells whether this policy was executed at all. Policy was not
        executed in case some internal exception or ClientError or
        something else. Either way resources.json is not created for such
        policies, PolicyException metric is present
        :return:
        """
        return self.resources is not None


class JobResult:
    class FormattedItem(TypedDict):  # our detailed report item
        policy: dict
        resources: list[dict]

    RegionRuleOutput = tuple[str, str, RuleRawOutput]

    def __init__(self, work_dir: Path, cloud: Cloud):
        self._work_dir = work_dir
        self._cloud = cloud

        self._res_decoded = msgspec.json.Decoder(type=list[dict])

    @staticmethod
    def cloud_to_resource_type_prefix() -> dict[Cloud, str]:
        return {
            Cloud.AWS: 'aws',
            Cloud.AZURE: 'azure',
            Cloud.GOOGLE: 'gcp',
            Cloud.KUBERNETES: 'k8s'
        }

    def adjust_resource_type(self, rt: str) -> str:
        rt = rt.split('.', maxsplit=1)[-1]
        return '.'.join((
            self.cloud_to_resource_type_prefix()[self._cloud], rt
        ))

    def _load_raw_rule_output(self, root: Path) -> RuleRawOutput | None:
        """
        Folder with rule output contains three files:
        'custodian-run.log' -> logs in text
        'metadata.json' -> dict
        'resources.json' -> list or resources
        In case resources.json files does not exist this execution did
        not happen due to some exception
        :param root:
        :return:
        """
        # logs = root / 'custodian-run.log'
        metadata = root / 'metadata.json'
        resources = root / 'resources.json'

        with open(metadata, 'r') as file:
            metadata_data = msgspec.json.decode(file.read(), type=dict)
        resources_data = None
        if resources.exists():
            with open(resources, 'r') as file:
                resources_data = self._res_decoded.decode(file.read())
        return RuleRawOutput(
            metadata=RuleRawMetadata(metadata_data),
            resources=resources_data
        )

    def _extend_resources(self, output: RuleRawOutput):
        """
        Adds some report fields (id, name, arn, namespace) to each resource
        """
        assert output.was_executed, 'You must provide this method only with policies that was executed without exceptions'  # noqa
        rt = self.adjust_resource_type(output.metadata.resource_type)
        ReportFieldsLoader.load((rt,))  # should be loaded before
        fields = ReportFieldsLoader.get(rt)
        for res in output.resources:
            for field, path in fields.items():
                if not path:
                    continue
                val = json_path_get(res, path)
                if not val:
                    continue
                res[field] = val

    def iter_raw(self) -> Generator[RegionRuleOutput, None, None]:
        dirs = filter(Path.is_dir, self._work_dir.iterdir())
        for region in dirs:
            for rule in filter(Path.is_dir, region.iterdir()):
                loaded = self._load_raw_rule_output(rule)
                if not loaded:
                    continue
                yield region.name, rule.name, loaded

    @staticmethod
    def resolve_azure_locations(it: Iterable[RegionRuleOutput]
                                ) -> Generator[RegionRuleOutput, None, None]:
        """
        The thing is: Custodian Custom Core cannot scan Azure
        region-dependently. A rule covers the whole subscription
        (or whatever, I don't know) and then each found resource has
        'location' field with its real location.
        In order to adhere to AWS logic, when a user wants to receive
        reports only for regions he activated, we need to filter out only
        appropriate resources.
        Also note that Custom Core has such a thing as `AzureCloud`. From
        my point of view it's like a mock for every region (because,
        I believe, in the beginning Core was designed for AWS and therefore
        there are regions). With the current scanner implementation
        (3.3.1) incoming `detailed_report` will always have one key:
        `AzureCloud` with a list of all the scanned rules. We must remap it.
        All the resources that does not contain
        'location' will be congested to 'multiregion' region.
        :return:
        """
        for _, rule, item in it:
            if not item.was_executed:
                yield GLOBAL_REGION, rule, item
                continue
            # was executed
            if not item.resources:  # we cannot know
                yield GLOBAL_REGION, rule, item
                continue
            # resources exist
            _loc_res = {}
            for res in item.resources:
                loc = res.get('location') or GLOBAL_REGION
                _loc_res.setdefault(loc, []).append(res)
            for location, resources in _loc_res.items():
                yield location, rule, RuleRawOutput(
                    metadata=item.metadata,
                    resources=resources
                )

    def build_default_iterator(self) -> Iterable[RegionRuleOutput]:
        it = self.iter_raw()
        if self._cloud == Cloud.AZURE:
            it = self.resolve_azure_locations(it)
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
        for region, rule, output in self.iter_raw():
            metadata = output.metadata
            item = {
                'policy': rule,
                'region': region,
                'tenant_name': tenant.name,
                'customer_name': tenant.customer_name,
                'start_time': metadata.start_time,
                'end_time': metadata.end_time,
                'api_calls': metadata.api_calls,
            }
            if output.was_executed:
                item['scanned_resources'] = metadata.all_resources_count
                item['failed_resources'] = metadata.failed_resources_count
            elif _failed := failed.get((region, rule)):
                item['error_type'] = _failed[0]
                item['reason'] = _failed[1]
                item['traceback'] = _failed[2]
            else:
                _LOG.warning(f'Rule {rule}:{region} has was not executed but '
                             f'failed map does not contain its info')
                item['error_type'] = PolicyErrorType.INTERNAL
            res.append(item)
        return res

    def iter_shard_parts(self) -> Generator[LazyPickleShardPart, None, None]:
        for region, rule, output in self.build_default_iterator():
            if not output.was_executed:
                continue
            self._extend_resources(output)  # todo use ijson
            yield LazyPickleShardPart.from_resources(
                resources=output.resources,
                policy=rule,
                location=region
            )

    def rules_meta(self) -> dict[str, dict]:
        """
        Collect some meta for each policy, currently it's everything that
        policy has except filters
        :return:
        """
        result = {}
        for _, rule, output in self.iter_raw():
            meta = {
                k: v for k, v in output.metadata.policy.items()
                if k not in ('filters', 'name')
            }
            if 'resource' in meta:
                meta['resource'] = self.adjust_resource_type(meta['resource'])
            result.setdefault(rule, {}).update(meta)
        return result
