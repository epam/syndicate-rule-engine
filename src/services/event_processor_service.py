import json
from abc import ABC
from functools import cached_property
from typing import Optional, Dict, Iterator, List, Generator, Tuple, Set, \
    Iterable

from helpers import deep_get, deep_set
from helpers.constants import AWS_VENDOR, MAESTRO_VENDOR, \
    S3SettingKey, GLOBAL_REGION, Cloud
from helpers.log_helper import get_logger
from services.clients.sts import StsClient
from services.environment_service import EnvironmentService
from services.s3_settings_service import S3SettingsService, \
    S3SettingsServiceLocalWrapper
_LOG = get_logger(__name__)

# CloudTrail event
CT_USER_IDENTITY = 'userIdentity'
CT_EVENT_SOURCE = 'eventSource'
CT_EVENT_NAME = 'eventName'
CT_ACCOUNT_ID = 'accountId'
CT_RECORDS = 'Records'
CT_RESOURCES = 'resources'
CT_REGION = 'awsRegion'
CT_EVENT_TIME = 'eventTime'
CT_EVENT_VERSION = 'eventVersion'

# EventBridge event
EB_ACCOUNT_ID = 'account'
EB_EVENT_SOURCE = 'source'
EB_REGION = 'region'
EB_DETAIL_TYPE = 'detail-type'
EB_DETAIL = 'detail'
EB_CLOUDTRAIL_API_CALL_DETAIL_TYPE = 'AWS API Call via CloudTrail'

# Maestro event
MA_EVENT_ACTION = 'eventAction'
MA_GROUP = 'group'
MA_SUB_GROUP = 'subGroup'
MA_EVENT_METADATA = 'eventMetadata'
MA_CLOUD = 'cloud'
MA_TENANT_NAME = 'tenantName'
MA_REGION_NAME = 'regionName'
MA_REQUEST = 'request'

# --AWS--
RegionRuleMap = Dict[str, Set[str]]
AccountRegionRuleMap = Dict[str, RegionRuleMap]  # Account means Tenant.project
# --AWS--

# --MAESTRO--
CloudTenantRegionRulesMap = Dict[str, Dict[str, Dict[str, Set[str]]]]
# --MAESTRO--

DEV = '323549576358'


class EventProcessorService:
    def __init__(self, s3_settings_service: S3SettingsService,
                 environment_service: EnvironmentService,
                 sts_client: StsClient):
        self.s3_settings_service = s3_settings_service
        self.environment_service = environment_service
        self.sts_client = sts_client
        self.mappings_collector = {}  # TODO: fix
        self.EVENT_TYPE_PROCESSOR_MAPPING = {
            AWS_VENDOR: EventBridgeEventProcessor,
            MAESTRO_VENDOR: MaestroEventProcessor
        }

    def get_processor(self, vendor: str) -> 'BaseEventProcessor':
        # vendor already validated
        processor_type = self.EVENT_TYPE_PROCESSOR_MAPPING[vendor]
        processor = processor_type(
            self.s3_settings_service,
            self.environment_service,
            self.sts_client,
            self.mappings_collector
        )
        return processor


class BaseEventProcessor(ABC):
    skip_where: Dict[Tuple[str, ...], Set] = {}
    keep_where: Dict[Tuple[str, ...], Set] = {}
    params_to_keep: Tuple[Tuple[str, ...], ...] = ()

    def __init__(self, s3_settings_service: S3SettingsService,
                 environment_service: EnvironmentService,
                 sts_client: StsClient,
                 mappings_collector: dict):
        self.s3_settings_service = S3SettingsServiceLocalWrapper(
            s3_settings_service)
        self.environment_service = environment_service
        self.sts_client = sts_client
        self.mappings_collector = mappings_collector

        self._events: List[Dict] = []

    @property
    def ct_mapping(self) -> dict:
        return self.mappings_collector.aws_events

    @cached_property
    def eb_mapping(self) -> dict:
        """
        This mapping exists but it is not used
        :return:
        """
        return self.s3_settings_service.get(
            S3SettingKey.EVENT_BRIDGE_EVENT_SOURCE_TO_RULES_MAPPING
        )

    def clear(self):
        self._events = []

    @property
    def events(self) -> List[Dict]:
        return self._events

    @events.setter
    def events(self, value: List[Dict]):
        assert isinstance(value, list)
        self.clear()
        self._events = value

    def number_of_received(self) -> int:
        return len(self._events)

    @property
    def i_events(self) -> Iterator[Dict]:
        return iter(self._events)

    @staticmethod
    def skip_record(record: dict, where: Dict[Tuple[str, ...], Set]) -> bool:
        for keys, values in where.items():
            if deep_get(record, keys) in values:
                return True
        return False

    @staticmethod
    def keep_record(record: dict, where: Dict[Tuple[str, ...], Set]) -> bool:
        if not where or not isinstance(where, dict):
            return True
        for keys, values in where.items():
            if deep_get(record, keys) not in values:
                return False
        return True

    @staticmethod
    def sieved_record(record: dict,
                      to_keep: Optional[Tuple[Tuple[str, ...], ...]],
                      allow_empty: bool = False) -> dict:
        to_keep = to_keep or tuple()
        if not to_keep and not allow_empty:
            return record
        result = {}
        for path in to_keep:
            item = deep_get(record, path)
            if item:
                deep_set(result, path, item)
        return result

    @staticmethod
    def digest(dct: dict):
        """
        >>> BaseEventProcessor.digest({3:4,1:2}) == BaseEventProcessor.digest({1:2, 3:4})
        True
        """
        return hash(json.dumps(dct, sort_keys=True))

    @classmethod
    def without_duplicates(cls, it: Iterable[Dict]) -> Generator[
        Dict, None, int]:
        emitted = set()
        for i in it:
            d = cls.digest(i)
            if d in emitted:
                continue
            emitted.add(d)
            yield i
        return len(emitted)

    def prepared_events(self) -> Generator[dict, None, int]:
        n = 0
        for record in self.i_events:
            if self.skip_record(record, self.skip_where) or \
                    not self.keep_record(record, self.keep_where):
                _LOG.warning(f'Filtering out the record: {record}')
                continue
            yield self.sieved_record(record, self.params_to_keep)
            n += 1
        return n


class CloudTrail:
    """
    Collection of CLoudTrail event bound functions
    """

    @staticmethod
    def get_rules(record: dict, mapping: dict) -> set:
        source, name = (
            record.get(CT_EVENT_SOURCE), record.get(CT_EVENT_NAME)
        )
        rules = mapping.get(source, {}).get(name, []) or mapping.get(name, [])
        if not rules:
            _LOG.warning(f'No rules found within CloudTrail {source}:{name}')
        return set(rules)

    @staticmethod
    def get_region(record: dict) -> Optional[str]:
        region = record.get(CT_REGION)
        if not region:
            _LOG.warning(
                f'No regions, found within a CloudTrail record - {record}.'
            )
        return region

    @classmethod
    def get_account_id(cls, record: dict) -> Optional[str]:
        _id = cls._account_id_from_user_identity(record)
        if not _id:
            _LOG.info(f'Account id not found in `userIdentity` in record: '
                      f'{record}.')
        return _id

    @staticmethod
    def _account_id_from_user_identity(record: dict) -> Optional[str]:
        return record.get(CT_USER_IDENTITY, {}).get(CT_ACCOUNT_ID)

    @staticmethod
    def _account_ids_from_resources(record: dict) -> set:
        """
        As far as I understood from CloudTrail docs:
        https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-event-reference-record-contents.html,
        `resources` can contain items with `accountId` inside. But such
        items represent resources that belongs to another AWS account.
        So, currently the method is not used.
        """
        ids = {res.get(CT_ACCOUNT_ID) for res in record.get(CT_RESOURCES, [])}
        if None in ids:
            ids.remove(None)
        return ids

    @staticmethod
    def is_cloudtrail_api_call(record: dict) -> bool:
        return record.get(EB_DETAIL_TYPE) == EB_CLOUDTRAIL_API_CALL_DETAIL_TYPE


class MaestroEventProcessor(BaseEventProcessor):
    skip_where = {}
    keep_where = {
        (MA_EVENT_METADATA, MA_REQUEST, MA_CLOUD): {Cloud.AZURE.value,
                                                    Cloud.GOOGLE.value},
        # (MA_EVENT_METADATA, MA_CLOUD,): {AZURE_CLOUD_ATTR, },
        (MA_GROUP,): {'MANAGEMENT'},
        (MA_SUB_GROUP,): {'INSTANCE'}
    }
    params_to_keep = (
        (MA_EVENT_ACTION,),
        (MA_GROUP,),
        (MA_SUB_GROUP,),
        (MA_EVENT_METADATA, MA_REQUEST, MA_CLOUD),
        (MA_EVENT_METADATA, MA_CLOUD),
        (MA_TENANT_NAME,)
    )

    @property
    def azure_mapping(self) -> dict:
        return self.mappings_collector.azure_events

    @property
    def maestro_azure_mapping(self) -> dict:
        return self.s3_settings_service.get(
            S3SettingKey.MAESTRO_SUBGROUP_ACTION_TO_AZURE_EVENTS_MAPPING
        )

    @property
    def google_mapping(self) -> dict:
        return self.mappings_collector.google_events

    @property
    def maestro_google_mapping(self) -> dict:
        return self.s3_settings_service.get(
            S3SettingKey.MAESTRO_SUBGROUP_ACTION_TO_GOOGLE_EVENTS_MAPPING
        )

    def cloud_tenant_region_rules(
            self, it: Iterable[Dict]
    ) -> Generator[Tuple[str, str, str, Set[str]], None, None]:
        """
        Maestro events does not contain account id or subscription id. They
        contain tenantName (maestro tenant) so we must rely on it.
        Yields tuples (cloud, tenantName, region, Set[rules])
        """
        for event in it:
            cloud = deep_get(event, (MA_EVENT_METADATA, MA_REQUEST, MA_CLOUD))
            # cloud = deep_get(event, (MA_EVENT_METADATA, MA_CLOUD))
            tenant = deep_get(event, (MA_TENANT_NAME,))
            # TODO currently only AZURE events are expected. In case we want
            #  to process AWS maestro audit events we should remap the
            #  maestro region to native name. For AZURE we can just ignore it
            if cloud == Cloud.AZURE.value:
                region = GLOBAL_REGION
            elif cloud == Cloud.GOOGLE.value:
                region = GLOBAL_REGION
            else:
                region = deep_get(event, (MA_REGION_NAME,))
            if not all((cloud, tenant, region)):
                continue
            rules = self.get_rules(event, cloud.upper())
            yield cloud.upper(), tenant, region, rules

    def cloud_tenant_region_rules_map(self, it: Iterable[Dict]
                                      ) -> CloudTenantRegionRulesMap:
        ref = {}
        for cloud, tenant, region, rules in self.cloud_tenant_region_rules(it):
            ref.setdefault(cloud, {}).setdefault(tenant, {}).setdefault(region,
                                                                        set()).update(
                rules)
        return ref

    def get_rules(self, event: dict, cloud: str) -> Set[str]:
        cloud_method = {
            Cloud.AZURE.value: self.get_rules_azure,
            Cloud.GOOGLE.value: self.get_rules_google
        }
        _get_rules = cloud_method.get(cloud) or (lambda e: set())
        return _get_rules(event)

    def get_rules_azure(self, event: dict) -> Set[str]:
        """
        Expected that event's cloud is AZURE. That means that AZURE's
        mappings will be used to retrieve rules
        """
        _maestro_map = self.maestro_azure_mapping
        _azure_map = self.azure_mapping
        sub_group = event.get(MA_SUB_GROUP)
        action = event.get(MA_EVENT_ACTION)
        azure_events: List[List[str]] = _maestro_map.get(
            sub_group, {}).get(action, [])
        rules = set()
        for e_source, e_name in azure_events:
            rules.update(_azure_map.get(e_source, {}).get(e_name, []))
        return rules

    def get_rules_google(self, event: dict) -> Set[str]:
        _maestro_map = self.maestro_google_mapping
        _google_map = self.google_mapping
        sub_group = event.get(MA_SUB_GROUP)
        action = event.get(MA_EVENT_ACTION)
        google_events: List[List[str]] = _maestro_map.get(
            sub_group, {}).get(action, [])
        rules = set()
        for e_source, e_name in google_events:
            rules.update(_google_map.get(e_source, {}).get(e_name, []))
        return rules


class EventBridgeEventProcessor(BaseEventProcessor):
    params_to_keep = (
        (EB_DETAIL_TYPE,),
        # (EB_EVENT_SOURCE,),
        # (EB_ACCOUNT_ID,),
        # (EB_REGION,),
        (EB_DETAIL, CT_EVENT_NAME),
        (EB_DETAIL, CT_EVENT_SOURCE),
        (EB_DETAIL, CT_USER_IDENTITY, CT_ACCOUNT_ID),
        (EB_DETAIL, CT_REGION)
    )

    def __init__(self, s3_settings_service: S3SettingsService,
                 environment_service: EnvironmentService,
                 sts_client: StsClient,
                 mappings_collector: dict):
        super().__init__(
            s3_settings_service,
            environment_service,
            sts_client,
            mappings_collector
        )
        # self.keep_where = {
        #     (EB_EVENT_SOURCE, ): set(self.eb_mapping.keys())
        # }
        self.keep_where = {
            (EB_DETAIL_TYPE,): {EB_CLOUDTRAIL_API_CALL_DETAIL_TYPE},
            (EB_DETAIL, CT_EVENT_SOURCE): set(self.ct_mapping.keys()),
            (EB_DETAIL, CT_EVENT_NAME): {
                name for values in self.ct_mapping.values() for name in values
            }
        }
        account_id = self.sts_client.get_account_id()
        if account_id != DEV:
            self.skip_where = {
                (EB_ACCOUNT_ID,): {account_id, },
                (EB_DETAIL, CT_USER_IDENTITY, CT_ACCOUNT_ID): {account_id, }
            }

    def account_region_rule_map(self,
                                it: Iterable[Dict]) -> AccountRegionRuleMap:
        ref = {}
        for event in it:
            account_id = self.get_account_id(record=event)
            region = self.get_region(record=event)
            rules = self.get_rules(record=event)
            if not all((account_id, region, rules)):
                continue
            _account_scope = ref.setdefault(account_id, {})
            _account_scope.setdefault(region, set()).update(rules)
        return ref

    @staticmethod
    def get_region(record: dict) -> Optional[str]:
        if CloudTrail.is_cloudtrail_api_call(record):
            return CloudTrail.get_region(record.get(EB_DETAIL) or {})
        return record.get(EB_REGION)

    @staticmethod
    def get_account_id(record: dict) -> Optional[str]:
        if CloudTrail.is_cloudtrail_api_call(record):
            return CloudTrail.get_account_id(record.get(EB_DETAIL) or {})
        return record.get(EB_ACCOUNT_ID)

    def get_rules(self, record: dict) -> set:
        """
        Here we should consider EventBridge event with detail-type:
        "AWS API Call via CloudTrail". Such an event contains CloudTrail's
        EventName & EventSource in its "details" attribute, and we
        definitely shall use them to get the rules.
        """
        if CloudTrail.is_cloudtrail_api_call(record):
            return CloudTrail.get_rules(record.get(EB_DETAIL) or {},
                                        self.ct_mapping)
        # TODO EB source to list of rules is a temp solution I was able to
        #  make, but it would be better to use EB detail-type here
        source = record.get(EB_EVENT_SOURCE)
        return set(self.eb_mapping.get(source) or [])
