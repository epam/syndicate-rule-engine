from __future__ import annotations

import json
from abc import ABC
from typing import TYPE_CHECKING
from functools import cached_property
from typing import Optional, Dict, Iterator, List, Generator, Tuple, Set, Iterable

from helpers import deep_get, deep_set
from helpers.constants import (
    AWS_VENDOR,
    MAESTRO_VENDOR,
    GLOBAL_REGION,
    Cloud,
    Env,
)
from helpers.log_helper import get_logger
from helpers.mixins import EventDrivenLicenseMixin
from services.metadata import DEFAULT_VERSION


if TYPE_CHECKING:
    from helpers import Version
    from services.clients.sts import StsClient
    from services.environment_service import EnvironmentService
    from services.license_service import LicenseService
    from services.event_driven import S3EventMappingProvider
    from modular_sdk.services.tenant_service import TenantService


_LOG = get_logger(__name__)

# CloudTrail event
CT_USER_IDENTITY = "userIdentity"
CT_EVENT_SOURCE = "eventSource"
CT_EVENT_NAME = "eventName"
CT_ACCOUNT_ID = "accountId"
CT_RECORDS = "Records"
CT_RESOURCES = "resources"
CT_REGION = "awsRegion"
CT_EVENT_TIME = "eventTime"
CT_EVENT_VERSION = "eventVersion"

# EventBridge event
EB_ACCOUNT_ID = "account"
EB_EVENT_SOURCE = "source"
EB_REGION = "region"
EB_DETAIL_TYPE = "detail-type"
EB_DETAIL = "detail"
EB_CLOUDTRAIL_API_CALL_DETAIL_TYPE = "AWS API Call via CloudTrail"

# Maestro event
MA_EVENT_ACTION = "eventAction"
MA_GROUP = "group"
MA_SUB_GROUP = "subGroup"
MA_EVENT_METADATA = "eventMetadata"
MA_CLOUD = "cloud"
MA_TENANT_NAME = "tenantName"
MA_REGION_NAME = "regionName"
MA_REQUEST = "request"

# --AWS--
RegionRuleMap = Dict[str, Set[str]]
AccountRegionRuleMap = Dict[str, RegionRuleMap]  # Account means Tenant.project
# --AWS--

# --MAESTRO--
CloudTenantRegionRulesMap = Dict[str, Dict[str, Dict[str, Set[str]]]]
# --MAESTRO--


class EventProcessorService:

    def __init__(
        self,
        environment_service: EnvironmentService,
        sts_client: StsClient,
        license_service: LicenseService,
        event_mapping_provider: S3EventMappingProvider,
        tenant_service: TenantService,
    ) -> None:
        self.environment_service = environment_service
        self.sts_client = sts_client
        self.license_service = license_service
        self.event_mapping_provider = event_mapping_provider
        self.tenant_service = tenant_service
        self.EVENT_TYPE_PROCESSOR_MAPPING = {
            AWS_VENDOR: EventBridgeEventProcessor,
            MAESTRO_VENDOR: MaestroEventProcessor,
        }

    def get_processor(self, vendor: str) -> BaseEventProcessor:
        # vendor already validated
        processor_type = self.EVENT_TYPE_PROCESSOR_MAPPING[vendor]
        processor = processor_type(
            self.environment_service,
            self.sts_client,
            self.license_service,
            self.event_mapping_provider,
            self.tenant_service,
        )
        return processor


class BaseEventProcessor(ABC):
    """
    Base class for all event processors.
    """

    skip_where: Dict[Tuple[str, ...], Set] = {}
    keep_where: Dict[Tuple[str, ...], Set] = {}
    params_to_keep: Tuple[Tuple[str, ...], ...] = ()

    def __init__(
        self,
        environment_service: EnvironmentService,
        sts_client: StsClient,
        event_mapping_provider: S3EventMappingProvider,
        tenant_service: TenantService,
        license_service: LicenseService,
    ) -> None:
        self.environment_service = environment_service
        self.sts_client = sts_client
        self._event_mapping_provider = event_mapping_provider
        self._tenant_service = tenant_service
        self._license_service = license_service

        self._events: List[Dict] = []

    def ct_mapping(self, license_key: str, version: Version) -> dict:
        data = self._event_mapping_provider.get_from_s3(
            version=version,
            license_key=license_key,
            cloud=Cloud.AWS,
        )
        if data is None:
            _LOG.warning(
                f"No event mapping found for AWS in S3 "
                f"for license key: {license_key} and version: {version.to_str()} "
                f"May be metadata is not synced for this license key and version"
            )
            return {}
        return data

    @cached_property
    def eb_mapping(self) -> dict:
        """
        This mapping exists but it is not used
        :return:
        """
        from helpers.mappings.event_bridge_event_source_to_rules_mapping import MAPPING

        return MAPPING

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
    def sieved_record(
        record: dict,
        to_keep: Optional[Tuple[Tuple[str, ...], ...]],
        allow_empty: bool = False,
    ) -> dict:
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
    def without_duplicates(cls, it: Iterable[Dict]) -> Generator[Dict, None, int]:
        emitted = set()
        for i in it:
            d = cls.digest(i)
            if d in emitted:
                _LOG.warning(f"Skipping duplicate event with digest {d}: {i}")
                continue
            emitted.add(d)
            yield i
        return len(emitted)

    def prepared_events(self) -> Generator[dict, None, int]:
        n = 0
        for record in self.i_events:
            if self.skip_record(record, self.skip_where) or not self.keep_record(
                record, self.keep_where
            ):
                _LOG.warning(f"Filtering out the record: {record}")
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
        source, name = (record.get(CT_EVENT_SOURCE), record.get(CT_EVENT_NAME))
        rules = mapping.get(source, {}).get(name, []) or mapping.get(name, [])
        if not rules:
            _LOG.warning(f"No rules found within CloudTrail {source}:{name}")
        return set(rules)

    @staticmethod
    def get_region(record: dict) -> Optional[str]:
        region = record.get(CT_REGION)
        if not region:
            _LOG.warning(f"No regions, found within a CloudTrail record - {record}.")
        return region

    @classmethod
    def get_account_id(cls, record: dict) -> Optional[str]:
        _id = cls._account_id_from_user_identity(record)
        if not _id:
            _LOG.info(
                f"Account id not found in `userIdentity` in record: " f"{record}."
            )
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


class MaestroEventProcessor(BaseEventProcessor, EventDrivenLicenseMixin):
    skip_where = {}
    keep_where = {
        (MA_EVENT_METADATA, MA_REQUEST, MA_CLOUD): {
            Cloud.AZURE.value,
            Cloud.GOOGLE.value,
        },
        # (MA_EVENT_METADATA, MA_CLOUD,): {AZURE_CLOUD_ATTR, },
        (MA_GROUP,): {"MANAGEMENT"},
        (MA_SUB_GROUP,): {"INSTANCE"},
    }
    params_to_keep = (
        (MA_EVENT_ACTION,),
        (MA_GROUP,),
        (MA_SUB_GROUP,),
        (MA_EVENT_METADATA, MA_REQUEST, MA_CLOUD),
        (MA_EVENT_METADATA, MA_CLOUD),
        (MA_TENANT_NAME,),
    )

    def __init__(
        self,
        environment_service: EnvironmentService,
        sts_client: StsClient,
        license_service: LicenseService,
        event_mapping_provider: S3EventMappingProvider,
        tenant_service: TenantService,
    ) -> None:
        super().__init__(
            environment_service,
            sts_client,
            event_mapping_provider,
            tenant_service,
            license_service,
        )

    def azure_mapping(self, version: Version, license_key: str) -> dict:
        data = self._event_mapping_provider.get_from_s3(
            version=version,
            license_key=license_key,
            cloud=Cloud.AZURE,
        )
        if data is None:
            _LOG.warning(
                f"No event mapping found for Azure in S3 "
                f"for license key: {license_key} and version: {version.to_str()} "
                f"May be metadata is not synced for this license key and version"
            )
            return {}
        return data

    @property
    def maestro_azure_mapping(self) -> dict:
        from helpers.mappings.maestro_subgroup_action_to_azure_events_mapping import (
            MAPPING,
        )

        return MAPPING

    def google_mapping(self, version: Version, license_key: str) -> dict:
        data = self._event_mapping_provider.get_from_s3(
            version=version,
            license_key=license_key,
            cloud=Cloud.GOOGLE,
        )
        if data is None:
            _LOG.warning(
                f"No event mapping found for Google in S3 "
                f"for license key: {license_key} and version: {version.to_str()} "
                f"May be metadata is not synced for this license key and version"
            )
            return {}
        return data

    @property
    def maestro_google_mapping(self) -> dict:
        from helpers.mappings.maestro_subgroup_action_to_google_events_mapping import (
            MAPPING,
        )

        return MAPPING

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
            tenant_name = deep_get(event, (MA_TENANT_NAME,))
            # TODO currently only AZURE events are expected. In case we want
            #  to process AWS maestro audit events we should remap the
            #  maestro region to native name. For AZURE we can just ignore it
            if cloud == Cloud.AZURE.value:
                region = GLOBAL_REGION
            elif cloud == Cloud.GOOGLE.value:
                region = GLOBAL_REGION
            else:
                region = deep_get(event, (MA_REGION_NAME,))
            if not all((cloud, tenant_name, region)):
                _LOG.debug(
                    f"Skipping event: {event} because of missing required data: "
                    f"cloud: {cloud}, tenant: {tenant_name}, region: {region}"
                )
                continue
            tenant = self._tenant_service.get(tenant_name)
            if not tenant:
                _LOG.warning(f"No tenant found for name: {tenant_name}")
                continue
            event_driven_license = self.get_allowed_event_driven_license(tenant)
            if not event_driven_license:
                _LOG.warning(f"No event driven license found for tenant: {tenant_name}")
                continue
            rules = self.get_rules(
                event=event,
                license_key=event_driven_license.license_key,
                cloud=cloud.upper(),
                version=DEFAULT_VERSION,
            ) 
            yield cloud.upper(), tenant_name, region, rules

    def cloud_tenant_region_rules_map(
        self, it: Iterable[Dict]
    ) -> CloudTenantRegionRulesMap:
        ref = {}
        for cloud, tenant, region, rules in self.cloud_tenant_region_rules(it):
            ref.setdefault(cloud, {}).setdefault(tenant, {}).setdefault(
                region, set()
            ).update(rules)
        return ref

    def get_rules(
        self,
        event: dict,
        license_key: str,
        cloud: str,
        version: Version = DEFAULT_VERSION
    ) -> Set[str]:
        cloud_method = {
            Cloud.AZURE.value: self.get_rules_azure,
            Cloud.GOOGLE.value: self.get_rules_google,
        }
        _get_rules = cloud_method.get(cloud) or (lambda e, l, v: set())
        return _get_rules(event, license_key, version)

    def get_rules_azure(
        self,
        event: dict,
        license_key: str,
        version: Version = DEFAULT_VERSION
    ) -> Set[str]:
        """
        Expected that event's cloud is AZURE. That means that AZURE's
        mappings will be used to retrieve rules
        """
        _maestro_map = self.maestro_azure_mapping
        _azure_map = self.azure_mapping(version, license_key)
        sub_group = event.get(MA_SUB_GROUP)
        action = event.get(MA_EVENT_ACTION)
        azure_events: List[List[str]] = _maestro_map.get(sub_group, {}).get(action, [])
        rules = set()
        for e_source, e_name in azure_events:
            rules.update(_azure_map.get(e_source, {}).get(e_name, []))
        return rules

    def get_rules_google(
        self,
        event: dict,
        license_key: str,
        version: Version = DEFAULT_VERSION
    ) -> Set[str]:
        _maestro_map = self.maestro_google_mapping
        _google_map = self.google_mapping(version, license_key)
        sub_group = event.get(MA_SUB_GROUP)
        action = event.get(MA_EVENT_ACTION)
        google_events: List[List[str]] = _maestro_map.get(sub_group, {}).get(action, [])
        rules = set()
        for e_source, e_name in google_events:
            rules.update(_google_map.get(e_source, {}).get(e_name, []))
        return rules


class EventBridgeEventProcessor(BaseEventProcessor, EventDrivenLicenseMixin):
    params_to_keep = (
        (EB_DETAIL_TYPE,),
        # (EB_EVENT_SOURCE,),
        # (EB_ACCOUNT_ID,),
        # (EB_REGION,),
        (EB_DETAIL, CT_EVENT_NAME),
        (EB_DETAIL, CT_EVENT_SOURCE),
        (EB_DETAIL, CT_USER_IDENTITY, CT_ACCOUNT_ID),
        (EB_DETAIL, CT_REGION),
    )

    def __init__(
        self,
        environment_service: EnvironmentService,
        sts_client: StsClient,
        license_service: LicenseService,
        event_mapping_provider: S3EventMappingProvider,
        tenant_service: TenantService,
    ) -> None:
        super().__init__(
            environment_service, 
            sts_client, 
            event_mapping_provider,
            tenant_service,
            license_service,
        )
        # self.keep_where = {
        #     (EB_EVENT_SOURCE, ): set(self.eb_mapping.keys())
        # }
        # Note: ct_mapping requires license_key and version, so we can't use it here
        # The filtering will happen later in get_rules method
        self.keep_where = {
            (EB_DETAIL_TYPE,): {EB_CLOUDTRAIL_API_CALL_DETAIL_TYPE},
        }

        account_id = self.sts_client.get_account_id()
        if account_id != Env.DEV_ACCOUNT_ID.get():
            self.skip_where = {
                (EB_ACCOUNT_ID,): {
                    account_id,
                },
                (EB_DETAIL, CT_USER_IDENTITY, CT_ACCOUNT_ID): {
                    account_id,
                },
            }

    def account_region_rule_map(self, it: Iterable[Dict]) -> AccountRegionRuleMap:
        ref = {}
        for event in it:
            account_id = self.get_account_id(record=event)
            region = self.get_region(record=event)
            if not account_id or not region:
                continue

            tenant = next(
                self._tenant_service.i_get_by_acc(
                    acc=str(account_id),
                    active=True,
                    limit=1,
                ),
                None,
            )
            if not tenant:
                _LOG.warning(f"No tenant found for account_id: {account_id}")
                continue

            event_driven_license = self.get_allowed_event_driven_license(tenant)
            if not event_driven_license:
                _LOG.warning(f"No event driven license found for tenant: {tenant.name}")
                continue

            rules = self.get_rules(
                record=event,
                license_key=event_driven_license.license_key,
                version=DEFAULT_VERSION,
            )
            if not rules:
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

    def get_rules(
        self,
        record: dict,
        license_key: str,
        version: Version = DEFAULT_VERSION,
    ) -> Set[str]:
        """
        Here we should consider EventBridge event with detail-type:
        "AWS API Call via CloudTrail". Such an event contains CloudTrail's
        EventName & EventSource in its "details" attribute, and we
        definitely shall use them to get the rules.
        """
        ct_mapping = self.ct_mapping(
            license_key=license_key,
            version=version,
        )
        if CloudTrail.is_cloudtrail_api_call(record):
            return CloudTrail.get_rules(record.get(EB_DETAIL) or {}, ct_mapping)
        # TODO EB source to list of rules is a temp solution I was able to
        #  make, but it would be better to use EB detail-type here
        source = record.get(EB_EVENT_SOURCE)
        return set(self.eb_mapping.get(source) or [])
