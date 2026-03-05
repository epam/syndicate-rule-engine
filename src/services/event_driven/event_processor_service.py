from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generator,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    cast,
)

from helpers import deep_get, deep_set
from helpers.constants import (
    AWS_VENDOR,
    GLOBAL_REGION,
    MAESTRO_VENDOR,
    Cloud,
    Env,
)
from helpers.log_helper import get_logger
from helpers.mixins import EventDrivenLicenseMixin
from services.metadata import DEFAULT_VERSION

from ._constants import *

if TYPE_CHECKING:
    from modular_sdk.services.tenant_service import TenantService

    from helpers import Version
    from services.clients.sts import StsClient
    from services.environment_service import EnvironmentService
    from services.event_driven import S3EventMappingProvider
    from services.license_service import LicenseService


_LOG = get_logger(__name__)


# --AWS--
RegionRuleMap = Dict[str, Set[str]]
AccountRegionRuleMap = Dict[str, RegionRuleMap]  # Account means Tenant.project
# --AWS--

# --MAESTRO--
CloudTenantRegionRulesMap = Dict[str, Dict[str, Dict[str, Set[str]]]]
# --MAESTRO--


@dataclass(frozen=True)
class FieldMatch:
    """
    A class to match a field in a record against a set of values.
    """

    path: tuple[str, ...]
    values: frozenset[Any]

    def __init__(self, path: tuple[str, ...], values: set[Any]):
        object.__setattr__(self, "path", path)
        object.__setattr__(self, "values", frozenset(values))


RequireAllCriteria = FieldMatch | tuple[str, ...]
"""Criteria that must be present in the record to keep it."""


@dataclass
class EventFilter:
    """
    This class is used to filter events based on the fields in the record.
    Reject if any of the fields in the record match the values in the FieldMatch.
    Require all of the fields in the record to match the values in the FieldMatch.
    Project the record to the fields in the projection.

    Example:
    >>> ef = EventFilter(
    ...     reject_if_any=[FieldMatch(("account",), {"1234567890"})],
    ...     require_all=[FieldMatch(("event",), {"create"})],
    ...     projection=[("event",), ("account",), ("metadata", "region",)],
    ... )
    >>> record = {"account": "1777", "event": "create", "name": "test", "metadata": {"region": "us-east-1", "name": "test"}}
    >>> ef.apply(record)
    {"event": "create", "account": "1777", "metadata": {"region": "us-east-1"}}
    >>> record2 = {"account": "1234567890", "event": "delete", "metadata": {"region": "us-east-1"}}
    >>> ef.apply(record2)
    None
    """

    reject_if_any: list[FieldMatch] = field(default_factory=list)
    """If any of the fields in the record match the values in the FieldMatch, reject the record."""
    require_all: list[RequireAllCriteria] = field(default_factory=list)
    """Require all of the fields in the record to match the values in the FieldMatch."""
    projection: list[tuple[str, ...]] = field(default_factory=list)
    """Project the record to the fields in the projection."""

    def should_reject(self, record: dict) -> bool:
        return any(deep_get(record, f.path) in f.values for f in self.reject_if_any)

    def should_keep(self, record: dict) -> bool:
        """
        Should keep record if all require_all criteria are matched.
        Criteria in require_all may be either FieldMatch (match by value) or tuple[str,...] (just present).
        """
        for f in self.require_all:
            if isinstance(f, FieldMatch):
                value = deep_get(record, f.path)
                if value not in f.values:
                    return False
            elif isinstance(f, tuple):
                # Only check that the value is present (not None)
                if deep_get(record, f) is None:
                    return False
            else:
                raise TypeError(f"Unsupported type in require_all: {type(f)}")
        return True

    def project(self, record: dict) -> dict:
        if not self.projection:
            return record
        _LOG.debug(f"Projecting record: {record} to paths: {self.projection}")
        result: dict = {}
        for path in self.projection:
            item = deep_get(record, path)
            if item is not None:
                deep_set(result, path, item)
        _LOG.debug(f"Projected record: {result}")
        return result

    def apply(self, record: dict) -> dict | None:
        if self.should_reject(record):
            _LOG.warning(f"Filtering out the record by reject condition: {record}")
            return None
        if not self.should_keep(record):
            _LOG.warning(f"Filtering out the record by keep condition: {record}")
            return None
        return self.project(record)


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

    def cloud_mapping(self, cloud: Cloud, license_key: str, version: Version) -> dict:
        data = self._event_mapping_provider.get_from_s3(
            version=version,
            license_key=license_key,
            cloud=cloud,
        )
        if data is None:
            _LOG.warning(
                f"No event mapping found for {cloud.value} in S3 "
                f"for license key: {license_key} and version: {version.to_str()} "
                f"May be metadata is not synced for this license key and version"
            )
            return {}
        return data

    @cached_property
    def eb_mapping(self) -> dict:
        from helpers.mappings.event_bridge_event_source_to_rules_mapping import (
            MAPPING,
        )

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

    @abstractmethod
    def get_filter(self, record: dict) -> EventFilter | None:
        """Return the EventFilter for a given record, or None to skip it."""

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
            ef = self.get_filter(record)
            if ef is None:
                continue
            result = ef.apply(record)
            if result is None:
                continue
            yield result
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
    def is_cloudtrail_api_call(record: dict) -> bool:
        return record.get(EB_DETAIL_TYPE) == EB_CLOUDTRAIL_API_CALL_DETAIL_TYPE

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


class MaestroEventProcessor(BaseEventProcessor, EventDrivenLicenseMixin):
    _MAESTRO_PROJECTION: list[tuple[str, ...]] = [
        (MA_EVENT_ACTION,),
        (MA_GROUP,),
        (MA_SUB_GROUP,),
        (MA_REGION_NAME,),
        (MA_EVENT_METADATA, MA_REQUEST, MA_CLOUD),
        (MA_EVENT_METADATA, MA_CLOUD),
        (MA_EVENT_METADATA, MA_EVENT_SOURCE),
        (MA_EVENT_METADATA, MA_EVENT_NAME),
        (MA_TENANT_NAME,),
    ]

    _MANAGEMENT_INSTANCE_FILTER: list[RequireAllCriteria] = [
        FieldMatch((MA_GROUP,), {"MANAGEMENT"}),
        FieldMatch((MA_SUB_GROUP,), {"INSTANCE"}),
    ]

    _FILTERS: dict[str, EventFilter] = {
        Cloud.AWS.value: EventFilter(
            require_all=[
                (MA_TENANT_NAME,),
                (MA_REGION_NAME,),
                (MA_EVENT_METADATA, MA_REQUEST, MA_CLOUD),
                (MA_EVENT_METADATA, MA_EVENT_SOURCE),
                (MA_EVENT_METADATA, MA_EVENT_NAME),
            ],
            projection=_MAESTRO_PROJECTION,
        ),
        Cloud.GOOGLE.value: EventFilter(
            require_all=_MANAGEMENT_INSTANCE_FILTER,
            projection=_MAESTRO_PROJECTION,
        ),
        Cloud.AZURE.value: EventFilter(
            require_all=_MANAGEMENT_INSTANCE_FILTER,
            projection=_MAESTRO_PROJECTION,
        ),
    }

    _GLOBAL_REGION_CLOUDS = frozenset({Cloud.AZURE.value, Cloud.GOOGLE.value})

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

    def get_filter(self, record: dict) -> EventFilter | None:
        cloud = record.get(MA_EVENT_METADATA, {}).get(MA_REQUEST, {}).get(MA_CLOUD)
        ef = self._FILTERS.get(cloud)
        if ef is None:
            _LOG.warning(
                f"Skipping record: {record} because cloud is not supported: {cloud}"
            )
        return ef

    def cloud_tenant_region_rules(
        self,
        it: Iterable[dict[str, Any]],
    ) -> Generator[Tuple[str, str, str, Set[str]], None, None]:
        """
        Maestro events does not contain account id or subscription id. They
        contain tenantName (maestro tenant) so we must rely on it.
        Yields tuples (cloud, tenantName, region, Set[rules])
        """
        for event in it:
            cloud = deep_get(event, (MA_EVENT_METADATA, MA_REQUEST, MA_CLOUD))
            tenant_name = deep_get(event, (MA_TENANT_NAME,))
            if cloud in self._GLOBAL_REGION_CLOUDS:
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
        self,
        it: Iterable[Dict],
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
        version: Version = DEFAULT_VERSION,
    ) -> Set[str]:
        if cloud == Cloud.AWS.value:
            return self._get_rules_aws(event, license_key, version)
        return self._get_rules_via_maestro_mapping(event, cloud, license_key, version)

    @staticmethod
    def _maestro_mapping(cloud: str) -> dict:
        if cloud == Cloud.AZURE.value:
            from helpers.mappings.maestro_subgroup_action_to_azure_events_mapping import (
                MAPPING,
            )

            return MAPPING
        if cloud == Cloud.GOOGLE.value:
            from helpers.mappings.maestro_subgroup_action_to_google_events_mapping import (
                MAPPING,
            )

            return MAPPING
        return {}

    def _get_rules_aws(
        self,
        event: dict,
        license_key: str,
        version: Version = DEFAULT_VERSION,
    ) -> Set[str]:
        record = cast(dict[str, Any], event.get(MA_EVENT_METADATA, {}))
        ct_mapping = self.cloud_mapping(Cloud.AWS, license_key, version)
        return CloudTrail.get_rules(record, ct_mapping)

    def _get_rules_via_maestro_mapping(
        self,
        event: dict,
        cloud: str,
        license_key: str,
        version: Version = DEFAULT_VERSION,
    ) -> set[str]:
        maestro_map = self._maestro_mapping(cloud)
        cloud_enum = Cloud(cloud)
        cloud_map = self.cloud_mapping(cloud_enum, license_key, version)
        sub_group = event.get(MA_SUB_GROUP)
        action = event.get(MA_EVENT_ACTION)
        cloud_events: list[list[str]] = maestro_map.get(sub_group, {}).get(action, [])
        rules: set[str] = set()
        for e_source, e_name in cloud_events:
            rules.update(cloud_map.get(e_source, {}).get(e_name, []))
        return rules


class EventBridgeEventProcessor(BaseEventProcessor, EventDrivenLicenseMixin):

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

        reject: list[FieldMatch] = []
        account_id = self.sts_client.get_account_id()
        if account_id != Env.DEV_ACCOUNT_ID.get():
            reject = [
                FieldMatch((EB_ACCOUNT_ID,), {account_id}),
                FieldMatch(
                    (EB_DETAIL, CT_USER_IDENTITY, CT_ACCOUNT_ID),
                    {account_id},
                ),
            ]

        self._filter = EventFilter(
            reject_if_any=reject,
            require_all=[
                FieldMatch(
                    (EB_DETAIL_TYPE,),
                    {EB_CLOUDTRAIL_API_CALL_DETAIL_TYPE},
                ),
            ],
            projection=[
                (EB_DETAIL_TYPE,),
                (EB_DETAIL, CT_EVENT_NAME),
                (EB_DETAIL, CT_EVENT_SOURCE),
                (EB_DETAIL, CT_USER_IDENTITY, CT_ACCOUNT_ID),
                (EB_DETAIL, CT_REGION),
            ],
        )

    def get_filter(self, record: dict) -> EventFilter | None:
        return self._filter

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
        ct_mapping = self.cloud_mapping(
            Cloud.AWS,
            license_key=license_key,
            version=version,
        )
        if CloudTrail.is_cloudtrail_api_call(record):
            return CloudTrail.get_rules(record.get(EB_DETAIL) or {}, ct_mapping)
        # TODO EB source to list of rules is a temp solution I was able to
        #  make, but it would be better to use EB detail-type here
        source = record.get(EB_EVENT_SOURCE)
        return set(self.eb_mapping.get(source) or [])
