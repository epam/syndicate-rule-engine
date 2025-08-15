from uuid import uuid4
from time import time
from datetime import datetime, timezone
from typing import Generator, Iterable
from functools import cmp_to_key

from pynamodb.pagination import ResultIterator
from modular_sdk.models.tenant import Tenant

from helpers.constants import Cloud, Severity
from helpers.log_helper import get_logger
from helpers.reports import SeverityCmp, keep_highest
from models.resource_exception import ResourceException
from services.base_data_service import BaseDataService
from services.resources import (
    load_cc_providers,
    prepare_resource_type,
    to_aws_resources,
    to_azure_resources,
    to_google_resources,
    to_k8s_resources,
    CloudResource,
    AWSResource,
    AZUREResource,
    GOOGLEResource,
    K8SResource,
)
from services.sharding import ShardsCollection, ShardPart
from services.metadata import Metadata, RuleMetadata

_LOG = get_logger(__name__)


class ResourceExceptionsCollection:
    _tag_end = '$$'

    def __init__(self, resource_exceptions: Iterable[ResourceException]):
        self._build_exception_maps(resource_exceptions)

    def _build_exception_maps(
        self, resource_exceptions: Iterable[ResourceException]
    ):
        """
        Builds a maps of resource exceptions for quick access.
        """
        self.exceptions = dict()
        self.arn_map = dict()
        self.resource_map = dict()
        self.tags_map = dict()
        for exception in resource_exceptions:
            self.exceptions[exception.id] = exception

            if exception.arn:
                self.arn_map[exception.arn] = exception.id
                continue

            if exception.resource_id:
                resource = (
                    exception.resource_id,
                    exception.resource_type,
                    exception.location,
                )
                self.resource_map[resource] = exception.id
                continue

            if exception.tags_filters:
                self._expand_tags_map(exception.tags_filters, exception.id)

    def _expand_tags_map(self, tags_filters: list[str], exception_id: str):
        sorted_tags = sorted(tags_filters)
        node = self.tags_map
        for tag in sorted_tags:
            if tag not in node:
                node[tag] = dict()
                node = node[tag]
            else:
                node = node[tag]
        node[self._tag_end] = exception_id

    def _match_tags(self, tags: set[str]) -> str | None:
        sorted_tags = sorted(tags)

        nodes = [self.tags_map]
        for tag in sorted_tags:
            for node in nodes:
                if self._tag_end in node:
                    return node[self._tag_end]
                if tag in node:
                    nodes.append(node[tag])

        return None

    def _in_exceptions_aws(self, resource: AWSResource) -> str | None:
        """
        Check if the AWS resource is in the exceptions.
        Returns the exception ID if found, None otherwise.
        """
        if (
            resource.id,
            resource.resource_type,
            resource.location,
        ) in self.resource_map:
            return self.resource_map[
                (resource.id, resource.resource_type, resource.location)
            ]
        if resource.arn in self.arn_map:
            return self.arn_map[resource.arn]

        tags = {f'{key}={value}' for key, value in resource.tags.items()}
        tag_match = self._match_tags(tags)
        if tag_match:
            return tag_match

        return None

    def _in_exceptions_azure(self, resource: AZUREResource) -> str | None:
        """
        Check if the Azure resource is in the exceptions.
        Returns the exception ID if found, None otherwise.
        """
        if (
            resource.id,
            resource.resource_type,
            resource.location,
        ) in self.resource_map:
            return self.resource_map[
                (resource.id, resource.resource_type, resource.location)
            ]
        if resource.id in self.arn_map:
            return self.arn_map[resource.id]

        tags = {f'{key}={value}' for key, value in resource.tags.items()}
        tag_match = self._match_tags(tags)
        if tag_match:
            return tag_match

        return None

    def _in_exceptions_google(self, resource: GOOGLEResource) -> str | None:
        """
        Check if the Google resource is in the exceptions.
        Returns the exception ID if found, None otherwise.
        """
        if (
            resource.id,
            resource.resource_type,
            resource.location,
        ) in self.resource_map:
            return self.resource_map[
                (resource.id, resource.resource_type, resource.location)
            ]
        if resource.urn in self.arn_map:
            return self.arn_map[resource.urn]

        tags = {f'{key}={value}' for key, value in resource.tags.items()}
        tag_match = self._match_tags(tags)
        if tag_match:
            return tag_match

        return None

    def _in_exceptions_k8s(self, resource: K8SResource) -> str | None:
        """
        Check if the K8s resource is in the exceptions.
        Returns the exception ID if found, None otherwise.
        """
        if (
            resource.id,
            resource.resource_type,
            resource.location,
        ) in self.resource_map:
            return self.resource_map[
                (resource.id, resource.resource_type, resource.location)
            ]
        if resource.id in self.arn_map:
            return self.arn_map[resource.id]

        tags = {f'{key}={value}' for key, value in resource.tags.items()}
        tag_match = self._match_tags(tags)
        if tag_match:
            return tag_match

        return None

    def _in_exceptions(self, resource: CloudResource) -> str | None:
        """
        Check if the resource is in the exceptions.
        Returns the exception ID if found, None otherwise.
        """
        if isinstance(resource, AWSResource):
            return self._in_exceptions_aws(resource)
        elif isinstance(resource, AZUREResource):
            return self._in_exceptions_azure(resource)
        elif isinstance(resource, GOOGLEResource):
            return self._in_exceptions_google(resource)
        elif isinstance(resource, K8SResource):
            return self._in_exceptions_k8s(resource)
        return None

    def _filter_shard_part(
        self, shard: ShardPart, resources: Iterable[CloudResource]
    ) -> tuple[dict[str, list[CloudResource]], ShardPart]:
        exceptions = {}
        non_exception_resources = []
        for res in resources:
            exception_id = self._in_exceptions(res)
            if exception_id:
                exceptions.setdefault(exception_id, []).append(res)
            else:
                non_exception_resources.append(res.data)

        non_exceptions = ShardPart(
            policy=shard.policy,
            location=shard.location,
            timestamp=shard.timestamp,
            resources=non_exception_resources,
            error=shard.error,
            previous_timestamp=shard.previous_timestamp,
        )
        return exceptions, non_exceptions

    def _get_resources_data(
        self,
        rule_resources: dict[str, set[CloudResource]],
        metadata: Metadata,
    ) -> dict[str, int]:
        """
        Get resources data grouped by severity, similar to OverviewReportGenerator.get_resources_severities
        """
        sev_resources = {}
        for rule in rule_resources:
            sev = metadata.rule(rule).severity.value
            sev_resources.setdefault(sev, set()).update(rule_resources[rule])
        
        # Keep only the highest severity for each unique resource
        keep_highest(
            *[
                sev_resources[k]
                for k in sorted(
                    sev_resources.keys(), key=cmp_to_key(SeverityCmp())
                )
            ]
        )
        
        res = {sev.value: 0 for sev in Severity}
        for sev, resources in sev_resources.items():
            res[sev] += len(resources)
        return res

    def _get_violations_data(
        self,
        rule_resources: dict[str, set[CloudResource]],
        metadata: Metadata,
    ) -> dict[str, int]:
        """
        Get violations data grouped by severity, similar to OverviewReportGenerator.get_violations_severities
        """
        res = {sev.value: 0 for sev in Severity}
        for rule, resources in rule_resources.items():
            res[metadata.rule(rule).severity.value] += len(resources)
        return res

    def _get_attacks_data(
        self,
        rule_resources: dict[str, set[CloudResource]],
        metadata: Metadata,
    ) -> dict[str, int]:
        """
        Get attacks data grouped by severity, similar to OverviewReportGenerator.get_attacks_severities
        """
        unique_resource_to_attack_rules = {}
        for rule in rule_resources:
            rm = metadata.rule(rule)
            # Get all MITRE attacks for this rule
            rule_attacks = tuple(rm.iter_mitre_attacks())
            if not rule_attacks:
                continue
            for resource in rule_resources[rule]:
                inner = unique_resource_to_attack_rules.setdefault(
                    resource, {}
                )
                for attack in rule_attacks:
                    inner.setdefault(attack, []).append(rule)

        res = {sev.value: 0 for sev in Severity}
        for resource in unique_resource_to_attack_rules:
            for attack, rules in unique_resource_to_attack_rules[
                resource
            ].items():
                sev = sorted(
                    [metadata.rule(r).severity.value for r in rules],
                    key=cmp_to_key(SeverityCmp()),
                )[-1]
                res[sev] += 1
        return res

    def filter_exception_resources(
        self,
        collection: ShardsCollection,
        cloud: Cloud,
        metadata: Metadata,
        account_id: str = '',
    ) -> tuple[list[dict], ShardsCollection]:
        """
        Filters the shard collection based on resource exceptions.
        Returns a tuple of two collections:
        - The first collection contains shards that match the resource exceptions.
        - The second collection contains shards that do not match the resource exceptions.

        :param shard_collection:
        :param metadata:
        """
        load_cc_providers()

        meta = collection.meta

        exception_rule_resource = {}
        non_exception_collection = ShardsCollection(
            collection.distributor, collection.io
        )
        non_exception_collection.meta = collection.meta

        for part in collection.iter_parts():
            rt = prepare_resource_type(meta[part.policy]['resource'], cloud)
            exceptions, non_exception_part = self._filter_shard_part(
                part,
                _shard_to_resources(
                    cloud, part, rt, metadata.rule(part.policy), account_id
                ),
            )
            non_exception_collection.put_part(non_exception_part)
            for exception_id, resource_list in exceptions.items():
                exception_rule_resource.setdefault(
                    exception_id, {}
                ).setdefault(part.policy, set()).update(resource_list)

        for part in collection.iter_error_parts():
            non_exception_collection.put_part(part)

        exception_data = []
        for exception_id, rule_resources in exception_rule_resource.items():
            exception_data.append(
                {
                    'exception': self.exceptions[exception_id].to_dict(),
                    'type': self.exceptions[exception_id].type,
                    'added_date': self.exceptions[exception_id].created_at,
                    'expiration_data': self.exceptions[
                        exception_id
                    ].expire_at.timestamp(),
                    'summary': {
                        'resources_data': self._get_resources_data(
                            rule_resources, metadata
                        ),
                        'violations_data': self._get_violations_data(
                            rule_resources, metadata
                        ),
                        'attacks_data': self._get_attacks_data(
                            rule_resources, metadata
                        ),
                    },
                }
            )

        return exception_data, non_exception_collection


class ResourceExceptionsService(BaseDataService[ResourceException]):
    def create(
        self,
        resource_id: str | None,
        location: str | None,
        resource_type: str | None,
        tenant_name: str | None,
        customer_name: str,
        arn: str | None,
        tags_filters: list[str] | None,
        expire_at: float,
    ) -> ResourceException:
        return ResourceException(
            id=str(uuid4()),
            resource_id=resource_id,
            location=location,
            resource_type=resource_type,
            tenant_name=tenant_name,
            customer_name=customer_name,
            arn=arn,
            tags_filters=tags_filters,
            created_at=time(),
            updated_at=time(),
            expire_at=datetime.fromtimestamp(expire_at, tz=timezone.utc),
        )

    def delete_by_id(self, id: str) -> None:
        """
        Delete a resource exception by its ID.
        """
        resource_exception = ResourceException.get_nullable(id)
        if resource_exception:
            resource_exception.delete()
        else:
            raise ValueError(f'Resource exception with ID {id} not found')

    def get_resource_exception_by_id(
        self, id: str
    ) -> ResourceException | None:
        """
        Get a resource exception by its ID.
        """
        return ResourceException.get_nullable(id)

    def get_resources_exceptions(
        self,
        resource_id: str | None = None,
        location: str | None = None,
        resource_type: str | None = None,
        tenant_name: str | None = None,
        customer_name: str | None = None,
        arn: str | None = None,
        tags_filters: list[str] | None = None,
        limit: int | None = None,
        last_evaluated_key: dict | None = None,
    ) -> ResultIterator[ResourceException]:
        filter_condition = None
        if resource_id:
            filter_condition &= ResourceException.resource_id == resource_id
        if location:
            filter_condition &= ResourceException.location == location
        if resource_type:
            filter_condition &= (
                ResourceException.resource_type == resource_type
            )
        if tenant_name:
            filter_condition &= ResourceException.tenant_name == tenant_name
        if customer_name:
            filter_condition &= (
                ResourceException.customer_name == customer_name
            )
        if arn:
            filter_condition &= ResourceException.arn == arn
        if tags_filters:
            for tag in tags_filters:
                filter_condition &= ResourceException.tags_filters.contains(tag)

        kwargs = dict()
        if limit is not None:
            kwargs['limit'] = limit
        if last_evaluated_key:
            kwargs['last_evaluated_key'] = last_evaluated_key

        # NOTE: it's not actual scan, we use compound index in mongo
        # that is not supported in modular SDK
        if filter_condition is not None:
            return ResourceException.scan(filter_condition, **kwargs)
        else:
            return ResourceException.scan(**kwargs)

    def get_resource_exceptions_collection_by_tenant(
        self, tenant: Tenant
    ) -> ResourceExceptionsCollection:
        """
        Get a collection of resource exceptions for a specific tenant.
        """
        resource_exceptions = self.get_resources_exceptions(
            customer_name=tenant.customer_name, tenant_name=tenant.name
        )
        if resource_exceptions:
            return ResourceExceptionsCollection(resource_exceptions)
        return ResourceExceptionsCollection([])

    def update_resource_exception_by_id(
        self,
        id: str,
        expire_at: float,
        resource_id: str | None = None,
        location: str | None = None,
        resource_type: str | None = None,
        tenant_name: str | None = None,
        customer_name: str | None = None,
        arn: str | None = None,
        tags_filters: list[str] | None = None,
    ) -> ResourceException:
        """
        Update a resource exception by its ID.
        """
        resource_exception = self.get_resource_exception_by_id(id)
        if not resource_exception:
            raise ValueError(f'Resource exception with ID {id} not found')

        resource_exception.resource_id = resource_id
        resource_exception.location = location
        resource_exception.resource_type = resource_type
        resource_exception.tenant_name = tenant_name
        resource_exception.customer_name = customer_name
        resource_exception.arn = arn
        resource_exception.tags_filters = tags_filters
        resource_exception.updated_at = time()
        resource_exception.expire_at = datetime.fromtimestamp(
            expire_at, tz=timezone.utc
        )

        resource_exception.save()

        return resource_exception


def _shard_to_resources(
    cloud: Cloud,
    shard: ShardPart,
    rt: str,
    metadata: RuleMetadata,
    account_id: str,
) -> Generator[CloudResource, None, None]:
    match cloud:
        case Cloud.AWS:
            return to_aws_resources(shard, rt, metadata, account_id)
        case Cloud.AZURE:
            return to_azure_resources(shard, rt)
        case Cloud.GCP:
            return to_google_resources(shard, rt, metadata, account_id)
        case Cloud.K8S:
            return to_k8s_resources(shard, rt)
        case _:
            raise ValueError(f'Unsupported cloud: {cloud}')
