from datetime import datetime, timezone
from abc import ABC, abstractmethod
from typing import TypeVar, Generic

from boto3 import Session
from botocore.client import BaseClient
from azure.identity import DefaultAzureCredential
from azure.mgmt.resourcegraph import ResourceGraphClient
from azure.mgmt.resourcegraph.models import QueryRequest, QueryRequestOptions

from helpers.log_helper import get_logger
from services.resources import CloudResource, AWSResource, AZUREResource

_LOG = get_logger(__name__)

T = TypeVar('T', bound=CloudResource)

class ResourceCollector(ABC, Generic[T]):
    """
    Abstract base class for resource collectors.
    Defines the interface for collecting resources from cloud providers.
    """

    @abstractmethod
    def collect(self, query: str | None = None) -> list[T]:
        """
        Collect resources based on the provided query.
        :param query: A query string to filter resources.
        :return: List of collected resources.
        """
        pass

    @abstractmethod
    def collect_all(self) -> list[T]:
        """
        Collect all resources without any filtering.
        :return: List of all collected resources.
        """
        pass

class AWSResourceCollector(ResourceCollector[AWSResource]):
    """
    Collect resources from AWS using Resource Explorer.
    Supported resource types: https://docs.aws.amazon.com/resource-explorer/latest/userguide/supported-resource-types.html
    """

    __slots__ = ('_session', '_regions', '_explorer_client', '_view_arn', )

    def __init__(self, session: Session, view_arn: str | None = None):
        """
        Initialize the AWS Resource Collector.
        If no view ARN is provided, default is used.
        If no default view is found, it will attempt to
        index resources across all regions and create default view.
        """
        self._explorer_client = None

        self._session = session
        self._regions = session.get_available_regions('resource-explorer-2')
        if view_arn:
            self.view_arn = view_arn
        else:
            self._init_explorer()

    @property
    def view_arn(self) -> str:
        return self._view_arn

    @view_arn.setter
    def view_arn(self, value: str):
        """
        Set the view ARN for Resource Explorer.
        This is used to specify which view to use when collecting resources.
        """
        try:
            self._session.client(
                service_name='resource-explorer-2'
            ).get_view(
                ViewArn=value,
            )
            self._view_arn = value
        except Exception as e:
            _LOG.error(f'Invalid view ARN provided: {value}. Error: {e}')
            raise ValueError(f'Invalid view ARN: {value}') from e

    @property
    def view_region(self) -> str:
        return self.view_arn.split(':')[3]

    def _get_default_view(self) -> str | None:
        """
        Get the default view ARN for Resource Explorer.
        If no default view is found, return None.
        """
        try:
            explorer_client = self._session.client(
                service_name='resource-explorer-2',
            )
            resp = explorer_client.get_default_view()
            return resp['ViewArn']
        except Exception as e:
            _LOG.info(f'Failed to get default view: {e}')
        return None

    def _create_explorer_index(self, region: str):
        """
        Create Resource Explorer index in the specified region.
        """
        try:
            explorer_client = self._session.client(
                service_name='resource-explorer-2'
            )
            if not explorer_client.list_indexes():
                _LOG.info(f'Creating Resource Explorer index in {region}')
                explorer_client.create_index()
        except Exception as e:
            _LOG.error(
                f'Failed to create Resource Explorer index in {region}: {e}'
            )
            raise

    def _create_explorer_view(self):
        """
        Create Resource Explorer view in the specified region.
        This will create a view for the indexed resources.
        """
        _LOG.info('Creating Resource Explorer view')
        try:
            explorer_client = self._session.client(
                service_name='resource-explorer-2'
            )
            resp = explorer_client.create_view(ViewName='all-resources')
            self._view_arn = resp['View']['ViewArn']
            explorer_client.associate_default_view(
                ViewArn=self._view_arn
            )
        except Exception as e:
            _LOG.error(
                f'Failed to create Resource Explorer view: {e}'
            )
            raise

    def _init_explorer(self):
        """
        Index all the resources across all regions.
        """
        view_arn = self._get_default_view()
        if view_arn:
            self.view_arn = view_arn
            _LOG.info(f'Using existing view: {self.view_arn}')
            return
        
        for region in self._regions:
            try:
                self._create_explorer_index(region)
            except Exception as e:
                _LOG.error(
                    f'Failed to create Resource Explorer index in {region}: {e}'
                )

        self._create_explorer_view()

    @property
    def explorer_client(self) -> BaseClient:
        """
        Get the Resource Explorer client for the specified view region.
        """
        if not self._explorer_client:
            self._explorer_client = self._session.client(
                service_name='resource-explorer-2',
            )
        return self._explorer_client

    @staticmethod
    def _resource_item_to_aws_resource(
        resource_item: dict
    ) -> AWSResource:
        """
        Convert a resource item from Resource Explorer to an AWSResource object.

        :param resource_item: A dictionary representing a resource item.
        :return: An AWSResource object.
        """
        arn = resource_item.get('Arn')

        parts = arn.split('/')
        if len(parts) >= 3:
            resource_id = parts[-1]
            name = parts[-2]
        else:
            resource_id = (
                arn.split('/')[-1] if '/' in arn else arn.split(':')[-1]
            )
            name = resource_id

        data = {}
        for prop in resource_item.get('Properties', []):
            if name := prop.get('Name'):
                data[name] = prop.get('Data')

        return AWSResource(
            id=resource_id,
            name=name,
            data=data,
            resource_type=resource_item.get('ResourceType'),
            arn=arn,
            region=resource_item.get('Region'),
            date=datetime.now(timezone.utc).date().isoformat(),
            sync_date=datetime.now(timezone.utc).timestamp(),
        )

    def collect(
        self,
        query: str | None = None,
    ) -> list[AWSResource]:
        """
        Collect resources of specified types from given locations.
        Transform the collected resource items into AWSResource objects.

        :param query: A query string to filter resources.
            If None, all resources will be collected.
            Example query: `service:ec2`.

        :return: List of collected resources.
        """
        args = {
            'ViewArn': self.view_arn,
            'MaxResults': 1000,
        }
        if query:
            args['Filters'] = {
                'FilterString': query,
            }

        paginator = self.explorer_client.get_paginator('list_resources')

        resource_items = []
        for page in paginator.paginate(
            **args
        ):
            resource_items.extend(page.get('Resources', []))

        resources = []
        for resource_item in resource_items:
            resources.append(
                self._resource_item_to_aws_resource(resource_item)
            )

        return resources

    def collect_all(self) -> list[AWSResource]:
        """
        Collect all AWS resources across all regions.
        """
        return self.collect()


class AZUREResourceCollector(ResourceCollector[AZUREResource]):
    """ 
    Collect resources from Azure using Resource Management API.
    """


    __slots__ = ('_credential', '_subscription_id')

    def __init__(self, subscription_id: str, credential=None):
        self._credential = credential or DefaultAzureCredential()
        self._subscription_id = subscription_id

    def _resource_item_to_azure_resource(
        self, resource_item: dict
    ) -> AZUREResource:
        """
        Convert a resource item from Azure Resource Graph to an AZUREResource object.

        :param resource_item: A dictionary representing a resource item from Resource Graph.
        :return: An AZUREResource object.
        """
        data = {}
        
        if res_group := resource_item.get('resourceGroup'):
            data['resourceGroup'] = res_group
            
        if tags := resource_item.get('tags'):
            data['tags'] = tags
            
        return AZUREResource(
            id=resource_item['id'],
            name=resource_item['name'],
            location=resource_item['location'],
            data=data,
            resource_type=resource_item['type'],
            sync_date=datetime.now(timezone.utc).timestamp(),
        )

    def collect(
        self,
        query: str | None = None,
    ) -> list[AZUREResource]:
        """
        Collect resources of specified types from given locations using Azure Resource Graph.

        :param query: A KQL query string to filter resources.
            If None, all resources will be collected.
            Example query: `where type == 'microsoft.compute/virtualmachines'`.
        :return: List of collected resources.
        """
        resource_graph_client = ResourceGraphClient(
            credential=self._credential
        )
        
        base_query = """
        resources
        | project id, name, type, location, resourceGroup, tags
        """
        
        if query:
            if query.strip().startswith('resources'):
                kql_query = query
            else:
                kql_query = f"{base_query.strip()} | {query}"
        else:
            kql_query = base_query

        resources = []
        skip = 0
        top = 1000
        
        try:
            while True:
                query_options = QueryRequestOptions(
                    skip=skip,
                    top=top,
                    result_format='objectArray'
                )
                
                query_request = QueryRequest(
                    subscriptions=[self._subscription_id],
                    query=kql_query,
                    options=query_options
                )
                
                response = resource_graph_client.resources(query_request).as_dict()
                resources.extend(response['data'])

                skip += top
                if skip >= response['total_records']:
                    break
                
        except Exception as e:
            _LOG.error(f"Failed to collect Azure resources using Resource Graph: {e}")
            raise
            
        return [
            self._resource_item_to_azure_resource(resource_dict)
            for resource_dict in resources
        ]

    def collect_all(self) -> list[AZUREResource]:
        """
        Collect all Azure resources in the subscription.
        """
        return self.collect()
