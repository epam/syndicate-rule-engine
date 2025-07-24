# NOTE: It 's alternative implementations for resource collector.
# It isn't used anywhere. We can plug them in later if needed.

import csv
import gzip
from io import StringIO, BytesIO
from typing import Iterable
from datetime import datetime, timezone
from importlib import import_module
from functools import lru_cache
import json

# from c7n.resources.resource_map import ResourceMap as AWSResourceMap
from c7n_azure.resources.resource_map import ResourceMap as AzureResourceMap
# from c7n_gcp.resources.resource_map import ResourceMap as GCPResourceMap

from azure.mgmt.resourcegraph import ResourceGraphClient
from azure.mgmt.resourcegraph.models import QueryRequest, QueryRequestOptions

from modular_sdk.modular import ModularServiceProvider
from modular_sdk.models.tenant import Tenant

from helpers.log_helper import get_logger
from helpers.constants import Cloud, ResourcesCollectorType
from executor.job import get_tenant_credentials
from services import SP
from services.resources_service import ResourcesService
from services.resources_collector import BaseResourceCollector

_LOG = get_logger(__name__)

def get_factory(class_path: str):
    """
    Returns a factory function that imports a class from the given path.
    """
    module_path, class_name = class_path.rsplit('.', 1)
    module = import_module(module_path)
    return getattr(module, class_name)

@lru_cache(maxsize=1)
def azure_custodian_resource_type_map() -> dict[str, str]:
    """
    Maps Azure resource types to Cloud Custodian resource types.
    """
    type_map = {}
    for resource_type, class_ in AzureResourceMap.items():
        if hasattr(get_factory(class_).resource_type, 'resource_type'):
            type_map[get_factory(class_).resource_type.resource_type.lower()] = resource_type
    return type_map

@lru_cache(maxsize=1)
def custodian_azure_resource_type_map() -> dict[str, str]:
    """
    Maps Cloud Custodian resource types to Azure resource types.
    """
    type_map = {}
    for resource_type, class_ in AzureResourceMap.items():
        if hasattr(get_factory(class_).resource_type, 'resource_type'):
            type_map[resource_type] = get_factory(class_).resource_type.resource_type.lower()
    return type_map

class AzureGraphResourceCollector(BaseResourceCollector):
    """ 
    Collect resources from Azure using Resource Management API.
    """

    def __init__(
        self,
        modular_service: ModularServiceProvider,
        resources_service: ResourcesService,
    ):
        self._ms = modular_service
        self._rs = resources_service

    @property
    def collector_type(self) -> ResourcesCollectorType:
        return ResourcesCollectorType.AZURE_RESOURCE_GRAPH

    @classmethod
    def build(cls) -> 'AzureGraphResourceCollector':
        """
        Builds a ResourceCollector instance.
        """
        return AzureGraphResourceCollector(
            modular_service=SP.modular_client,
            resources_service=SP.resources_service,
        )

    def _get_credentials(self, tenant: Tenant) -> dict:
        credentials = get_tenant_credentials(tenant)
        if not credentials:
            raise ValueError(f'No credentials found for tenant {tenant.name}')

        return credentials

    def _load_scan(
        self,
        data: dict,
        account_id: str,
        tenant_name: str,
        customer_name: str,
    ):
        resource_id = data['id']
        resource_name = data.get('name') or resource_id
        location = data['location']
        
        resource_type = azure_custodian_resource_type_map().get(
            data['type'].lower()
        )
        if not resource_type:
            _LOG.warning(
                f"Resource type {data['type']} not found in Azure resource map. "
                "Skipping resource."
            )
            return

        self._rs.create(
            id=resource_id,
            name=resource_name,
            location=location,
            resource_type=resource_type,
            account_id=account_id,
            tenant_name=tenant_name,
            customer_name=customer_name,
            data=data,
            sync_date=datetime.now(timezone.utc).timestamp(),
            collector_type=self.collector_type,
        ).save()
    
    def collect_tenant_resources(
        self,
        tenant_name: str,
        regions: Iterable[str] | None = None,
        resource_types: Iterable[str] | None = None,
        **kwargs
    ):
        tenant = self._ms.tenant_service().get(tenant_name)
        if not tenant:
            raise ValueError(f'Tenant {tenant_name} not found')
        account_id = tenant.project if tenant.project else ""


        resource_graph_client = ResourceGraphClient(
            credential=self._get_credentials(tenant)
        )
        
        # NOTE: We can use just `resources` query to get all resources
        # but it will return all the attributes of resources. That can be too much
        query = "resources"
        if regions:
            regions = [region.lower() for region in regions]
            query += ' | where location in (' + ', '.join(f"'{r}'" for r in regions) + ')'
        if resource_types:
            type_map = custodian_azure_resource_type_map()
            resource_types = [type_map[rt.lower()] for rt in resource_types]
            query += ' | where type in (' + ', '.join(f"'{rt}'" for rt in resource_types) + ')'
        query += ' | project id, name, type, location, tags'

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
                    subscriptions=[account_id],
                    query=query,
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
            
        for resource in resources:
            self._load_scan(
                data=resource,
                account_id=account_id,
                tenant_name=tenant.name,
                customer_name=tenant.customer_name,
            )

    def collect_all_resources(
        self,
        regions: Iterable[str] | None = None,
        resource_types: Iterable[str] | None = None,
        **kwargs
    ):
        """
        Collect all Azure resources in the subscription using Resource Graph.
        """
        tenants = self._ms.tenant_service().scan_tenants(only_active=True)
        for tenant in tenants:
            if tenant.cloud != Cloud.AZURE:
                continue
            try:
                _LOG.info(f'Collecting resources for tenant: {tenant.name}')
                self.collect_tenant_resources(
                    tenant_name=tenant.name,
                    regions=regions,
                    resource_types=resource_types,
                    **kwargs
                )
            except Exception as e:
                _LOG.error(
                    f'Error collecting resources for tenant {tenant.name}: {e}'
                )

class AzureFOCUSResourceCollector(BaseResourceCollector):
    """
    Collect resources from Azure FOCUS (FinOps Cost and Usage Specification) data.
    FOCUS data is typically exported to Azure blob storage in CSV or gzipped CSV format.
    This collector extracts resource information from FOCUS billing data.
    """

    def __init__(
        self,
        modular_service: ModularServiceProvider,
        resources_service: ResourcesService,
        storage_account_name: str,
        container_name: str,
        blob_prefix: str,
        connection_string: str | None = None
    ):
        self._ms = modular_service
        self._rs = resources_service
        self._storage_account_name = storage_account_name
        self._container_name = container_name
        self._blob_prefix = blob_prefix
        self._connection_string = connection_string

    @property
    def collector_type(self) -> ResourcesCollectorType:
        return ResourcesCollectorType.FOCUS

    @classmethod
    def build(
        cls, 
        storage_account_name: str,
        container_name: str,
        blob_prefix: str = "",
        connection_string: str | None = None
    ) -> 'AzureFOCUSResourceCollector':
        """
        Builds a ResourceCollector instance.
        """
        return AzureFOCUSResourceCollector(
            modular_service=SP.modular_client,
            resources_service=SP.resources_service,
            storage_account_name=storage_account_name,
            container_name=container_name,
            blob_prefix=blob_prefix,
            connection_string=connection_string
        )

    def _get_credentials(self, tenant: Tenant):
        """Get Azure credentials for the tenant"""
        credentials = get_tenant_credentials(tenant)
        if not credentials:
            raise ValueError(f'No credentials found for tenant {tenant.name}')
        return credentials

    def _get_blob_service_client(self, tenant: Tenant):
        """Create Azure Blob Service Client with tenant credentials"""
        from azure.storage.blob import BlobServiceClient
        
        credentials = self._get_credentials(tenant)
        
        if self._connection_string:
            return BlobServiceClient.from_connection_string(self._connection_string)
        else:
            account_url = f"https://{self._storage_account_name}.blob.core.windows.net"
            return BlobServiceClient(account_url=account_url, credential=credentials)

    def _get_latest_focus_blob(self, container_client):
        """Get the latest FOCUS export blob from the container"""
        blobs = container_client.list_blobs(name_starts_with=self._blob_prefix)
        
        data_blobs = [
            blob for blob in blobs 
            if (blob.name.endswith('.csv') or blob.name.endswith('.csv.gz')) 
            and not blob.name.endswith('manifest.json')
        ]
        
        if not data_blobs:
            raise ValueError(f"No FOCUS export data files found with prefix '{self._blob_prefix}'")
            
        latest_blob = max(data_blobs, key=lambda x: x.last_modified)
        _LOG.info(f"Latest FOCUS file: {latest_blob.name}")
        return latest_blob

    def _load_focus_data(self, blob_client, blob_name: str):
        """Load FOCUS data from blob (CSV format, optionally gzipped)"""
        blob_data = blob_client.download_blob().readall()
        
        if blob_name.endswith('.csv.gz'):
            with gzip.GzipFile(fileobj=BytesIO(blob_data)) as gz_file:
                csv_content = gz_file.read().decode('utf-8')
            csv_reader = csv.DictReader(StringIO(csv_content))
            focus_data = list(csv_reader)
            
        elif blob_name.endswith('.csv'):
            csv_content = blob_data.decode('utf-8')
            csv_reader = csv.DictReader(StringIO(csv_content))
            focus_data = list(csv_reader)
            
        else:
            raise ValueError(f"Unsupported file format for blob: {blob_name}. Only CSV and CSV.GZ files are supported.")
            
        _LOG.info(f"Loaded {len(focus_data)} rows of FOCUS data")
        return focus_data
            
        

    def _extract_resource_info(self, focus_row: dict) -> dict | None:
        """
        Extract resource information from FOCUS data row.
        FOCUS spec typically includes ResourceId, ResourceName, ResourceType, Region, etc.
        """
        resource_id = focus_row.get('ResourceId') or focus_row.get('resource_id')
        resource_name = focus_row.get('ResourceName') or focus_row.get('resource_name') 
        resource_type = focus_row.get('ResourceType') or focus_row.get('resource_type')
        region = focus_row.get('Region') or focus_row.get('region') or focus_row.get('AvailabilityZone')
        
        tags = {}
        tags_field = focus_row.get('Tags') or focus_row.get('tags')
        if tags_field:
            try:
                if isinstance(tags_field, str):
                    tags = json.loads(tags_field)
                elif isinstance(tags_field, dict):
                    tags = tags_field
            except (json.JSONDecodeError, TypeError):
                _LOG.warning(f"Failed to parse tags: {tags_field}")

        if not resource_id or not resource_type:
            return None

        if resource_type:
            mapped_resource_type = azure_custodian_resource_type_map().get(
                resource_type.lower()
            )
        else:
            mapped_resource_type = None
        
        if not mapped_resource_type:
            mapped_resource_type = f"azure.{resource_type.lower().replace('/', '.')}" if resource_type else None

        return {
            'id': resource_id,
            'name': resource_name or resource_id,
            'type': mapped_resource_type,
            'location': region or 'unknown',
            'tags': tags,
            'original_data': dict(focus_row)
        }

    def _load_scan(
        self,
        resource_info: dict,
        account_id: str,
        tenant_name: str,
        customer_name: str,
    ):
        """Save extracted resource information to database"""
        if not resource_info['type']:
            _LOG.warning(
                f"Resource type not found for resource {resource_info['id']}. Skipping."
            )
            return

        self._rs.create(
            id=resource_info['id'],
            name=resource_info['name'],
            location=resource_info['location'],
            resource_type=resource_info['type'],
            account_id=account_id,
            tenant_name=tenant_name,
            customer_name=customer_name,
            data=resource_info['original_data'],
            sync_date=datetime.now(timezone.utc).timestamp(),
            collector_type=self.collector_type,
        ).save()

    def collect_tenant_resources(
        self,
        tenant_name: str,
        regions: Iterable[str] | None = None,
        resource_types: Iterable[str] | None = None,
        **kwargs
    ):
        """Collect resources for a specific tenant from FOCUS data"""
        tenant = self._ms.tenant_service().get(tenant_name)
        if not tenant:
            raise ValueError(f'Tenant {tenant_name} not found')
        
        if tenant.cloud != Cloud.AZURE:
            _LOG.warning(f'Tenant {tenant_name} is not Azure tenant. Skipping.')
            return

        account_id = tenant.project if tenant.project else ""

        try:
            blob_service_client = self._get_blob_service_client(tenant)
            container_client = blob_service_client.get_container_client(self._container_name)
            
            latest_blob = self._get_latest_focus_blob(container_client)
            blob_client = container_client.get_blob_client(latest_blob.name)
            
            focus_data = self._load_focus_data(blob_client, latest_blob.name)
            
            if regions:
                regions_lower = [r.lower() for r in regions]
                filtered_data = []
                for row in focus_data:
                    row_region = (row.get('Region') or row.get('region') or row.get('AvailabilityZone', '')).lower()
                    if row_region in regions_lower:
                        filtered_data.append(row)
                focus_data = filtered_data

            processed_resources = set()
            resources_saved = 0
            
            for row in focus_data:
                resource_info = self._extract_resource_info(row)
                
                if not resource_info:
                    continue

                if resource_types:
                    if resource_info['type'] not in resource_types:
                        continue

                resource_key = (
                    resource_info['id'],
                    resource_info['type'],
                    resource_info['location']
                )
                
                if resource_key in processed_resources:
                    continue
                
                processed_resources.add(resource_key)
                
                self._load_scan(
                    resource_info=resource_info,
                    account_id=account_id,
                    tenant_name=tenant.name,
                    customer_name=tenant.customer_name,
                )
                resources_saved += 1

            _LOG.info(f"Saved {resources_saved} unique resources from FOCUS data for tenant {tenant_name}")

        except Exception as e:
            _LOG.error(f"Failed to collect FOCUS resources for tenant {tenant_name}: {e}")
            raise

    def collect_all_resources(
        self,
        regions: Iterable[str] | None = None,
        resource_types: Iterable[str] | None = None,
        **kwargs
    ):
        """
        Collect all Azure resources from FOCUS data for all tenants.
        """
        tenants = self._ms.tenant_service().scan_tenants(only_active=True)
        for tenant in tenants:
            if tenant.cloud != Cloud.AZURE:
                continue
            try:
                _LOG.info(f'Collecting FOCUS resources for tenant: {tenant.name}')
                self.collect_tenant_resources(
                    tenant_name=tenant.name,
                    regions=regions,
                    resource_types=resource_types,
                    **kwargs
                )
            except Exception as e:
                _LOG.error(
                    f'Error collecting FOCUS resources for tenant {tenant.name}: {e}'
                )