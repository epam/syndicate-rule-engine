from pynamodb.pagination import ResultIterator

from helpers.log_helper import get_logger
from models.resource import Resource
from services.base_data_service import BaseDataService

_LOG = get_logger(__name__)


class ResourcesService(BaseDataService[Resource]):

    def create(
        self,
        id: str,
        name: str,
        location: str,
        resource_type: str,
        tenant_name: str,
        customer_name: str,
        data: dict,
        sync_date: str,
    ):
        return Resource(
            id=id,
            name=name,
            location=location,
            resource_type=resource_type,
            tenant_name=tenant_name,
            customer_name=customer_name,
            _data=data,
            sync_date=sync_date,
        )
    
    def update(
        self,
        resource: Resource,
        data: dict | None = None,
        sync_date: str | None = None,
    ):
        actions = False
        if data:
            resource.data = data
            actions = True
        if sync_date:
            resource.sync_date = sync_date
            actions = True
        
        # NOTE: using save because update does not change hash
        # and we need to update it after data change
        if actions:
            resource.save()

    def get_resource(
        self,
        id: str,
        name: str,
        location: str,
        resource_type: str,
        tenant_name: str,
        customer_name: str,
    ) -> Resource | None:
        filter_condition = (Resource.id == id) \
            & (Resource.name == name) \
            & (Resource.location == location) \
            & (Resource.resource_type == resource_type) \
            & (Resource.tenant_name == tenant_name) \
            & (Resource.customer_name == customer_name)
        
        # NOTE: it's not actual scan, we use compound index in mongo 
        # that is not supported in modular SDK
        res = list(Resource.scan(filter_condition))
        return res[0] if res else None
    
    def load_new_scan(
        self,
        id: str,
        name: str,
        location: str,
        resource_type: str,
        tenant_name: str,
        customer_name: str,
        data: dict,
        sync_date: str
    ) -> Resource:
        """
        Loads a new scan or updates existing one.
        """
        resource = self.get_resource(
            id=id,
            name=name,
            location=location,
            resource_type=resource_type,
            tenant_name=tenant_name,
            customer_name=customer_name
        )
        
        if not resource:
            resource = self.create(
                id=id,
                name=name,
                location=location,
                resource_type=resource_type,
                tenant_name=tenant_name,
                customer_name=customer_name,
                data=data,
                sync_date=sync_date
            )
            resource.save()
            return resource
        
        self.update(resource, data=data, sync_date=sync_date)
        return resource
    
    def get_resources(
        self,
        id: str | None = None,
        name: str | None = None,
        location: str | None = None,
        resource_type: str | None = None,
        tenant_name: str | None = None,
        customer_name: str | None = None,
        limit: int = 50,
        last_evaluated_key: dict | None = None,
    ) -> ResultIterator[Resource]:
        """
        Retrieves a list of resources based on the provided filters with pagination support.
        """

        filter_condition = None
        if id:
            filter_condition &= (Resource.id == id)
        if name:
            filter_condition &= (Resource.name == name)
        if location:
            filter_condition &= (Resource.location == location)
        if resource_type:
            filter_condition &= (Resource.resource_type == resource_type)
        if tenant_name:
            filter_condition &= (Resource.tenant_name == tenant_name)
        if customer_name:
            filter_condition &= (Resource.customer_name == customer_name)

        
        kwargs = {
            'limit': limit
        }
        if last_evaluated_key:
            kwargs['last_evaluated_key'] = last_evaluated_key

        # NOTE: it's not actual scan, we use compound index in mongo
        # that is not supported in modular SDK
        if filter_condition is not None:
            return Resource.scan(filter_condition, **kwargs)
        else:
            return Resource.scan(**kwargs)
