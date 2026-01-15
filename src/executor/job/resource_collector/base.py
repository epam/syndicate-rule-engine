from abc import ABC, abstractmethod
from typing import Generator

from helpers.constants import ResourcesCollectorType
from models.resource import Resource
from services.resources_service import ResourcesService
from services.sharding import ShardPart


class BaseResourceCollector(ABC):
    """
    Abstract base class for resource collectors.
    All resource collectors should inherit from this class and implement its methods.
    """

    collector_type: ResourcesCollectorType

    @abstractmethod
    def collect_all_resources(
        self,
        regions: set[str] | None = None,
        resource_types: set[str] | None = None,
    ) -> None: ...


class ResourceIteratorStrategy(ABC):
    """
    Abstract base class for cloud-specific resource iteration strategies.
    Implements the Strategy pattern for converting raw resources to Resource models.
    """

    @abstractmethod
    def iterate(
        self,
        part: ShardPart,
        account_id: str,
        location: str,
        resource_type: str,
        customer_name: str,
        tenant_name: str,
        resources_service: ResourcesService,
        collector_type: ResourcesCollectorType,
    ) -> Generator[Resource, None, None]:
        """Iterate over resources and yield Resource models."""
        ...

