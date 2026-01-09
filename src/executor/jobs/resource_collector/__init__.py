"""
Resources Collector package.

This package provides resource collection functionality using Cloud Custodian.
"""

from .base import BaseResourceCollector, ResourceIteratorStrategy
from .collector import CustodianResourceCollector
from .strategies import get_resource_iterator


__all__ = (
    # Base classes
    "BaseResourceCollector",
    "ResourceIteratorStrategy",
    # Collector
    "CustodianResourceCollector",
    # Strategies
    "get_resource_iterator",
)

