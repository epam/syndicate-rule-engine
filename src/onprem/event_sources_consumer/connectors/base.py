"""
Base connector interface for message queues.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class Message:
    """A single message from a queue."""

    message_id: str
    body: str | bytes | dict
    receipt_handle: str | None = None  # for SQS ack
    raw: Any = None


class BaseConnector(ABC):
    """Base class for queue connectors."""

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the queue."""
        ...

    @abstractmethod
    def consume(
        self,
        callback: Callable[[Message], None],
        max_messages: int = 10,
    ) -> None:
        """
        Consume messages and invoke callback for each.
        Callback receives (message: Message) and returns None on success.
        Raise to signal failure (nack).
        """
        ...

    @abstractmethod
    def ack(self, message: Message) -> None:
        """Acknowledge successful processing of a message."""
        ...

    @abstractmethod
    def disconnect(self) -> None:
        """Close the connection."""
        ...
