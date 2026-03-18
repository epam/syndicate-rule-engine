"""
SQS connector for consuming messages from AWS SQS.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Callable

import boto3
from botocore.exceptions import ClientError

from helpers.log_helper import get_logger

from onprem.event_sources_consumer import settings

from .base import BaseConnector, Message

if TYPE_CHECKING:
    from onprem.event_sources_consumer.config_loader import EventSourceConfig

_LOG = get_logger(__name__)


class SQSConnector(BaseConnector):
    """SQS queue connector."""

    def __init__(
        self,
        config: EventSourceConfig,
        credentials: dict | None = None,
    ):
        self._config = config
        self._credentials = credentials
        self._client = None

    def connect(self) -> None:
        kwargs: dict[str, Any] = {
            "service_name": "sqs",
            "region_name": self._config.region,
        }
        if self._credentials:
            kwargs["aws_access_key_id"] = self._credentials.get(
                "aws_access_key_id"
            )
            kwargs["aws_secret_access_key"] = self._credentials.get(
                "aws_secret_access_key"
            )
            if self._credentials.get("aws_session_token"):
                kwargs["aws_session_token"] = self._credentials[
                    "aws_session_token"
                ]
        self._client = boto3.client(**kwargs)

    def consume(
        self,
        callback: Callable[[Message], None],
        max_messages: int = settings.queue.batch_size,
        wait_time_seconds: int = settings.queue.wait_seconds,
        visibility_timeout: int = settings.queue.visibility_timeout,
    ) -> None:
        if not self._client:
            raise RuntimeError("SQSConnector not connected")
        response = self._client.receive_message(
            QueueUrl=self._config.queue_url,
            MaxNumberOfMessages=min(max_messages, 10),
            WaitTimeSeconds=wait_time_seconds,
            VisibilityTimeout=visibility_timeout,
        )
        for raw_msg in response.get("Messages", []):
            body = raw_msg.get("Body", "")
            try:
                body_parsed = (
                    json.loads(body) if isinstance(body, str) else body
                )
            except json.JSONDecodeError:
                body_parsed = body
            msg = Message(
                message_id=raw_msg.get("MessageId", ""),
                body=body_parsed,
                receipt_handle=raw_msg.get("ReceiptHandle"),
                raw=raw_msg,
            )
            try:
                callback(msg)
                self.ack(msg)
            except Exception as e:
                _LOG.exception(
                    "Failed to process SQS message %s: %s. Not acking.",
                    msg.message_id,
                    e,
                )

    def ack(self, message: Message) -> None:
        if not self._client or not message.receipt_handle:
            return
        try:
            self._client.delete_message(
                QueueUrl=self._config.queue_url,
                ReceiptHandle=message.receipt_handle,
            )
        except ClientError as e:
            _LOG.warning(
                "Failed to delete SQS message %s: %s",
                message.message_id,
                e,
            )

    def disconnect(self) -> None:
        self._client = None
