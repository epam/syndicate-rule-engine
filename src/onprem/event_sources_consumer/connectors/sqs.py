"""
SQS connector for consuming messages from AWS SQS.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Callable

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from helpers.log_helper import get_logger
from onprem.event_sources_consumer.constants import EventConsumerEnv

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
            'service_name': 'sqs',
            'region_name': self._config.region,
        }
        if self._credentials:
            _creds = {
                k: v
                for k, v in self._credentials.items()
                if not k.startswith('_')
            }
            kwargs['aws_access_key_id'] = _creds.get('aws_access_key_id')
            kwargs['aws_secret_access_key'] = _creds.get(
                'aws_secret_access_key'
            )
            if _creds.get('aws_session_token'):
                kwargs['aws_session_token'] = _creds['aws_session_token']
        kwargs['config'] = Config(
            connect_timeout=EventConsumerEnv.BOTO_CONNECT_TIMEOUT.as_int(),
            read_timeout=EventConsumerEnv.BOTO_READ_TIMEOUT.as_int(),
        )
        self._client = boto3.client(**kwargs)

    def consume(
        self,
        callback: Callable[[Message], None],
        max_messages: int = EventConsumerEnv.BATCH_SIZE.as_int(),
        wait_time_seconds: int = EventConsumerEnv.WAIT_SECONDS.as_int(),
        visibility_timeout: int = EventConsumerEnv.VISIBILITY_TIMEOUT.as_int(),
    ) -> None:
        if not self._client:
            raise RuntimeError('SQSConnector not connected')
        response = self._client.receive_message(
            QueueUrl=self._config.queue_url,
            MaxNumberOfMessages=min(max_messages, 10),
            WaitTimeSeconds=wait_time_seconds,
            VisibilityTimeout=visibility_timeout,
        )
        for raw_msg in response.get('Messages', []):
            body = raw_msg.get('Body', '')
            try:
                body_parsed = (
                    json.loads(body) if isinstance(body, str) else body
                )
            except json.JSONDecodeError:
                body_parsed = body
            msg = Message(
                message_id=raw_msg.get('MessageId', ''),
                body=body_parsed,
                receipt_handle=raw_msg.get('ReceiptHandle'),
                raw=raw_msg,
            )
            try:
                callback(msg)
                self.ack(msg)
            except Exception as e:
                _LOG.exception(
                    'Failed to process SQS message %s: %s. Not acking.',
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
                'Failed to delete SQS message %s: %s',
                message.message_id,
                e,
            )

    def disconnect(self) -> None:
        self._client = None

    def reconnect(self, credentials: dict | None) -> None:
        """Reconnect with new credentials (e.g. after assume_role refresh)."""
        self._credentials = credentials
        self._client = None
        self.connect()
