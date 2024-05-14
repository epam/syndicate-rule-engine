import logging
from abc import ABC, abstractmethod
from typing import BinaryIO

import msgspec
from aws_xray_sdk.core import xray_recorder, patch
from aws_xray_sdk.core.models.entity import Entity
from aws_xray_sdk.core.sampling.local.sampler import LocalSampler

from helpers.log_helper import get_logger

_LOG = get_logger(__name__)

patch(('requests', 'pymongo', 'botocore'))


class Emitter(ABC):
    @abstractmethod
    def send_entity(self, entity: Entity):
        """
        Must write this entity data somewhere. Entity is a segment
        or subsegment
        :param entity:
        :return:
        """


class BytesEmitter(Emitter):
    """
    Writes JSONs in bytes line by line to the given buffer
    """
    __slots__ = '_buffer', '_encoder'

    def __init__(self, buffer: BinaryIO):
        self._buffer = buffer
        self._encoder = msgspec.json.Encoder(enc_hook=str)

    def send_entity(self, entity: Entity):
        try:
            self._buffer.write(self._encoder.encode(entity.to_dict()))
            self._buffer.write(b'\n')
        except Exception:  # noqa
            _LOG.exception('Failed to write entity to bytes buffer')


SAMPLING_RULES = {
    "version": 2,
    "default": {
        "fixed_target": 0,
        "rate": .5  # 50% of jobs are sampled
    },
    "rules": [
    ]
}

logging.getLogger('aws_xray_sdk').setLevel(logging.ERROR)
xray_recorder.configure(
    context_missing='IGNORE_ERROR',
    sampling=True,
    sampler=LocalSampler(SAMPLING_RULES),
    service='custodian-executor',
    streaming_threshold=1000
)
