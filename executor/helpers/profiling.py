import json
import logging
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import BinaryIO

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

    def __init__(self, buffer: BinaryIO):
        self._buffer = buffer

    def send_entity(self, entity: Entity):
        try:
            self._buffer.write(json.dumps(
                obj=entity.to_dict(),
                separators=(',', ':'),
                default=str
            ).encode())
            self._buffer.write(b'\n')
        except Exception:  # noqa
            _LOG.exception('Failed to write entity to bytes buffer')


class FileEmitter(Emitter):
    """
    Custom emitter which sends segments to a file instead of sending them
    to x-ray daemon. Writes JSON segments split by newline to a file
    """

    def __init__(self, filename: Path):
        self._filename = filename

    def send_entity(self, entity: Entity):
        try:
            with open(self._filename, 'a') as fp:
                fp.write(json.dumps(
                    obj=entity.to_dict(),
                    separators=(',', ':'),
                    default=str
                ) + os.linesep)
        except Exception:  # noqa
            _LOG.exception('Failed to write entity to json')

    @property
    def filename(self) -> Path:
        return self._filename


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
