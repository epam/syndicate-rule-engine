import logging
from typing import BinaryIO

import msgspec
from aws_xray_sdk.core import patch, xray_recorder
from aws_xray_sdk.core.emitters.udp_emitter import UDPEmitter
from aws_xray_sdk.core.models.entity import Entity
from aws_xray_sdk.core.sampling.local.sampler import LocalSampler

from helpers.log_helper import get_logger

_LOG = get_logger(__name__)

# patch(('requests', 'botocore'))


class BytesEmitter(UDPEmitter):
    """
    Writes JSONs in bytes line by line to the given buffer
    """

    enc = msgspec.json.Encoder(enc_hook=str)

    def __init__(self, buffer: BinaryIO):
        self._buffer = buffer

    def send_entity(self, entity: Entity):
        try:
            self._buffer.write(self.enc.encode(entity.to_dict()))
            self._buffer.write(b'\n')
        except Exception:  # noqa
            _LOG.exception('Failed to write entity to bytes buffer')

    def set_daemon_address(self, address: str) -> None:
        pass

    @property
    def ip(self):
        pass

    @property
    def port(self):
        pass


SAMPLING_RULES = {
    'version': 2,
    'default': {
        'fixed_target': 0,
        'rate': 0.5,  # 50% of jobs are sampled
    },
    'rules': [],
}

logging.getLogger('aws_xray_sdk').setLevel(logging.ERROR)
xray_recorder.configure(
    context_missing='IGNORE_ERROR',
    sampling=True,
    sampler=LocalSampler(SAMPLING_RULES),
    service='syndicate-rule-engine-executor',
    streaming_threshold=1000,
)
