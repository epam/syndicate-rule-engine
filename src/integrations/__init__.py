from abc import ABC, abstractmethod
from typing import TypedDict, Optional, List

import requests
from botocore.exceptions import ClientError

from helpers import RESPONSE_OK_CODE, RESPONSE_SERVICE_UNAVAILABLE_CODE
from helpers.log_helper import get_logger

_LOG = get_logger(__name__)


class Result(TypedDict):
    job_id: str
    job_type: str
    type: str
    status: int
    message: Optional[str]
    error: Optional[str]


class AbstractAdapter(ABC):
    siem_type = None
    request_error = None

    def __init__(self, **kwargs):
        self._entities = []

    @abstractmethod
    def add_entity(self, **kwargs):
        raise NotImplementedError()

    def upload_all_entities(self) -> List[Result]:
        results = []
        for entity in self._entities:
            try:
                self.upload(**entity)
                results.append(
                    self.result(
                        job_id=entity.get('job_id'),
                        job_type=entity.get('job_type')
                    )
                )
            except (requests.RequestException, ClientError, ValueError) as e:
                _LOG.error(f'{self.request_error} Error: {str(e)}')
                results.append(
                    self.result(
                        job_id=entity.get('job_id'),
                        job_type=entity.get('job_type'), error=str(e)
                    )
                )
        return results

    @abstractmethod
    def upload(self, **kwargs):
        raise NotImplementedError()

    @classmethod
    def result(cls, job_id, job_type, message='Pushed successfully',
               error=None) -> Result:
        result = {
            'job_id': job_id, 'job_type': job_type, 'type': cls.siem_type,
            'status': RESPONSE_OK_CODE, 'message': message
        }
        if error:
            result.pop('message')
            result.update({
                'status': RESPONSE_SERVICE_UNAVAILABLE_CODE,
                'error': error
            })
        return result

    def purge(self):
        self._entities.clear()
