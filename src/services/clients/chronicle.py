from enum import Enum
import math
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

from google.auth.transport import requests
from google.oauth2 import service_account
import msgspec

from helpers import batches
from helpers.constants import HTTPMethod
from helpers.log_helper import get_logger

_LOG = get_logger(__name__)


class ChronicleEndpoint(str, Enum):
    UDM_EVENTS_CREATE = '/v2/udmevents:batchCreate'
    UNSTRUCTURED_LOG_ENTRIES_CREATE = '/v2/unstructuredlogentries:batchCreate'
    ENTITIES_CREATE = '/v2/entities:batchCreate'
    LOGTYPES_GET = '/v2/logtypes'


class LogType:
    __slots__ = 'log_type', 'description', 'index'

    def __init__(self, log_type: str, description: str, index: int):
        self.log_type = log_type
        self.description = description
        self.index = index

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}(log_type={self.log_type})'


class ChronicleV2Client:
    """
    https://cloud.google.com/chronicle/docs/reference/ingestion-api
    """
    _scopes = ['https://www.googleapis.com/auth/malachite-ingestion']
    _payload_size_limit = 1 << 20

    __slots__ = '_baseurl', '_session', '_customer_id', '_encoder'

    @staticmethod
    def _init_session(credentials: Path, scopes: list[str]
                      ) -> requests.AuthorizedSession:
        credentials = service_account.Credentials.from_service_account_file(
            filename=str(credentials),
            scopes=scopes
        )
        return requests.AuthorizedSession(credentials)

    def __init__(self, url: str, credentials: Path,
                 customer_id: str | None = None):
        """
        :param url: http://127.0.0.1:8080
        :param credentials: path to file with Google credentials
        :param customer_id: Chronicle instance customer_id.
        Will be used by default
        """
        self._baseurl = url
        self._session = self._init_session(credentials, self._scopes)
        self._customer_id = customer_id
        self._encoder = msgspec.json.Encoder()

    def _batches(self, entities: list[dict]
                 ) -> Generator[list[dict], None, None]:
        """
        Chronicle accepts only payloads less or eq that 1mb. This method
        calculates the total length of payload for given list of entities
        splits them to batches less than 1mb. We do this assuming that each
        entity has more or less the same size
        :param entities:
        :return:
        """
        # TODO maybe rewrite because this logic is not ideal
        total = len(self._encoder.encode(entities))  # total bytes
        number = math.ceil(total / self._payload_size_limit) + 2  # number of requests
        yield from batches(entities, max(1, len(entities) // number))

    @staticmethod
    def _load_json(resp) -> dict | list | None:
        try:
            return resp.json()
        except Exception:
            return

    def create_udm_events(self, events: list[dict],
                          customer_id: str | None = None) -> bool:
        cid = customer_id or self._customer_id
        assert cid, 'customer_id must be provided if there is no default'
        success = True
        for i, batch in enumerate(self._batches(events), start=1):
            data = self._encoder.encode({
                'customer_id': cid,
                'events': batch
            })
            _LOG.debug(f'Making the request №{i} with payload size {len(data)}')
            resp = self._request(
                path=ChronicleEndpoint.UDM_EVENTS_CREATE,
                method=HTTPMethod.POST,
                data=data,
                headers={'Content-Type': 'application/json'}
            )
            if resp is None or not resp.ok:
                _LOG.warning(f'Error occurred creating events: '
                             f'{self._load_json(resp.json())}')
                success = False
        return success

    def create_udm_entities(self, entities: list[dict], log_type: str,
                            customer_id: str | None = None) -> bool:
        _LOG.info('Uploading udm entities to chronicle')
        cid = customer_id or self._customer_id
        assert cid, 'customer_id must be provided if there is no default'
        success = True
        for i, batch in enumerate(self._batches(entities), start=1):
            data = self._encoder.encode({
                'customer_id': cid,
                'entities': batch,
                'log_type': log_type
            })
            _LOG.debug(f'Making the request №{i} with payload size {len(data)}')
            resp = self._request(
                path=ChronicleEndpoint.ENTITIES_CREATE,
                method=HTTPMethod.POST,
                data=data,
                headers={'Content-Type': 'application/json'}
            )
            if resp is None or not resp.ok:
                _LOG.warning(f'Error occurred creating entities: '
                             f'{self._load_json(resp.json())}')
                success = False
        return success

    def iter_log_types(self) -> Generator[LogType, None, None]:
        resp = self._request(
            path=ChronicleEndpoint.LOGTYPES_GET,
            method=HTTPMethod.GET
        )
        if not resp.ok:
            _LOG.warning('Error occurred getting log types')
            return
        for dct in (self._load_json(resp) or {}).get('logTypes', []):
            yield LogType(
                log_type=dct['logType'],
                description=dct['description'],
                index=dct['index']
            )

    def _request(self, path: ChronicleEndpoint, method: HTTPMethod,
                 data: bytes | None = None, headers: dict | None = None
                 ):
        _LOG.info(f'Making Chronicle request {method} {path}')
        try:
            resp = self._session.request(
                method=method.value,
                url=urljoin(self._baseurl, path),
                data=data,
                headers=headers
            )
            _LOG.info(f'Response status code: {resp.status_code}')
            return resp
        except Exception:
            _LOG.exception('Error occurred making request to dojo')
