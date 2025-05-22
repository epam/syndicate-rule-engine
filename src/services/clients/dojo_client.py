from datetime import datetime
from typing import Generator

import requests
import msgspec

from helpers import batches_with_critic
from helpers.constants import HTTPMethod, SRE_DOJO_PAYLOAD_SIZE_LIMIT_BYTES
from helpers.log_helper import get_logger

_LOG = get_logger(__name__)


class DojoV2Client:
    __slots__ = ('_url', '_session')

    encoder = msgspec.json.Encoder()

    # There are three limits one in nginx and two in dojo
    # In nginx it set to 800MB (by default it's 1MB) 
    # In dojo first set to 8MB for POST requests
    # In dojo second set to 100MB for file size for scan added via API
    # nginx.conf:
    # 14  client_max_body_size 800m;
    # settings.dist.py:
    # 95  DD_DATA_UPLOAD_MAX_MEMORY_SIZE=(int, 8388608)
    # ...
    # 272 DD_SCAN_FILE_MAX_SIZE=(int, 100)

    # tested limit for POST requests (DD_DATA_UPLOAD_MAX_MEMORY_SIZE) 
    # and couldn't reproduced described error

    # TODO: test with ngingx limit and file size limit (DD_SCAN_FILE_MAX_SIZE)


    _payload_size_limit = SRE_DOJO_PAYLOAD_SIZE_LIMIT_BYTES

    def __init__(self, url: str, api_key: str):
        """
        :param url: http://127.0.0.1:8080/api/v2
        :param api_key:
        """
        url.strip('/')
        if 'api/v2' not in url:
            url = url + '/api/v2'

        self._url = url
        self._session = requests.Session()
        self._session.headers.update({'Authorization': f'Token {api_key}'})

    def __del__(self):
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._session.close()

    def user_profile(self) -> dict | None:
        resp = self._request(path='user_profile', method=HTTPMethod.GET)
        if resp is None or not resp.ok:
            return
        return resp.json()
    
    @staticmethod
    def _load_json(resp) -> dict | list | None:
        try:
            return resp.json()
        except Exception:
            return
    
    def _batches(self, entities: list
                 ) -> Generator[list[dict], None, None]:
        """
        Dojo accepts only payloads of limited size. 
        This function batch data into batches 
        that can't be bigger than specified limit
        :param entities:
        :return:
        """
        yield from batches_with_critic(
            iterable = entities, 
            critic = lambda x: len(self.encoder.encode(x)), 
            limit = self._payload_size_limit,
            drop_violating_items=True
        )

    def import_scan(self, scan_type: str, scan_date: datetime,
                    product_type_name: str,
                    product_name: str, engagement_name: str, test_title: str,
                    data: dict, auto_create_context: bool = True,
                    tags: list[str] | None = None, reimport: bool = True,
                    ) -> requests.Response | None:
        # TODO: it should return bool or list of responses. 
        # Using final_response to not break existing logic with it.
        final_response = None
        _LOG.debug(f'Data bytes: {len(self.encoder.encode(data))}')
        _LOG.debug(f'Data elements: {len(data["findings"])}')
        for batch in self._batches(data['findings']):
            _LOG.debug(f'Batch bytes: {len(self.encoder.encode(batch))}')
            _LOG.debug(f'Batch elements: {len(batch)}')
            resp = self._request(
                path='/reimport-scan/' if reimport else '/import-scan/',
                method=HTTPMethod.POST,
                data={
                    'product_type_name': product_type_name,
                    'product_name': product_name,
                    'engagement_name': engagement_name,
                    'test_title': test_title,
                    'auto_create_context': auto_create_context,
                    'tags': tags or [],
                    'scan_type': scan_type,
                    'scan_date': scan_date.date().isoformat()
                },
                files={
                    'file': ('report.json', self.encoder.encode({'findings': batch}))
                }
            )
            if resp is None or not resp.ok:
                _LOG.warning(f'Error occurred. Resp: {self._load_json(resp)}')
            final_response = resp
        return final_response

    def _request(self, path: str, method: HTTPMethod,
                 params: dict | None = None, data: dict | None = None,
                 files: dict | None = None, timeout: int | None = None
                 ) -> requests.Response | None:
        _LOG.info(f'Making dojo request {method.value} {path}')
        try:
            resp = self._session.request(
                method=method.value,
                url=self._url + path,
                params=params,
                data=data,
                files=files,
                timeout=timeout
            )
            _LOG.info(f'Response status code: {resp.status_code}')
            _LOG.debug(f'Response body: {resp.text}')
            return resp
        except requests.RequestException:
            _LOG.exception('Error occurred making request to dojo')
