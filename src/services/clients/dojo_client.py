from datetime import datetime

import requests
import msgspec

from helpers.constants import HTTPMethod
from helpers.log_helper import get_logger

_LOG = get_logger(__name__)


class DojoV2Client:
    __slots__ = ('_url', '_session')

    encoder = msgspec.json.Encoder()

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

    def import_scan(self, scan_type: str, scan_date: datetime,
                    product_type_name: str,
                    product_name: str, engagement_name: str, test_title: str,
                    data: dict, auto_create_context: bool = True,
                    tags: list[str] | None = None, reimport: bool = True,
                    ) -> requests.Response | None:
        return self._request(
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
                'file': ('report.json', self.encoder.encode(data))
            }
        )

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
