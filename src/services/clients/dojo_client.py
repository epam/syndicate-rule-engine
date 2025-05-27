from collections import defaultdict
from datetime import datetime
from typing import Generator, TYPE_CHECKING

import msgspec
import requests

from helpers import encode_into
from helpers.constants import CAASEnv, HTTPMethod
from helpers.log_helper import get_logger

if TYPE_CHECKING:
    from services.report_convertors import Findings

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

    @staticmethod
    def _load_json(resp) -> dict | list | None:
        try:
            return resp.json()
        except Exception:
            return

    def _yield_list_report(
        self, data: list[dict], limit: int
    ) -> Generator[bytearray, None, None]:
        def build_buf():
            return bytearray(b'[')

        it = encode_into(
            it=data,
            encode=self.encoder.encode_into,
            limit=limit,
            new=build_buf,
            sep=b',',
        )
        for buf in it:
            buf.append(ord(b']'))
            yield buf

    def _yield_dict_report(
        self, data: 'Findings', limit: int
    ) -> Generator[bytearray, None, None]:
        def build_buf():
            return bytearray(b'{"findings":[')

        # TODO: handle case if one item is bigger than limit
        it = encode_into(
            it=data.get('findings') or [],
            encode=self.encoder.encode_into,
            limit=limit,
            new=build_buf,
            sep=b',',
        )
        for buf in it:
            buf.extend(b']}')
            yield buf

    def _iter_files(
        self, data: 'list | Findings'
    ) -> Generator[tuple[int | None, bytearray], None, None]:
        if isinstance(data, list) and not data:
            yield None, bytearray(b'[]')
            return
        if isinstance(data, dict) and not data.get('findings'):
            yield None, bytearray(b'{"findings":[]}')
            return

        limit = CAASEnv.DOJO_PAYLOAD_SIZE_LIMIT_BYTES.get()
        if limit is None or not limit.isalnum():
            _LOG.info('No dojo limit, encoding the whole report')
            buf = bytearray()
            self.encoder.encode_into(data, buf)
            yield None, buf
            return
        limit = int(limit)
        _LOG.info(f'Dojo limit is {limit}')
        if isinstance(data, list):
            gen = self._yield_list_report
        else:
            gen = self._yield_dict_report
        for i, buf in enumerate(gen(data, limit)):
            yield i, buf

    def import_scan(
        self,
        scan_type: str,
        scan_date: datetime,
        product_type_name: str,
        product_name: str,
        engagement_name: str,
        test_title: str,
        data: 'list | Findings',
        auto_create_context: bool = True,
        tags: list[str] | None = None,
        reimport: bool = True,
    ) -> tuple[dict[str, int], list]:
        result = defaultdict(int)
        failure_codes = []
        for i, buf in self._iter_files(data):
            if i is not None:
                tt = f'{test_title}-{i}'
            else:
                tt = f'{test_title}'

            resp = self._request(
                path='/reimport-scan/' if reimport else '/import-scan/',
                method=HTTPMethod.POST,
                data={
                    'product_type_name': product_type_name,
                    'product_name': product_name,
                    'engagement_name': engagement_name,
                    'test_title': tt,
                    'auto_create_context': auto_create_context,
                    'tags': tags or [],
                    'scan_type': scan_type,
                    'scan_date': scan_date.date().isoformat(),
                },
                files={'file': ('report.json', buf)},
            )

            if resp is None or not resp.ok:
                _LOG.warning(f'Error occurred. Resp: {self._load_json(resp)}')
                result['failure'] += 1
                failure_codes.append(getattr(resp, 'status_code', None))
            else:
                result['success'] += 1

        return result, failure_codes

    def _request(
        self,
        path: str,
        method: HTTPMethod,
        params: dict | None = None,
        data: dict | None = None,
        files: dict | None = None,
        timeout: int | None = None,
    ) -> requests.Response | None:
        _LOG.info(f'Making dojo request {method.value} {path}')
        try:
            resp = self._session.request(
                method=method.value,
                url=self._url + path,
                params=params,
                data=data,
                files=files,
                timeout=timeout,
            )
            _LOG.info(f'Response status code: {resp.status_code}')
            _LOG.debug(f'Response body: {resp.text}')
            return resp
        except requests.RequestException:
            _LOG.exception('Error occurred making request to dojo')
