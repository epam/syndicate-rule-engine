import io

import requests

from helpers import RESPONSE_FORBIDDEN_CODE
from helpers.time_helper import utc_datetime
from helpers.log_helper import get_logger

DOJO_CAAS_SCAN_TYPE = 'Cloud Custodian Scan'

_LOG = get_logger(__name__)

ENGAGEMENT_NAME_TEMPLATE = '{target_start} - {target_end} scan'
ENGAGEMENT_START_END_FORMAT = '%Y-%m-%d'
POST_METHOD = 'POST'
DELETE_METHOD = 'DELETE'
GET_METHOD = 'GET'


class DojoClient:
    def __init__(self, host: str, api_key: str):
        """
        :param host: assuming that host came from Modular AccessMeta
        :param api_key:
        """
        _LOG.info('Init Dojo client')
        self.host = host
        self.api_key = api_key
        self.headers = {'Authorization': "Token " + self.api_key}
        self._check_connection()

    def _check_connection(self):
        _LOG.info('Checking DefectDojo\'s connection')
        self._request(
            method=GET_METHOD,
            url='/user_profile/',
            timeout=4
        )
        _LOG.info('Connected successfully')

    def create_context(self, product_type_name, product_name, engagement_name,
                       scan_date=None):
        """Created necessary entities without actually importing findings"""
        response = self._request(
            method=POST_METHOD,
            url='/import-scan/',
            data={
                "product_type_name": product_type_name,
                "product_name": product_name,
                "engagement_name": engagement_name,
                "scan_type": DOJO_CAAS_SCAN_TYPE,
                "auto_create_context": True,
                "scan_date": scan_date or utc_datetime().date().isoformat()
            },
            files={
                'file': ('report.json', io.BytesIO(b'{"findings": []}'))
            }
        )
        self._request(
            method=DELETE_METHOD,
            url=f'/tests/{response.get("test_id")}'
        )

    def import_scan(self, product_type_name, product_name, engagement_name,
                    buffer: io.BytesIO, scan_type, reimport=False,
                    auto_create_context=True, test_title=None, scan_date=None):
        data = {
            "product_type_name": product_type_name,
            "product_name": product_name,
            "engagement_name": engagement_name,
            "scan_type": scan_type,
            "auto_create_context": auto_create_context,
            "scan_date": scan_date or utc_datetime().date().isoformat()
        }
        if test_title:
            data['test_title'] = test_title
        files = {
            "file": ("report.json", buffer)
        }
        url = '/reimport-scan/' if reimport else 'import-scan/'
        return self._request(
            method=POST_METHOD,
            url=url,
            data=data,
            files=files
        )

    def _request(self, method, url, params=None, data=None, files=None,
                 timeout=None):
        url = self.host + url
        _LOG.info(f"Making a request to DefectDojo's url: {url}, "
                  f"prodiving: params={params}, data={data}, "
                  f"files={files}.")
        try:
            response = requests.request(
                method=method,
                url=url,
                params=params,
                data=data,
                files=files,
                headers=self.headers,
                timeout=timeout
            )
            response.raise_for_status()
            _LOG.info('The request to DefectDojo was successful!')
            return response.json()
        except ValueError:  # JsonDecodeError (either simplejson or json)
            _LOG.warning('The request is successful but without JSON')
            return {}
        except requests.HTTPError as e:
            _LOG.error(e)
            error = f'An error {e.response.status_code} occurred ' \
                    f'while making a request to DefectDojo server.'
            if e.response.status_code == RESPONSE_FORBIDDEN_CODE:
                error = f'{error} Forbidden.'
            raise requests.RequestException(error)
        except requests.exceptions.ConnectionError as e:
            error = f'Could not connect to the DefectDojo ' \
                    f'server {self.host}'
            _LOG.error(f'{error}: {e}')
            raise requests.RequestException(error)
