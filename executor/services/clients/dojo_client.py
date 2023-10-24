import io

import requests

from helpers.log_helper import get_logger

# DOJO_CAAS_SCAN_TYPE = 'CaaS Scan'
DOJO_CAAS_SCAN_TYPE = 'Cloud Custodian Scan'

_LOG = get_logger(__name__)

ENGAGEMENT_NAME_TEMPLATE = '{target_start} - {target_end} scan'
ENGAGEMENT_START_END_FORMAT = '%Y-%m-%d'
POST_METHOD = 'POST'
DELETE_METHOD = 'DELETE'
GET_METHOD = 'GET'

RESPONSE_FORBIDDEN_CODE = 403


class DojoClient:
    def __init__(self, host: str, api_key: str):
        """
        :param host: assuming that host came from modular AccessMeta
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

    def list_products(self, name=None):
        query = {}
        if name:
            query['name'] = name
        return self._request(
            method=GET_METHOD,
            url='/products/',
            params=query
        )

    def get_product_by_name(self, name):
        products = self.list_products(name=name)
        products = products.get('results', [])

        for product in products:
            if product.get('name') == name:
                return product

    def list_engagements(self, product_id=None, name=None, target_start=None,
                         target_end=None):
        query = {}
        if product_id:
            query['product'] = product_id
        if name:
            query['name'] = name
        if target_start:
            query['target_start'] = target_start
        if target_end:
            query['target_end'] = target_end
        return self._request(
            method=GET_METHOD,
            url='/engagements/',
            params=query
        )

    def get_engagement(self, name, product_id=None):
        if product_id:
            engagements = self.list_engagements(
                name=name,
                product_id=product_id
            )
        else:
            engagements = self.list_engagements(name=name)
        engagements = engagements.get('results', [])
        if len(engagements) > 0:
            return engagements[0]

    def import_scan(self, product_type_name, product_name, engagement_name,
                    buffer: io.BytesIO, reimport=False, test_title=None):
        data = {
            "product_type_name": product_type_name,
            "product_name": product_name,
            "engagement_name": engagement_name,
            "scan_type": DOJO_CAAS_SCAN_TYPE,
            "auto_create_context": True,
        }
        if test_title:
            data['test_title'] = test_title
        files = {
            "file": ("report.json", buffer)
        }
        url = '/reimport-scan/' if reimport else '/import-scan/'
        return self._request(
            method=POST_METHOD,
            url=url,
            data=data,
            files=files
        )

    def _request(self, method, url, params=None, data=None, files=None,
                 timeout=None):
        url = self.host + url
        _LOG.info(f"Making a request to DefectDojo's url: {url}")
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
            _LOG.warning(f'The request is successful but without JSON')
            return {}
        except requests.HTTPError as e:
            _LOG.error(e)
            error = f'An error {e.response.status_code} occurred ' \
                    f'while making a request to DefectDojo server.'
            if e.response.status_code == RESPONSE_FORBIDDEN_CODE:
                error = f'{error} Forbidden - invalid token'
            raise requests.RequestException(error)
        except requests.exceptions.ConnectionError as e:
            error = f'Could not connect to the DefectDojo ' \
                    f'server {self.host}'
            _LOG.error(f'{error}: {e}')
            raise requests.RequestException(error)
