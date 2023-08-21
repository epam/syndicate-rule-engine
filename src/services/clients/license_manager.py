from json import JSONDecodeError
from typing import Optional
from typing import Union, List, Type, Dict

from requests import request, Response
from requests.exceptions import RequestException
from modular_sdk.services.impl.maestro_credentials_service import AccessMeta
from functools import cached_property

from helpers.constants import POST_METHOD, PATCH_METHOD, \
    LICENSE_KEY_ATTR, STATUS_ATTR, TENANT_ATTR, TENANT_LICENSE_KEY_ATTR,\
    AUTHORIZATION_PARAM, CUSTOMER_ATTR, TENANT_LICENSE_KEYS_ATTR
from helpers.log_helper import get_logger
from services.setting_service import SettingsService

SET_TENANT_ACTIVATION_DATE_PATH = '/tenants/set-activation-date'
SET_CUSTOMER_ACTIVATION_DATE_PATH = '/customers/set-activation-date'
JOB_CHECK_PERMISSION_PATH = '/jobs/check-permission'
SYNC_LICENSE_PATH = '/license/sync'
JOBS_PATH = '/jobs'

HOST_KEY = 'host'

JOB_ID = 'job_id'
CREATED_AT_ATTR = 'created_at'
STARTED_AT_ATTR = 'started_at'
STOPPED_AT_ATTR = 'stopped_at'

_LOG = get_logger(__name__)


class LicenseManagerClient:

    def __init__(self, setting_service: SettingsService):
        self.setting_service = setting_service
        self._access_data = None
        self._client_key_data = None

    @cached_property
    def access_data(self) -> dict:
        return self.setting_service.get_license_manager_access_data() or {}

    @property
    def host(self) -> str:
        return AccessMeta.from_dict(self.access_data).url

    @property
    def client_key_data(self):
        if not self._client_key_data:
            self._client_key_data = \
                self.setting_service.get_license_manager_client_key_data()
            self._client_key_data = self._client_key_data or {}
        return self._client_key_data

    def license_sync(self, license_key: str, auth: str):
        """
        Delegated to commence license-synchronization, bound to a list
        of tenant licenses accessible to a client, authorized by a respective
        token.
        :parameter license_key: str
        :parameter auth: Union[Type[None], str]
        :return: Union[Response, Type[None]]
        """
        if not self.host:
            _LOG.warning('CustodianLicenceManager access data has not been'
                         ' provided.')
            return None
        url = self.host + SYNC_LICENSE_PATH
        payload = {
            LICENSE_KEY_ATTR: license_key
        }
        headers = {
            AUTHORIZATION_PARAM: auth
        }
        return self._send_request(
            url=url, method=POST_METHOD, payload=payload, headers=headers
        )

    def job_check_permission(
        self, customer: str, tenant: str,
        tenant_license_keys: List[str], auth: str
    ):
        """
        Delegated to check for permission to license Job,
        bound to a tenant within a customer, exhausting balance derived by
        given tenant-license-keys.
        :parameter customer: str
        :parameter tenant: str
        :parameter auth: str, authorization token
        :parameter tenant_license_keys: List[str]
        :return: Union[Response, Type[None]]
        """
        host, method = self.host, POST_METHOD
        if not host:
            _LOG.error('CustodianLicenceManager access data has not been'
                       ' provided.')
            return None

        host = host.strip('/')
        url = host + JOB_CHECK_PERMISSION_PATH

        payload = {
            CUSTOMER_ATTR: customer,
            TENANT_ATTR: tenant,
            TENANT_LICENSE_KEYS_ATTR: tenant_license_keys
        }

        headers = {
            AUTHORIZATION_PARAM: auth
        }

        return self._send_request(
            url=url, method=method, payload=payload, headers=headers
        )

    def update_job(self, job_id: str, created_at: str, started_at: str,
                   stopped_at: str, status: str, auth: str):
        """
        Delegated to update an id-derivable licensed Job entity, providing
        necessary state data.
        :parameter job_id: str
        :parameter created_at: str
        :parameter started_at: str
        :parameter stopped_at: str
        :parameter status: str
        :parameter auth: str, authorization token
        :return: Union[Response, Type[None]]
        """
        host, method = self.host, PATCH_METHOD
        if not host:
            _LOG.error('CustodianLicenceManager access data has not been'
                       ' provided.')
            return None
        url = host.strip('/') + JOBS_PATH
        payload = {
            JOB_ID: job_id,
            CREATED_AT_ATTR: created_at,
            STARTED_AT_ATTR: started_at,
            STOPPED_AT_ATTR: stopped_at,
            STATUS_ATTR: status
        }
        headers = {
            AUTHORIZATION_PARAM: auth
        }
        return self._send_request(
            url=url, method=method, payload=payload, headers=headers
        )

    def activate_tenant(
        self, tenant: str, tlk: str, auth: str
    ) -> Optional[Response]:
        if not self.host:
            _LOG.warning('CustodianLicenceManager access data has not been'
                         ' provided.')
            return None
        url = self.host + SET_TENANT_ACTIVATION_DATE_PATH
        payload = {TENANT_ATTR: tenant, TENANT_LICENSE_KEY_ATTR: tlk}
        headers = {
            AUTHORIZATION_PARAM: auth
        }
        return self._send_request(
            url=url, method=POST_METHOD, payload=payload, headers=headers
        )

    def activate_customer(self, customer: str, tlk: str, auth: str
                          ) -> Optional[Response]:
        if not self.host:
            _LOG.warning('CustodianLicenceManager access data has not been'
                         ' provided.')
            return None
        url = self.host + SET_CUSTOMER_ACTIVATION_DATE_PATH
        payload = {CUSTOMER_ATTR: customer, TENANT_LICENSE_KEY_ATTR: tlk}
        headers = {
            AUTHORIZATION_PARAM: auth
        }
        return self._send_request(
            url=url, method=POST_METHOD, payload=payload, headers=headers
        )

    @classmethod
    def _send_request(
        cls, url: str, method: str, payload: dict,
        headers: Optional[dict] = None
    ) -> Optional[Response]:
        """
        Meant to commence a request to a given url, by deriving a
        proper delegated handler. Apart from that, catches any risen
        request related exception.
        :parameter url: str
        :parameter method:str
        :parameter payload: dict
        :return: Union[Response, Type[None]]
        """
        _injectable_payload = cls._request_payload_injector(method, payload)
        try:
            _input = f'data - {_injectable_payload}'
            if headers:
                _input += f', headers: {headers}'

            _LOG.debug(f'Going to send \'{method}\' request to \'{url}\''
                       f' with the following {_input}.')

            response = request(
                url=url, method=method, headers=headers, **_injectable_payload
            )
            _LOG.debug(f'Response from {url}: {response}')
            return response
        except (RequestException, Exception) as e:
            _LOG.error(f'Error occurred while executing request. Error: {e}')
            return

    @classmethod
    def _request_payload_injector(cls, method: str, payload: dict):
        _map = cls._define_method_injection_map(payload)
        return _map.get(method, payload) if method in _map else None

    @staticmethod
    def retrieve_json(response: Response) -> Union[Dict, Type[None]]:
        _json = None
        try:
            _json = response.json()
            _LOG.debug(f'JSON data has been decoded: {_json}.')
        except JSONDecodeError as je:
            _LOG.warning(f'JSON response from \'{response.url}\' not be '
                         f'decoded. An exception has occurred: {je}')
        return _json

    @staticmethod
    def _define_method_injection_map(payload):
        json_payload = dict(json=payload)
        return {POST_METHOD: json_payload, PATCH_METHOD: json_payload}
