import dataclasses
import json
from abc import ABC, abstractmethod
from typing import List, Optional

import requests
from modular_sdk.services.impl.maestro_credentials_service import AccessMeta

from helpers.constants import HTTPMethod, \
    LICENSE_KEY_ATTR, STATUS_ATTR, TENANT_LICENSE_KEY_ATTR, \
    AUTHORIZATION_PARAM, CUSTOMER_ATTR, TENANT_LICENSE_KEYS_ATTR, \
    TENANTS_ATTR, TENANT_ATTR
from helpers.log_helper import get_logger
from services.setting_service import SettingsService

SET_CUSTOMER_ACTIVATION_DATE_PATH = '/customers/set-activation-date'
JOB_CHECK_PERMISSION_PATH = '/jobs/check-permission'
SYNC_LICENSE_PATH = '/license/sync'
JOBS_PATH = '/jobs'

JOB_ID = 'job_id'
CREATED_AT_ATTR = 'created_at'
STARTED_AT_ATTR = 'started_at'
STOPPED_AT_ATTR = 'stopped_at'

_LOG = get_logger(__name__)


@dataclasses.dataclass()
class LMAccessData(AccessMeta):
    api_version: Optional[str]


class LicenseManagerClientInterface(ABC):
    def __init__(self, host: str, client_key_data: str):
        self._host = host
        self._client_key_data = client_key_data

    @property
    def host(self):
        return self._host

    @property
    def client_key_data(self):
        return self._client_key_data

    @abstractmethod
    def license_sync(self, license_key: str, auth: str
                     ) -> Optional[requests.Response]:
        """
        Delegated to commence license-synchronization, bound to a list
        of tenant licenses accessible to a client, authorized by a respective
        token.
        :parameter license_key: str
        :parameter auth: Union[Type[None], str]
        :return: Union[Response, Type[None]]
        """

    @abstractmethod
    def job_check_permission(self, customer: str,
                             tenant: str,
                             tenant_license_keys: List[str], auth: str
                             ) -> bool:
        """
        Delegated to check for permission to license Job,
        bound to a tenant within a customer, exhausting balance derived by
        given tenant-license-keys.
        :parameter customer: str
        :parameter tenant: str
        :parameter auth: str, authorization token
        :parameter tenant_license_keys: List[str]
        :return: bool
        """

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

    def activate_customer(self, customer: str, tlk: str, auth: str
                          ) -> Optional[requests.Response]:
        pass

    @classmethod
    def _send_request(cls, url: str, method: str,
                      params: Optional[dict] = None,
                      payload: Optional[dict] = None,
                      headers: Optional[dict] = None
                      ) -> Optional[requests.Response]:
        try:
            _LOG.debug(f'Going to send \'{method}\' request to \'{url}\'')
            response = requests.request(
                url=url, method=method, headers=headers, params=params or {},
                json=payload or {}
            )
            _LOG.debug(f'Response from {url}: {response}')
            return response
        except (requests.RequestException, Exception) as e:
            _LOG.error(f'Error occurred while executing request. Error: {e}')
            return

    @staticmethod
    def retrieve_json(response: requests.Response) -> Optional[dict]:
        try:
            return response.json()
        except json.JSONDecodeError as je:
            _LOG.warning(f'JSON response from \'{response.url}\' not be '
                         f'decoded. An exception has occurred: {je}')


class LicenseManagerClientMain(LicenseManagerClientInterface):
    """
    Main LM client -> the one that is used by default (in case desired
    version is not specified). The one that has base implementations for
    all the necessary endpoints. Other clients represent some api deviations
    """

    def license_sync(self, license_key: str, auth: str
                     ) -> Optional[requests.Response]:
        if not self.host:
            _LOG.warning('CustodianLicenceManager access data has not been'
                         ' provided.')
            return None
        return self._send_request(
            url=self.host + SYNC_LICENSE_PATH,
            method=HTTPMethod.POST,
            payload={LICENSE_KEY_ATTR: license_key},
            headers={AUTHORIZATION_PARAM: auth}
        )

    def job_check_permission(self, customer: str,
                             tenant: str,
                             tenant_license_keys: List[str], auth: str
                             ) -> bool:
        """
        Currently only one tenant_license_key valid for business logic
        :param customer:
        :param tenant:
        :param tenant_license_keys:
        :param auth:
        :return:
        """
        host = self.host
        if not host:
            _LOG.error('CustodianLicenceManager access data has not been'
                       ' provided.')
            return False

        resp = self._send_request(
            url=host.strip('/') + JOB_CHECK_PERMISSION_PATH,
            method=HTTPMethod.POST,
            payload={
                CUSTOMER_ATTR: customer,
                TENANTS_ATTR: [tenant],
                TENANT_LICENSE_KEYS_ATTR: tenant_license_keys
            },
            headers={AUTHORIZATION_PARAM: auth}
        )
        if not resp or not resp.ok:
            return False
        return tenant in \
            resp.json().get('items')[0][tenant_license_keys[0]]['allowed']

    def update_job(self, job_id: str, created_at: str, started_at: str,
                   stopped_at: str, status: str, auth: str):
        host = self.host
        if not host:
            _LOG.error('CustodianLicenceManager access data has not been'
                       ' provided.')
            return None
        return self._send_request(
            url=host.strip('/') + JOBS_PATH,
            method=HTTPMethod.PATCH,
            payload={
                JOB_ID: job_id,
                CREATED_AT_ATTR: created_at,
                STARTED_AT_ATTR: started_at,
                STOPPED_AT_ATTR: stopped_at,
                STATUS_ATTR: status
            },
            headers={AUTHORIZATION_PARAM: auth}
        )

    def activate_customer(self, customer: str, tlk: str, auth: str
                          ) -> Optional[requests.Response]:
        if not self.host:
            _LOG.warning('CustodianLicenceManager access data has not been'
                         ' provided.')
            return None
        return self._send_request(
            url=self.host.strip('/') + SET_CUSTOMER_ACTIVATION_DATE_PATH,
            method=HTTPMethod.POST,
            payload={CUSTOMER_ATTR: customer, TENANT_LICENSE_KEY_ATTR: tlk},
            headers={AUTHORIZATION_PARAM: auth}
        )


class LicenseManagerClientLess2p7(LicenseManagerClientMain):
    """
    License manager client for <2.7.x version where check permissions
    endpoint slightly differ
    """

    def job_check_permission(self, customer: str,
                             tenant: str,
                             tenant_license_keys: List[str], auth: str
                             ) -> bool:
        host = self.host
        if not host:
            _LOG.error('CustodianLicenceManager access data has not been'
                       ' provided.')
            return False

        return bool(self._send_request(
            url=host.strip('/') + JOB_CHECK_PERMISSION_PATH,
            method=HTTPMethod.POST,
            payload={
                CUSTOMER_ATTR: customer,
                TENANT_ATTR: tenant,
                TENANT_LICENSE_KEYS_ATTR: tenant_license_keys
            },
            headers={AUTHORIZATION_PARAM: auth}
        ))


class LicenseManagerClientFactory:
    def __init__(self, settings_service: SettingsService):
        self._settings_service = settings_service

    def create(self) -> LicenseManagerClientInterface:
        _LOG.info('Going to build license manager client')
        ad = LMAccessData.from_dict(
            self._settings_service.get_license_manager_access_data() or {}
        )
        ck = self._settings_service.get_license_manager_client_key_data() or {}

        # todo implement a more smart dispatch if necessary
        if not ad.api_version:
            _LOG.info('No desired api version supplied. Using default')
            return LicenseManagerClientMain(host=ad.url, client_key_data=ck)
        if ad.api_version >= '2.7.0':
            _LOG.info(f'Desired version is {ad.api_version}. '
                      f'Using client for 2.7.0+')
            return LicenseManagerClientMain(host=ad.url, client_key_data=ck)
        # < 2.7.0
        _LOG.info(f'Desired version is {ad.api_version}. '
                  f'Using client for <2.7.0')
        return LicenseManagerClientLess2p7(host=ad.url, client_key_data=ck)
