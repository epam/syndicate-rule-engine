import dataclasses
from enum import Enum
from http import HTTPStatus
import re

from modular_sdk.services.impl.maestro_credentials_service import AccessMeta
import requests

from helpers import JWTToken, Version, urljoin
from helpers.__version__ import __version__
from helpers.constants import (
    ALG_ATTR,
    AUTHORIZATION_PARAM,
    CUSTOMER_ATTR,
    HTTPMethod,
    JobState,
    KID_ATTR,
    TENANTS_ATTR,
    TENANT_ATTR,
    TENANT_LICENSE_KEYS_ATTR,
    TENANT_LICENSE_KEY_ATTR,
    TOKEN_ATTR,
    CAASEnv
)
from helpers.log_helper import get_logger
from helpers.system_customer import SYSTEM_CUSTOMER
from services.clients.ssm import AbstractSSMClient
from services.setting_service import SettingsService


_LOG = get_logger(__name__)


class LMException(Exception):
    pass


class LMEmptyBalance(LMException):
    pass


class LMInvalidData(LMException):
    pass


class LMUnavailable(LMException):
    pass


class LMEndpoint(str, Enum):
    LICENSE_SYNC = '/license/sync'
    RULESET_REGISTRY = '/registry/ruleset'
    CUSTOMER_SET_ACTIVATION_DATE = '/customers/set-activation-date'
    JOBS = '/jobs'
    JOBS_CHECK_PERMISSION = '/jobs/check-permission'
    WHOAMI = '/whoami'


@dataclasses.dataclass()
class LMAccessData(AccessMeta):
    pass


class LmTokenProducer:
    __slots__ = '_ss', '_ssm', '_kid', '_alg', '_pem', '_cached'

    def __init__(self, settings_service: SettingsService,
                 ssm: AbstractSSMClient):
        self._ss = settings_service
        self._ssm = ssm

        self._kid = None
        self._alg = None  # not used due to overdesign
        self._pem = None

    @staticmethod
    def derive_client_private_key_id(kid: str) -> str:
        return f'cs_lm_client_{kid}_prk'

    def get_kid(self) -> str | None:
        if self._kid:
            return self._kid
        _LOG.debug('Getting LM client key id from settings')
        ck = self._ss.get_license_manager_client_key_data() or {}
        self._kid = ck.get(KID_ATTR)
        self._alg = ck.get(ALG_ATTR)
        return self._kid

    def get_pem(self) -> bytes | None:
        if self._pem:
            return self._pem
        _LOG.debug('Getting LM private key from SSM')
        kid = self.get_kid()
        if not kid:
            return
        resp = self._ssm.get_secret_value(
            secret_name=self.derive_client_private_key_id(kid)
        ) or {}
        self._pem = resp['value'].encode()
        return self._pem

    @staticmethod
    def get_ssm_auth_token_name(customer: str = SYSTEM_CUSTOMER):
        customer = re.sub(r'[\s-]', '_', customer.lower())
        return f'caas_lm_auth_token_{customer}'

    def produce(self, lifetime: int = 120, customer: str | None = None,
                cached: bool = True
                ) -> str | None:
        """
        Lm uses cached token to improve performance
        :param lifetime:
        :param customer:
        :param cached:
        :return:
        """
        customer = customer or SYSTEM_CUSTOMER
        secret_name = self.get_ssm_auth_token_name(customer)
        if cached:
            v = (self._ssm.get_secret_value(secret_name) or {}).get(TOKEN_ATTR)
            if v and not JWTToken(v).is_expired():
                _LOG.debug(f'Returning cached JWT token for customer: {customer}')
                return v

        pem = self.get_pem()
        if not pem:
            return
        # not to bring cryptography to global
        from services.license_manager_token import LicenseManagerToken
        token = LicenseManagerToken(
            customer=customer,
            lifetime=lifetime,
            kid=self.get_kid(),
            private_pem=pem
        ).produce()
        if cached:
            self._ssm.create_secret(
                secret_name=secret_name,
                secret_value={TOKEN_ATTR: token}
            )
        return token


class LMClient:
    """
    This is the base LM client with all endpoints that were available from
    the beginning (except whoami)
    """
    __slots__ = '_baseurl', '_token_producer', '_session'

    def __init__(self, baseurl: str, token_producer: LmTokenProducer):
        self._baseurl = baseurl
        self._token_producer = token_producer

        self._session = requests.Session()

    def __del__(self):
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._session.close()

    def _send_request(self, endpoint: LMEndpoint, method: HTTPMethod,
                      params: dict | None = None,
                      data: dict | None = None,
                      token: str | None = None,
                      ) -> requests.Response | None:
        _LOG.debug(f'Going to send \'{method}\' request to \'{endpoint}\'')
        kw = dict(
            method=method.value,
            url=urljoin(self._baseurl, endpoint.value)
        )
        if params:
            kw.update(params=params)
        if data:
            kw.update(json=data)
        if token:
            kw.update(headers={AUTHORIZATION_PARAM: token})
        try:
            resp = self._session.request(**kw)
            _LOG.debug(f'Response from {resp}')
            return resp
        except (requests.RequestException, Exception) as e:
            _LOG.exception('Error occurred while executing request.')
            return

    def sync_license(self, license_key: str, customer: str | None = None
                     ) -> dict | None:
        resp = self._send_request(
            endpoint=LMEndpoint.LICENSE_SYNC,
            method=HTTPMethod.POST,
            data={'license_key': license_key},
            token=self._token_producer.produce(customer=customer)
        )
        if resp is None or not resp.ok:
            _LOG.warning('Failed license sync')
            return
        return resp.json()['items'].pop()

    def check_permission(self, customer: str, tenant: str,
                         tenant_license_key: str) -> bool:
        return bool(self._send_request(
            endpoint=LMEndpoint.JOBS_CHECK_PERMISSION,
            method=HTTPMethod.POST,
            data={
                CUSTOMER_ATTR: customer,
                TENANT_ATTR: tenant,
                TENANT_LICENSE_KEYS_ATTR: [tenant_license_key]
            },
            token=self._token_producer.produce(customer=customer)
        ))

    def activate_customer(self, customer: str, tlk: str
                          ) -> tuple[str, str] | None:
        resp = self._send_request(
            endpoint=LMEndpoint.CUSTOMER_SET_ACTIVATION_DATE,
            method=HTTPMethod.POST,
            data={
                CUSTOMER_ATTR: customer,
                TENANT_LICENSE_KEY_ATTR: tlk
            },
            token=self._token_producer.produce(customer=customer)
        )
        if resp is None or not resp.ok:
            _LOG.warning('Cannot activate customer')
            return
        data = resp.json()['items'][0]
        return data['license_key'], data['tenant_license_key']

    def post_job(self, job_id: str, customer: str, tenant: str,
                 ruleset_map: dict[str, list[str]]
                 ) -> dict:
        resp = self._send_request(
            endpoint=LMEndpoint.JOBS,
            method=HTTPMethod.POST,
            data={
                'service_type': 'CUSTODIAN',
                'job_id': job_id,
                'customer': customer,
                'tenant': tenant,
                'rulesets': ruleset_map,
                'installation_version': __version__
            },
            token=self._token_producer.produce(customer=customer)
        )
        if resp is None:
            raise LMUnavailable('Cannot access the License manager')
        match resp.status_code:
            case HTTPStatus.OK:
                return resp.json()['items'][0]
            case HTTPStatus.FORBIDDEN:
                raise LMEmptyBalance(resp.json().get('message') or '')
            case HTTPStatus.NOT_FOUND:
                raise LMInvalidData(resp.json().get('message') or '')
            case _:
                raise LMUnavailable(resp.json().get('message') or '')

    def update_job(self, job_id: str, customer: str | None,
                   created_at: str | None = None,
                   started_at: str | None = None,
                   stopped_at: str | None = None,
                   status: JobState | None = None) -> bool:
        resp = self._send_request(
            endpoint=LMEndpoint.JOBS,
            method=HTTPMethod.PATCH,
            data={
                'job_id': job_id,
                'created_at': created_at,
                'started_at': started_at,
                'stopped_at': stopped_at,
                'status': status
            },
            token=self._token_producer.produce(customer=customer)
        )
        if resp is None or not resp.ok:
            _LOG.info('Failed to update job in lm')
            return False
        return True

    def whoami(self) -> tuple[str | None, str | None]:
        """
        Starting from 2.15.0 LM has /whoami endpoint that allows to retrieve
        client_id and api version that can be used for dispatching
        :return: (client_id, api_version)
        """
        resp = self._send_request(
            endpoint=LMEndpoint.WHOAMI,
            method=HTTPMethod.GET,
            token=self._token_producer.produce(lifetime=15, cached=False)
        )
        if resp is None or not resp.ok:
            return None, None
        return resp.json().get('client_id'), resp.headers.get('Accept-Version')


class LMClientAfter2p7(LMClient):
    """
    This class introduces API changes in LM >= 2.7.0
    """
    def check_permission(self, customer: str, tenant: str,
                         tenant_license_key: str) -> bool:
        resp = self._send_request(
            endpoint=LMEndpoint.JOBS_CHECK_PERMISSION,
            method=HTTPMethod.POST,
            data={
                CUSTOMER_ATTR: customer,
                TENANTS_ATTR: [tenant],
                TENANT_LICENSE_KEYS_ATTR: [tenant_license_key]
            },
            token=self._token_producer.produce(customer=customer)
        )
        if resp is None or not resp.ok:
            _LOG.warning('Cannot check permission')
            return False
        return tenant in resp.json().get('items')[0][tenant_license_key]['allowed']


class LMClientAfter3p0(LMClientAfter2p7):
    """
    This class introduces changes in LM >= 3.0.0
    """
    def post_ruleset(self, name: str, version: str, cloud: str,
                     description: str, display_name: str, download_url: str,
                     rules: list[str]) -> tuple[HTTPStatus, str] | None:
        """
        Returns boolean whether the ruleset was released. If None is returned
        the request is unsuccessful
        """
        resp = self._send_request(
            endpoint=LMEndpoint.RULESET_REGISTRY,
            method=HTTPMethod.POST,
            data={
                'name': name,
                'version': version,
                'cloud': cloud.upper(),
                'description': description,
                'display_name': display_name,
                'download_url': download_url,
                'rules': rules
            },
            token=self._token_producer.produce(lifetime=15, cached=False)
        )
        if resp is None:
            return
        return HTTPStatus(resp.status_code), resp.json().get('message', '')


class LMClientFactory:
    __slots__ = '_ss', '_ssm'

    def __init__(self, settings_service: SettingsService,
                 ssm: AbstractSSMClient):
        self._ss = settings_service
        self._ssm = ssm

    def create(self) -> LMClient:
        _LOG.info('Going to build license manager client')
        ad = LMAccessData.from_dict(
            self._ss.get_license_manager_access_data() or {}
        )
        producer = LmTokenProducer(self._ss, self._ssm)

        cl = LMClient(baseurl=ad.url, token_producer=producer)
        _LOG.debug('Making whoami request to get version')
        _, version = cl.whoami()
        _LOG.debug(f'Received api version: {version}')

        if not version:
            _LOG.info('No desired api version supplied. '
                      'Using afrer 2.7.0 client')
            return LMClientAfter2p7(baseurl=ad.url, token_producer=producer)
        if Version(version) >= Version('3.0.0'):
            _LOG.info(f'Desired version is {version}. '
                      f'Using client for 3.0.0+')
            return LMClientAfter3p0(baseurl=ad.url, token_producer=producer)
        if Version(version) >= Version('2.7.0'):
            _LOG.info(f'Desired version is {version}. '
                      f'Using client for 2.7.0+')
            return LMClientAfter2p7(baseurl=ad.url, token_producer=producer)
        # < 2.7.0
        _LOG.info(f'Desired version is {version}. '
                  f'Using client for <2.7.0')
        return cl
