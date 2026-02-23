import base64
import dataclasses
import json
import re
from enum import Enum
from http import HTTPStatus

from typing import Literal, Tuple, Union
from typing_extensions import TypedDict, NotRequired
import requests
from modular_sdk.services.impl.maestro_credentials_service import AccessMeta

from helpers import JWTToken, Version, urljoin
from helpers.__version__ import __version__
from helpers.constants import (
    ALG_ATTR,
    AUTHORIZATION_PARAM,
    KID_ATTR,
    TOKEN_ATTR,
    HTTPMethod,
    JobState,
)
from helpers.log_helper import get_logger
from helpers.system_customer import SystemCustomer
from services.clients.ssm import AbstractSSMClient
from services.setting_service import SettingsService

_LOG = get_logger(__name__)


class LMRulesetDTO(TypedDict):
    name: str
    cloud: Literal['AWS', 'AZURE', 'GCP', 'KUBERNETES']
    description: str
    version: str
    creation_date: str
    compliance_name: str | None
    rules: list[str]
    versions: list[str]
    download_url: NotRequired[str]


class LMAllowanceDTO(TypedDict):
    time_range: str
    job_balance: int
    balance_exhaustion_model: str


class LMEventDrivenDTO(TypedDict):
    active: bool
    quota: int


class LMLicenseDTO(TypedDict):
    customers: dict[str, dict]
    description: str
    allowance: LMAllowanceDTO
    event_driven: LMEventDrivenDTO
    valid_from: str
    valid_until: str
    service_type: str
    license_key: str
    rulesets: list[LMRulesetDTO]


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
    LICENSE_METADATA_ALL = '/license/metadata/all'


@dataclasses.dataclass()
class LMAccessData(AccessMeta):
    pass


class LmTokenProducer:
    __slots__ = '_ss', '_ssm', '_kid', '_alg', '_pem', '_cached'

    def __init__(
        self, settings_service: SettingsService, ssm: AbstractSSMClient
    ):
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
        resp = (
            self._ssm.get_secret_value(
                secret_name=self.derive_client_private_key_id(kid)
            )
            or {}
        )
        self._pem = resp['value'].encode()
        return self._pem

    @staticmethod
    def get_ssm_auth_token_name(customer: str):
        customer = re.sub(r'[\s-]', '_', customer.lower())
        return f'caas_lm_auth_token_{customer}'

    def produce(
        self,
        lifetime: int = 120,
        customer: str | None = None,
        cached: bool = True,
    ) -> str | None:
        """
        Lm uses cached token to improve performance
        :param lifetime:
        :param customer:
        :param cached:
        :return:
        """
        customer = customer or SystemCustomer.get_name()
        secret_name = self.get_ssm_auth_token_name(customer)
        if cached:
            v = (self._ssm.get_secret_value(secret_name) or {}).get(TOKEN_ATTR)
            if v and not JWTToken(v).is_expired():
                _LOG.debug(
                    f'Returning cached JWT token for customer: {customer}'
                )
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
            private_pem=pem,
        ).produce()
        if cached:
            self._ssm.create_secret(
                secret_name=secret_name, secret_value={TOKEN_ATTR: token}
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

    def _safe_json(self, resp: requests.Response) -> dict | None:
        """
        Safely parse JSON response. If response is not JSON, returns None.
        Logs warning if parsing fails.
        """
        try:
            return resp.json()
        except (json.JSONDecodeError, ValueError) as e:
            _LOG.warning(
                f'Failed to parse JSON response from {resp.url}. '
                f'Response content type: {resp.headers.get("Content-Type", "unknown")}, '
                f'Status: {resp.status_code}, Error: {e}'
            )
            _LOG.debug(f'Response text: {resp.text[:500]}')
            return None

    def _get_error_message(self, resp: requests.Response, default: str = '') -> str:
        """
        Extract error message from response. Tries JSON first, falls back to text.
        """
        json_data = self._safe_json(resp)
        return json_data.get('message', '') if json_data else resp.text or default

    def _get_items_from_response(self, resp: requests.Response) -> list | None:
        """
        Safely extract 'items' array from JSON response.
        Returns None if response is invalid or items are missing.
        """
        json_data = self._safe_json(resp)
        if not json_data or 'items' not in json_data or not json_data['items']:
            return None
        return json_data['items']

    def _send_request(
        self,
        endpoint: LMEndpoint,
        method: HTTPMethod,
        params: dict | None = None,
        data: dict | None = None,
        token: str | None = None,
    ) -> requests.Response | None:
        _LOG.debug(f"Going to send '{method}' request to '{endpoint}'")
        kw = dict(
            method=method.value, url=urljoin(self._baseurl, endpoint.value)
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
            _LOG.exception(f'Error occurred while executing request: {e}')
            return

    def sync_license(
        self,
        license_key: str,
        customer: str | None = None,
        installation_version: str | None = None,
        include_ruleset_links: bool = True,
    ) -> Tuple[Union[LMLicenseDTO, str], int]:
        data = dict(
            license_key=license_key,
            include_ruleset_links=include_ruleset_links,
        )
        if installation_version:
            data['installation_version'] = installation_version
        resp = self._send_request(
            endpoint=LMEndpoint.LICENSE_SYNC,
            method=HTTPMethod.POST,
            data=data,
            token=self._token_producer.produce(customer=customer),
        )
        if resp is None:
            _LOG.warning('Failed license sync')
            return 'Failed license sync', HTTPStatus.INTERNAL_SERVER_ERROR
        if not resp.ok:
            err = self._get_error_message(resp, 'Failed license sync')
            _LOG.warning(f'Failed license sync: {err}')
            return err, resp.status_code

        items = self._get_items_from_response(resp)
        if not items:
            _LOG.warning('Invalid response format from license sync')
            return 'Invalid response format', resp.status_code
        return items.pop(), resp.status_code

    def check_permission(
        self, customer: str, tenant: str, tenant_license_key: str
    ) -> bool:
        return bool(
            self._send_request(
                endpoint=LMEndpoint.JOBS_CHECK_PERMISSION,
                method=HTTPMethod.POST,
                data={
                    'tenant': tenant,
                    'tenant_license_keys': [tenant_license_key],
                },
                token=self._token_producer.produce(customer=customer),
            )
        )

    def activate_customer(
        self, customer: str, tlk: str
    ) -> tuple[str, str] | None:
        resp = self._send_request(
            endpoint=LMEndpoint.CUSTOMER_SET_ACTIVATION_DATE,
            method=HTTPMethod.POST,
            data={'tenant_license_key': tlk},
            token=self._token_producer.produce(customer=customer),
        )
        if resp is None or not resp.ok:
            _LOG.warning('Cannot activate customer')
            return
        items = self._get_items_from_response(resp)
        if not items:
            _LOG.warning('Invalid response format from activate customer')
            return
        data = items[0]
        return data['license_key'], data['tenant_license_key']

    def post_job(
        self,
        job_id: str,
        customer: str,
        tenant: str,
        ruleset_map: dict[str, list[str]],
        include_ruleset_links: bool = False,
    ) -> dict:
        resp = self._send_request(
            endpoint=LMEndpoint.JOBS,
            method=HTTPMethod.POST,
            data={
                'service_type': 'CUSTODIAN',
                'job_id': job_id,
                'tenant': tenant,
                'rulesets': ruleset_map,
                'installation_version': __version__,
                'include_ruleset_links': include_ruleset_links,
            },
            token=self._token_producer.produce(customer=customer),
        )
        if resp is None:
            raise LMUnavailable('Cannot access the License manager')
        match resp.status_code:
            case HTTPStatus.OK:
                items = self._get_items_from_response(resp)
                if not items:
                    raise LMUnavailable('Invalid response format from post job')
                return items[0]
            case HTTPStatus.FORBIDDEN:
                raise LMEmptyBalance(self._get_error_message(resp))
            case HTTPStatus.NOT_FOUND:
                raise LMInvalidData(self._get_error_message(resp))
            case _:
                raise LMUnavailable(self._get_error_message(resp))

    def update_job(
        self,
        job_id: str,
        customer: str | None,
        created_at: str | None = None,
        started_at: str | None = None,
        stopped_at: str | None = None,
        status: JobState | str | None = None,
    ) -> bool:
        if isinstance(status, JobState):
            status = status.value
        resp = self._send_request(
            endpoint=LMEndpoint.JOBS,
            method=HTTPMethod.PATCH,
            data={
                'job_id': job_id,
                'created_at': created_at,
                'started_at': started_at,
                'stopped_at': stopped_at,
                'status': status,
            },
            token=self._token_producer.produce(customer=customer),
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
            token=self._token_producer.produce(lifetime=15, cached=False),
        )
        if resp is None or not resp.ok:
            return None, None
        json_data = self._safe_json(resp)
        if not json_data:
            _LOG.warning('Invalid response format from whoami')
            return None, None
        return json_data.get('client_id'), resp.headers.get('Accept-Version')

    def get_all_metadata(
        self,
        customer: str,
        tenant_license_key: str,
        installation_version: str | None = None,
    ) -> bytes | None:
        pass


class LMClientAfter2p7(LMClient):
    """
    This class introduces API changes in LM >= 2.7.0
    """

    def check_permission(
        self, customer: str, tenant: str, tenant_license_key: str
    ) -> bool:
        resp = self._send_request(
            endpoint=LMEndpoint.JOBS_CHECK_PERMISSION,
            method=HTTPMethod.POST,
            data={
                'tenants': [tenant],
                'tenant_license_keys': [tenant_license_key],
            },
            token=self._token_producer.produce(customer=customer),
        )
        if resp is None or not resp.ok:
            _LOG.warning('Cannot check permission')
            return False
        items = self._get_items_from_response(resp)
        if not items:
            _LOG.warning('Invalid response format from check permission')
            return False
        item = items[0]
        if tenant_license_key not in item:
            _LOG.warning(f'Tenant license key {tenant_license_key} not found in response')
            return False
        return tenant in item[tenant_license_key].get('allowed', [])


class LMClientAfter3p0(LMClientAfter2p7):
    """
    This class introduces changes in LM >= 3.0.0
    """

    def post_ruleset(
        self,
        name: str,
        version: str,
        cloud: str,
        description: str,
        display_name: str,
        download_url: str,
        rules: list[str],
        overwrite: bool,
    ) -> tuple[HTTPStatus, str] | None:
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
                'rules': rules,
                'overwrite': overwrite,
            },
            token=self._token_producer.produce(lifetime=15, cached=False),
        )
        if resp is None:
            return
        return HTTPStatus(resp.status_code), self._get_error_message(resp)


class LMClientAfter3p3(LMClientAfter3p0):
    """
    This class introduces changes in LM >= 3.3.0
    """

    def get_all_metadata(
        self,
        customer: str,
        tenant_license_key: str,
        installation_version: str | None = None,
    ) -> bytes | None:
        params = {'tenant_license_key': tenant_license_key}
        if installation_version:
            params['installation_version'] = installation_version
        resp = self._send_request(
            endpoint=LMEndpoint.LICENSE_METADATA_ALL,
            method=HTTPMethod.GET,
            params=params,
            token=self._token_producer.produce(customer=customer),
        )
        if resp is None or not resp.ok:
            _LOG.warning('Could not get metadata')
            return
        return base64.b64decode(resp.content)


class LMClientFactory:
    __slots__ = '_ss', '_ssm'

    def __init__(
        self, settings_service: SettingsService, ssm: AbstractSSMClient
    ):
        self._ss = settings_service
        self._ssm = ssm

    def create(self) -> LMClient:
        _LOG.info('Going to build license manager client')
        ad = LMAccessData.from_dict(
            self._ss.get_license_manager_access_data() or {}
        )
        url = ad.url
        _LOG.debug(f'License manager URL: {url}')

        producer = LmTokenProducer(
            settings_service=self._ss,
            ssm=self._ssm,
        )
        cl = LMClientAfter3p3(
            baseurl=url, 
            token_producer=producer,
        )

        _LOG.debug('Making whoami request to get version')
        _, version = cl.whoami()
        _LOG.debug(f'Received api version: {version}')

        if not version:
            _LOG.info(
                'No desired api version supplied. Using client for 3.3.0+'
            )
            return cl
            
        if Version(version) >= Version('3.3.0'):
            _LOG.info(f'Desired version is {version}. Using client for 3.3.0+')
            return cl
        if Version(version) >= Version('3.0.0'):
            _LOG.info(f'Desired version is {version}. Using client for 3.0.0+')
            return LMClientAfter3p0(
                baseurl=url,
                token_producer=producer,
            )
        if Version(version) >= Version('2.7.0'):
            _LOG.info(f'Desired version is {version}. Using client for 2.7.0+')
            return LMClientAfter2p7(
                baseurl=url,
                token_producer=producer,
            )

        _LOG.info(f'Desired version is {version}. Using client for 3.3.0+')
        return cl
