import re
import time
from datetime import datetime
from functools import cached_property
from http import HTTPStatus

from requests.exceptions import RequestException, ConnectionError, Timeout

from helpers.constants import KID_ATTR, ALG_ATTR, TOKEN_ATTR, EXPIRATION_ATTR
from helpers.log_helper import get_logger
from services.clients.license_manager import LicenseManagerClientInterface, \
    LicenseManagerClientFactory
from services.environment_service import EnvironmentService
from services.setting_service import SettingsService
from services.ssm_service import SSMService

CONNECTION_ERROR_MESSAGE = 'Can\'t establish connection with ' \
                           'License Manager. Please contact the support team.'
SSM_LM_TOKEN_KEY = 'caas_lm_auth_token_{customer}'
DEFAULT_CUSTOMER = 'default'

_LOG = get_logger(__name__)


class LicenseManagerService:
    def __init__(self, settings_service: SettingsService,
                 ssm_service: SSMService,
                 environment_service: EnvironmentService):
        self.settings_service = settings_service
        self.ssm_service = ssm_service
        self.environment_service = environment_service

    @cached_property
    def client(self) -> LicenseManagerClientInterface:
        _LOG.debug('Creating license manager client inside LM service')
        return LicenseManagerClientFactory(self.settings_service).create()

    def synchronize_license(self, license_key: str):
        """
        Mandates License synchronization request, delegated to prepare
        a custodian service-token, given the Service is the SaaS installation.
        For any request related exception, returns the respective instance
        to handle on.

        :parameter license_key: str,
        :parameter expires: Optional[dict]
        :return: Union[Response, ConnectionError, RequestException]
        """
        auth = self._get_client_token()
        if not auth:
            _LOG.warning('Client authorization token could be established.')
            return None

        try:
            response = self.client.license_sync(
                license_key=license_key, auth=auth
            )

        except (ConnectionError, Timeout) as _ce:
            _LOG.warning(f'Connection related error has occurred: {_ce}.')
            _error = 'Connection to LicenseManager can not be established.'
            response = ConnectionError(CONNECTION_ERROR_MESSAGE)

        except RequestException as _re:
            _LOG.exception('An exception occurred')
            response = RequestException(CONNECTION_ERROR_MESSAGE)

        return response

    def is_allowed_to_license_a_job(self, customer: str, tenant: str,
                                    tenant_license_keys: list[str]) -> bool:
        """
        License manager allows to check whether the job is allowed for
        multiple tenants. But currently for custodian we just need to check
        one tenant
        :param customer:
        :param tenant:
        :param tenant_license_keys:
        :return:
        """
        auth = self._get_client_token(customer=customer)
        if not auth:
            _LOG.warning('Client authorization token could be established.')
            return False

        return self.client.job_check_permission(
            customer=customer, tenant=tenant,
            tenant_license_keys=tenant_license_keys, auth=auth
        )

    def update_job_in_license_manager(self, job_id, created_at, started_at,
                                      stopped_at, status) -> int | None:
        auth = self._get_client_token()
        if not auth:
            _LOG.warning('Client authorization token could be established.')
            return None

        response = self.client.update_job(
            job_id=job_id, created_at=created_at, started_at=started_at,
            stopped_at=stopped_at, status=status, auth=auth
        )
        return getattr(response, 'status_code', None)

    def activate_customer(self, customer: str, tlk: str) -> dict | None:
        auth = self._get_client_token(customer=customer)
        if not auth:
            _LOG.warning('Client authorization token could be established.')
            return None
        _empty_response = {}
        response = self.client.activate_customer(
            customer=customer, tlk=tlk, auth=auth
        )
        if getattr(response, 'status_code', None) != HTTPStatus.OK:
            return _empty_response

        _json = self.client.retrieve_json(response=response)
        _json = _json or dict()
        response = _json.get('items') or []
        return response[0] if len(response) == 1 else {}

    def _get_client_token(self, customer: str = None):
        secret_name = self.get_ssm_auth_token_name(customer=customer)
        cached_auth = self.ssm_service.get_secret_value(
            secret_name=secret_name) or {}
        cached_token = cached_auth.get(TOKEN_ATTR)
        cached_token_expiration = cached_auth.get(EXPIRATION_ATTR)

        if (cached_token and cached_token_expiration and
                not self.is_expired(expiration=cached_token_expiration)):
            _LOG.debug(f'Using cached lm auth token.')
            return cached_token
        _LOG.debug(f'Cached lm auth token are not found or expired. '
                   f'Generating new token.')
        lifetime_minutes = self.environment_service.lm_token_lifetime_minutes()
        token = self._generate_client_token(
            lifetime=lifetime_minutes,
            customer=customer
        )

        _LOG.debug(f'Updating lm auth token in SSM.')
        secret_data = {
            EXPIRATION_ATTR: int(time.time()) + lifetime_minutes * 60,
            TOKEN_ATTR: token
        }
        self.ssm_service.create_secret_value(
            secret_name=secret_name,
            secret_value=secret_data
        )
        return token

    @staticmethod
    def is_expired(expiration: int):
        now = int(datetime.utcnow().timestamp())
        return now >= expiration

    def _generate_client_token(self, lifetime: int, customer: str):
        """
        Delegated to derive a custodian-service-token, encoding any given
        payload key-value pairs into the claims.
        :parameter lifetime: token lifetime in minutes
        :parameter customer: str
        :return: Union[str, Type[None]]
        """
        # not to bring cryptography to global
        from services.license_manager_token import LicenseManagerToken
        key_data = self.client.client_key_data
        kid, alg = key_data.get(KID_ATTR), key_data.get(ALG_ATTR)
        if not (kid and alg):
            _LOG.warning('LicenseManager Client-Key data is missing.')
            return
        pem = self.ssm_service.get_secret_value(
            secret_name=self.derive_client_private_key_id(kid)
        ).get('value')
        token = LicenseManagerToken(
            customer=customer,
            lifetime=lifetime,
            kid=kid,
            private_pem=pem.encode()
        )
        return token.produce()

    @staticmethod
    def derive_client_private_key_id(kid: str):
        return f'cs_lm_client_{kid}_prk'

    @staticmethod
    def get_ssm_auth_token_name(customer: str = None):
        if customer:
            customer = re.sub(r"[\s-]", '_', customer.lower())
        else:
            customer = DEFAULT_CUSTOMER
        return SSM_LM_TOKEN_KEY.format(customer=customer)
