from helpers import RESPONSE_OK_CODE
from helpers.constants import KID_ATTR, ALG_ATTR, CLIENT_TOKEN_ATTR
from helpers.log_helper import get_logger
from services.clients.license_manager import LicenseManagerClient
from services.token_service import TokenService
from typing import Optional, List

from helpers.time_helper import utc_datetime
from datetime import timedelta
from requests.exceptions import RequestException, ConnectionError, Timeout


CHECK_PERMISSION_PATH = '/jobs/check-permission'
JOB_PATH = '/jobs'
SET_CUSTOMER_ACTIVATION_DATE_PATH = '/customers/set-activation-date'

CONNECTION_ERROR_MESSAGE = 'Can\'t establish connection with ' \
                           'License Manager. Please contact the support team.'

CLIENT_TYPE_SAAS = 'SAAS'
CLIENT_TYPE_ONPREM = 'ONPREM'

STATUS_CODE_ATTR = 'status_code'

_LOG = get_logger(__name__)


class LicenseManagerService:
    def __init__(self, license_manager_client: LicenseManagerClient,
                 token_service: TokenService):
        self.license_manager_client = license_manager_client
        self.token_service = token_service

    def synchronize_license(self, license_key: str, expires: dict = None):
        """
        Mandates License synchronization request, delegated to prepare
        a custodian service-token, given the Service is the SaaS installation.
        For any request related exception, returns the respective instance
        to handle on.

        :parameter license_key: str,
        :parameter expires: Optional[dict]
        :return: Union[Response, ConnectionError, RequestException]
        """
        auth = self._get_client_token(expires or dict(hours=1))
        if not auth:
            _LOG.warning('Client authorization token could be established.')
            return None

        try:
            response = self.license_manager_client.license_sync(
                license_key=license_key, auth=auth
            )

        except (ConnectionError, Timeout) as _ce:
            _LOG.warning(f'Connection related error has occurred: {_ce}.')
            _error = 'Connection to LicenseManager can not be established.'
            response = ConnectionError(CONNECTION_ERROR_MESSAGE)

        except RequestException as _re:
            _LOG.warning(f'An exception occurred, during the request: {_re}.')
            response = RequestException(CONNECTION_ERROR_MESSAGE)

        return response

    def is_allowed_to_license_a_job(
        self, customer: str, tenant: str, tenant_license_keys: List[str],
        expires: dict = None
    ):
        auth = self._get_client_token(expires or dict(hours=1))
        if not auth:
            _LOG.warning('Client authorization token could be established.')
            return None

        response = self.license_manager_client.job_check_permission(
            customer=customer, tenant=tenant,
            tenant_license_keys=tenant_license_keys, auth=auth
        )
        # Given 200 - returns True, otherwise False.
        return bool(response)

    def update_job_in_license_manager(
        self, job_id, created_at, started_at,
        stopped_at, status, expires: dict = None
    ):

        auth = self._get_client_token(expires or dict(hours=1))
        if not auth:
            _LOG.warning('Client authorization token could be established.')
            return None

        response = self.license_manager_client.update_job(
            job_id=job_id, created_at=created_at, started_at=started_at,
            stopped_at=stopped_at, status=status, auth=auth
        )

        return getattr(response, STATUS_CODE_ATTR, None)

    def activate_tenant(
        self, tenant: str, tlk: str, expires: dict = None
    ) -> Optional[dict]:

        auth = self._get_client_token(expires or dict(hours=1))
        if not auth:
            _LOG.warning('Client authorization token could be established.')
            return None

        _empty_response = {}
        response = self.license_manager_client.activate_tenant(
            tenant=tenant, tlk=tlk, auth=auth
        )
        if getattr(response, STATUS_CODE_ATTR, None) != RESPONSE_OK_CODE:
            return _empty_response

        _json = self.license_manager_client.retrieve_json(response=response)
        _json = _json or dict()
        response = _json.get('items') or []
        return response[0] if len(response) == 1 else {}

    def activate_customer(self, customer: str, tlk: str, expires: dict = None
                          ) -> Optional[dict]:
        auth = self._get_client_token(expires or dict(hours=1))
        if not auth:
            _LOG.warning('Client authorization token could be established.')
            return None
        _empty_response = {}
        response = self.license_manager_client.activate_customer(
            customer=customer, tlk=tlk, auth=auth
        )
        if getattr(response, STATUS_CODE_ATTR, None) != RESPONSE_OK_CODE:
            return _empty_response

        _json = self.license_manager_client.retrieve_json(response=response)
        _json = _json or dict()
        response = _json.get('items') or []
        return response[0] if len(response) == 1 else {}

    def _get_client_token(self, expires: dict, **payload):
        """
        Delegated to derive a custodian-service-token, encoding any given
        payload key-value pairs into the claims.
        :parameter expires: dict, meant to store timedelta kwargs
        :parameter payload: dict
        :return: Union[str, Type[None]]
        """
        token_type = CLIENT_TOKEN_ATTR
        key_data = self.license_manager_client.client_key_data
        kid, alg = key_data.get(KID_ATTR), key_data.get(ALG_ATTR)
        if not (kid and alg):
            _LOG.warning('LicenseManager Client-Key data is missing.')
            return

        t_head = f'\'{token_type}\''
        encoder = self.token_service.derive_encoder(
            token_type=CLIENT_TOKEN_ATTR, **payload
        )

        if not encoder:
            return None

        # Establish a kid reference to a key.
        encoder.prk_id = self.derive_client_private_key_id(
            kid=kid
        )
        _LOG.info(f'{t_head} - {encoder.prk_id} private-key id has been '
                  f'assigned.')

        encoder.kid = kid
        _LOG.info(f'{t_head} - {encoder.kid} token \'kid\' has been assigned.')

        encoder.alg = alg
        _LOG.info(f'{t_head} - {encoder.alg} token \'alg\' has been assigned.')

        encoder.expire(utc_datetime() + timedelta(**expires))
        try:
            token = encoder.product
        except (Exception, BaseException) as e:
            _LOG.error(f'{t_head} could not be encoded, due to: {e}.')
            token = None

        if not token:
            _LOG.warning(f'{t_head} token could not be encoded.')
        return token

    @staticmethod
    def derive_client_private_key_id(kid: str):
        return f'cs_lm_client_{kid}_prk'
