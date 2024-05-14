import re
import time
from datetime import datetime
from functools import cached_property
from http import HTTPStatus
from typing import List, Dict, Optional

from executor.services.environment_service import BatchEnvironmentService
from helpers.constants import KID_ATTR, ALG_ATTR, JobState, TOKEN_ATTR, \
    EXPIRATION_ATTR
from helpers.log_helper import get_logger
from services.clients.license_manager import LicenseManagerClientFactory
from services.clients.license_manager import LicenseManagerClientInterface
from services.clients.ssm import AbstractSSMClient
from services.setting_service import SettingsService

_LOG = get_logger(__name__)

SSM_LM_TOKEN_KEY = 'caas_lm_auth_token_{customer}'
DEFAULT_CUSTOMER = 'default'


class BalanceExhaustion(Exception):
    def __init__(self, message: str):
        self.message = message

    def __str__(self):
        return self.message


class InaccessibleAssets(Exception):
    def __init__(
            self, message: str, assets: Dict[str, List[str]],
            hr_sep: str, ei_sep: str, i_sep: str, i_wrap: Optional[str] = None
    ):
        self._assets = self._dissect(
            message=message, assets=assets, hr_sep=hr_sep, ei_sep=ei_sep,
            i_sep=i_sep, i_wrap=i_wrap
        )

    @staticmethod
    def _dissect(
            message: str, assets: Dict[str, List[str]],
            hr_sep: str, ei_sep: str, i_sep: str, i_wrap: Optional[str] = None
    ):
        """
        Dissects License Manager response of entity(ies)-not-found message.
        Such as: TenantLicense or Ruleset(s):$id(s) - $reason.
        param message: str - maintains the raw response message
        param assets: Dict[str, List[str]] - source of assets to
        param hr_sep: str - head-reason separator, within the response message
        param ei_sep: str - entity type - id(s) separator, within the head of
        the message
        param i_sep: str - separator of entity-identifier(s), within the raw
        id(s).
        param i_wrap: Optional[str] - quote-type wrapper of each identifier.
        """
        each_template = 'Each of {} license-subscription'
        head, *_ = message.rsplit(hr_sep, maxsplit=1)
        head = head.strip(' ')
        if not head:
            _LOG.error(f'Response message is not separated by a \'{hr_sep}\'.')
            return

        entity, *ids = head.split(ei_sep, maxsplit=1)
        ids = ids[0] if len(ids) == 1 else ''
        if 's' in entity and entity.index('s') == len(entity) - 1:
            ids = ids.split(i_sep)

        ids = [each.strip(i_wrap or '') for each in ids.split(i_sep)]

        if 'TenantLicense' in entity:
            ids = [
                asset
                for tlk in ids
                if tlk in assets
                for asset in assets[tlk] or [each_template.format(tlk)]
            ]

        return ids

    def __str__(self):
        head = 'Ruleset'

        if len(self._assets) > 1:
            head += 's'
        scope = ', '.join(f'"{each}"' for each in self._assets)
        reason = 'are' if len(self._assets) > 1 else 'is'
        reason += ' no longer accessible'
        return f'{head}:{scope} - {reason}.'

    def __iter__(self):
        return iter(self._assets)


class LicenseManagerService:

    def __init__(self, settings_service: SettingsService,
                 ssm_client: AbstractSSMClient,
                 environment_service: BatchEnvironmentService):
        self.settings_service = settings_service
        self.ssm_client = ssm_client
        self.environment_service = environment_service

    @cached_property
    def client(self) -> LicenseManagerClientInterface:
        _LOG.debug('Creating license manager client inside LM service')
        return LicenseManagerClientFactory(self.settings_service).create()

    def update_job_in_license_manager(self, job_id: str,
                                      created_at: str = None,
                                      started_at: str = None,
                                      stopped_at: str = None,
                                      status: JobState = None):

        auth = self._get_client_token()
        if not auth:
            _LOG.warning('Client authorization token could be established.')
            return None

        response = self.client.update_job(
            job_id=job_id, created_at=created_at, started_at=started_at,
            stopped_at=stopped_at, status=status, auth=auth
        )

        if response and response.status_code == HTTPStatus.OK.value:
            return self.client.retrieve_json(response)
        return

    def instantiate_licensed_job_dto(self, job_id: str, customer: str,
                                     tenant: str,
                                     ruleset_map: dict[str, list[str]]
                                     ) -> dict | None:
        """
        Mandates licensed Job data transfer object retrieval,
        by successfully interacting with LicenseManager providing the
        following parameters.

        :parameter job_id: str
        :parameter customer: str
        :parameter tenant: str
        :parameter ruleset_map: Union[Type[None], List[str]]

        :raises: InaccessibleAssets, given the requested content is not
        accessible
        :raises: BalanceExhaustion, given the job-balance has been exhausted
        :return: Optional[Dict]
        """
        auth = self._get_client_token(customer=customer)
        if not auth:
            _LOG.warning('Client authorization token could be established.')
            return None
        response = self.client.post_job(
            job_id=job_id, customer=customer, tenant=tenant,
            ruleset_map=ruleset_map, auth=auth
        )
        if response is None:
            return

        decoded = self.client.retrieve_json(response) or {}
        if response.status_code == HTTPStatus.OK.value:
            items = decoded.get('items', [])
            if len(items) != 1:
                _LOG.warning(f'Unexpected License Manager response: {items}.')
                item = None
            else:
                item = items.pop()
            return item

        else:
            message = decoded.get('message')
            if response.status_code == HTTPStatus.NOT_FOUND.value:
                raise InaccessibleAssets(
                    message=message, assets=ruleset_map,
                    hr_sep='-', ei_sep=':', i_sep=', ', i_wrap='\''
                )
            elif response.status_code == HTTPStatus.FORBIDDEN.value:
                raise BalanceExhaustion(message)

    def instantiate_job_sourced_ruleset_list(self, licensed_job_dto: dict):
        """
        Mandates production of ruleset dto list, items of which have been
        attached to a licensed job. Aforementioned data is retrieved from
        a response object of a `Job` instantiation request, denoted
        `license_job_dto`.
        :parameter licensed_job_dto: dict
        :return: List[Dict]
        """
        _default = self._default_instance
        licensed_job_dto = _default(licensed_job_dto, dict)
        content = _default(licensed_job_dto.get('ruleset_content'), dict)

        return [
            self._instantiate_licensed_ruleset_data(ruleset_id=ruleset_id,
                                                    source=source)
            for ruleset_id, source in content.items()
        ]

    def _get_client_token(self, customer: str = None):
        secret_name = self.get_ssm_auth_token_name(customer=customer)
        cached_auth = self.ssm_client.get_secret_value(
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
        self.ssm_client.create_secret(
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
        pem = self.ssm_client.get_secret_value(
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
    def _instantiate_licensed_ruleset_data(ruleset_id: str, source: str):
        """
        Designated to produce an ambiguously licensed ruleset data, including
        a given `ruleset_id` and URI `source`.
        :parameter ruleset_id: str
        :parameter source: str
        :return: Dict
        """
        return dict(id=ruleset_id, licensed=True, s3_path=source,
                    active=True, status=dict(code='READY_TO_SCAN'))

    @staticmethod
    def _default_instance(value, _type: type, *args, **kwargs):
        return value if isinstance(value, _type) else _type(*args, **kwargs)

    @staticmethod
    def get_ssm_auth_token_name(customer: str = None):
        if customer:
            customer = re.sub(r"[\s-]", '_', customer.lower())
        else:
            customer = DEFAULT_CUSTOMER
        return SSM_LM_TOKEN_KEY.format(customer=customer)
