from typing import List, Dict, Optional

from helpers.constants import RESPONSE_OK_CODE, RULESET_CONTENT_ATTR, \
    READY_TO_SCAN_CODE, ITEMS_PARAM, MESSAGE_PARAM, CLIENT_TOKEN_ATTR, \
    KID_ATTR, ALG_ATTR, RESPONSE_FORBIDDEN_CODE,\
    RESPONSE_RESOURCE_NOT_FOUND_CODE
from helpers.log_helper import get_logger
from services.clients.license_manager import LicenseManagerClient
from services.token_service import TokenService
from helpers.time_helper import utc_datetime
from datetime import timedelta

_LOG = get_logger(__name__)

GENERIC_JOB_LICENSING_ISSUE = 'Job:\'{id}\' could not be granted by the ' \
                              'License Manager Service.'


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
        if 's' in entity and entity.index('s') == len(entity)-1:
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
        scope = ', ' .join(f'"{each}"' for each in self._assets)
        reason = 'are' if len(self._assets) > 1 else 'is'
        reason += ' no longer accessible'
        return f'{head}:{scope} - {reason}.'

    def __iter__(self):
        return iter(self._assets)


class LicenseManagerService:

    def __init__(
        self, license_manager_client: LicenseManagerClient,
        token_service: TokenService
    ):
        self.license_manager_client = license_manager_client
        self.token_service = token_service

    def update_job_in_license_manager(
        self, job_id: str, created_at: str = None, started_at: str = None,
        stopped_at: str = None, status: str = None, expires: dict = None
    ):

        auth = self._get_client_token(expires or dict(hours=1))
        if not auth:
            _LOG.warning('Client authorization token could be established.')
            return None

        response = self.license_manager_client.patch_job(
            job_id=job_id, created_at=created_at, started_at=started_at,
            stopped_at=stopped_at, status=status, auth=auth
        )

        if response and response.status_code == RESPONSE_OK_CODE:
            return self.license_manager_client.retrieve_json(response)
        return

    def instantiate_licensed_job_dto(
        self, job_id: str, customer: str, tenant: str,
        ruleset_map: Dict[str, List[str]], expires: dict = None
    ):
        """
        Mandates licensed Job data transfer object retrieval,
        by successfully interacting with LicenseManager providing the
        following parameters.

        :parameter job_id: str
        :parameter customer: str
        :parameter tenant: str
        :parameter ruleset_map: Union[Type[None], List[str]]
        :parameter expires: dict, denotes auth-token expiration

        :raises: InaccessibleAssets, given the requested content is not
        accessible
        :raises: BalanceExhaustion, given the job-balance has been exhausted
        :return: Optional[Dict]
        """
        auth = self._get_client_token(expires or dict(hours=1))
        if not auth:
            _LOG.warning('Client authorization token could be established.')
            return None
        response = self.license_manager_client.post_job(
            job_id=job_id, customer=customer, tenant=tenant,
            ruleset_map=ruleset_map, auth=auth
        )
        if response is None:
            return

        decoded = self.license_manager_client.retrieve_json(response) or {}
        if response.status_code == RESPONSE_OK_CODE:
            items = decoded.get(ITEMS_PARAM, [])
            if len(items) != 1:
                _LOG.warning(f'Unexpected License Manager response: {items}.')
                item = None
            else:
                item = items.pop()
            return item

        else:
            message = decoded.get(MESSAGE_PARAM)
            if response.status_code == RESPONSE_RESOURCE_NOT_FOUND_CODE:
                raise InaccessibleAssets(
                    message=message, assets=ruleset_map,
                    hr_sep='-', ei_sep=':', i_sep=', ', i_wrap='\''
                )
            elif response.status_code == RESPONSE_FORBIDDEN_CODE:
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
        content = _default(licensed_job_dto.get(RULESET_CONTENT_ATTR), dict)

        return [
            self._instantiate_licensed_ruleset_data(ruleset_id=ruleset_id,
                                                    source=source)
            for ruleset_id, source in content.items()
        ]

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
                    active=True, status=dict(code=READY_TO_SCAN_CODE))

    @staticmethod
    def _default_instance(value, _type: type, *args, **kwargs):
        return value if isinstance(value, _type) else _type(*args, **kwargs)
