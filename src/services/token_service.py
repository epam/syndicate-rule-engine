from services.clients.abstract_key_management import \
    AbstractKeyManagementClient
from helpers.security import AbstractTokenEncoder, TokenEncoder

from helpers.constants import CLIENT_TOKEN_ATTR

from helpers.log_helper import get_logger

from typing import Union, Type

_LOG = get_logger(__name__)

EXPIRATION_ATTR = 'exp'
ENCODING = 'utf-8'


class TokenService:
    def __init__(self, client: AbstractKeyManagementClient):
        self._client = client

    def derive_encoder(self, token_type: str, **payload) -> \
            Union[AbstractTokenEncoder, Type[None]]:
        """
        Mandates preliminary token-encoder preparation, based on a respective
        token type, given there is such one. For any injectable data,
        attaches said payload to the token as claims.
        :parameter token_type: str
        :return: Union[AbstractTokenEncoder, Type[None]]
        """
        t_head = f'\'{token_type}\''
        _LOG.debug(f'Going to instantiate a {t_head} encoder.')

        reference_map = self._token_type_builder_map()
        _Encoder: Type[AbstractTokenEncoder] = reference_map.get(token_type)
        if not _Encoder:
            _LOG.warning(f'{t_head.capitalize()} encoder does not exist.')
            return None

        encoder, key = _Encoder(), None
        encoder.key_management = self._client
        encoder.typ = token_type
        for key, value in payload.items():
            encoder[key] = value
        else:
            if key:
                _LOG.debug(f'{t_head} encoder has been attached '
                           f'with {payload} claims.')
        return encoder

    @staticmethod
    def _token_type_builder_map():
        return {
            CLIENT_TOKEN_ATTR: TokenEncoder
        }
