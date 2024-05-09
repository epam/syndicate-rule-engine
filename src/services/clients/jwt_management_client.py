import base64
import json
import time
from typing import Literal
from datetime import timedelta, datetime

from jwcrypto import jwk, jwt

from helpers.log_helper import get_logger

_LOG = get_logger(__name__)


class JWTManagementClient:
    kty_to_encrypt_alg = {  # $kty: ($alg, $enc)
        'RSA': ('RSA-OAEP-256', 'A256CBC-HS512'),
        'EC': ('ECDH-ES+A256KW', 'A256GCM')
    }

    def __init__(self, key: jwk.JWK):
        """
        :param key: private key
        """
        # self._key.key_type
        assert key and key.get('kty') in ('EC', 'RSA'), \
            'EC and RSA keys only allowed'
        self._key = key

    @property
    def key_type(self) -> Literal['EC', 'RSA']:
        return self._key.get('kty')  # self._key.key_type

    @property
    def key_alg(self) -> str:
        """
        To be used in JWT header
        :return:
        """
        if self.key_type == 'RSA':
            return 'PS256'  # TODO get some specific
        curve = self._key.get('crv')  # self._key.key_curve
        match curve:
            case 'P-256':
                return 'ES256'
            case 'P-521':
                return 'ES512'
            case _:
                return 'ES256'  # default

    @property
    def jwk(self) -> jwk.JWK:
        return self._key

    @classmethod
    def from_pem(cls, pem: str | bytes):
        if isinstance(pem, str):
            pem = pem.encode()
        return cls(jwk.JWK.from_pem(pem))

    @classmethod
    def from_b64_pem(cls, pem: str | bytes):
        return cls.from_pem(base64.b64decode(pem))

    def _sign_header(self, **kwargs) -> dict:
        return {
            'alg': self.key_alg,
            # 'typ': 'JWS',  # I don't know whether it's JWT or JWS? Do u know?
            'kid': self._key.thumbprint(),
            'kty': self.key_type,
            **kwargs
        }

    def _encrypt_header(self) -> dict:
        kty = self._key.get('kty')
        return {
            'alg': self.kty_to_encrypt_alg[kty][0],
            'typ': 'JWE',
            'enc': self.kty_to_encrypt_alg[kty][1],
            'kid': self._key.thumbprint(),
            'kty': kty
        }

    @staticmethod
    def _normalize_exp(exp: datetime | timedelta | int) -> int:
        if isinstance(exp, (int, float)):
            return int(exp)
        if isinstance(exp, datetime):
            return int(exp.timestamp())
        if isinstance(exp, timedelta):
            return int(time.time() + exp.seconds)

    def sign(self, claims: dict | str,
             exp: datetime | timedelta | int = None,
             iss: str | None = None, headers: dict = None) -> str:
        """

        :param claims:
        :param exp:
        :param iss:
        :param headers:
        :return:
        """
        if isinstance(claims, dict):  # adding custom params to claims
            if exp:
                claims['exp'] = self._normalize_exp(exp)
            claims['iat'] = int(time.time())  # issued at
            if isinstance(iss, str):
                claims['iss'] = iss

        headers = headers or {}
        token = jwt.JWT(
            header=self._sign_header(**headers),
            claims=claims
        )
        token.make_signed_token(self._key)
        return token.serialize()

    def verify(self, token: str | jwt.JWT) -> jwt.JWT:
        """
        Can raise
        :param token:
        :return:
        """
        return jwt.JWT(
            key=self._key,
            jwt=token.claims if isinstance(token, jwt.JWT) else token,
            expected_type='JWS'
        )

    def encrypt(self, token: str | jwt.JWT) -> str:
        e_token = jwt.JWT(
            header=self._encrypt_header(),
            claims=token.claims if isinstance(token, jwt.JWT) else token
        )
        e_token.make_encrypted_token(self._key)
        return e_token.serialize()

    def decrypt(self, token: str) -> jwt.JWT | None:
        try:
            return jwt.JWT(key=self._key, jwt=token, expected_type="JWE")
        except Exception as e:
            _LOG.warning(f'Cloud not decrypt JWE: {str(e)}')
            return

    def encrypt_dict(self, dct: dict) -> str:
        s = json.dumps(dct, sort_keys=True, separators=(',', ':'))
        return self.encrypt(s)

    def decrypt_dict(self, token: str) -> dict | None:
        """
        Retrieves dict from token received by encrypt_dict
        :param token:
        :return:
        """
        decrypted = self.decrypt(token)
        if decrypted:
            return json.loads(decrypted.claims)
