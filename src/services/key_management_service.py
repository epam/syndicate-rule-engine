from services.clients.abstract_key_management import \
    AbstractKeyManagementClient, IKey, KEY_TYPE_ATTR, KEY_STD_ATTR, \
    HASH_TYPE_ATTR, HASH_STD_ATTR, SIG_SCHEME_ATTR

from helpers.constants import KEY_ID_ATTR, ALGORITHM_ATTR, VALUE_ATTR, \
    B64ENCODED_ATTR
from helpers.log_helper import get_logger
from base64 import standard_b64encode
from typing import Optional

PUBLIC_KEY_ATTR = 'puk'
PRIVATE_KEY_ATTR = 'prk'
ALG_ATTR = 'alg'
KID_ATTR = 'kid'
FORMAT_ATTR = 'format'


_LOG = get_logger(__name__)


class ManagedKey:
    def __init__(self, kid: str, alg: str, key: IKey):
        self.kid = kid
        self.alg = alg
        self.key = key

    def export_key(self, frmt: str, base64encode: bool = False):
        try:
            value = self.key.export_key(format=frmt)
        except (TypeError, Exception) as e:
            _LOG.warning(f'Key:\'{self.kid}\' could not be exported into '
                         f'{frmt} format, due to: "{e}".')
            value = None

        base = {
            KEY_ID_ATTR: self.kid,
            ALGORITHM_ATTR: self.alg
        }

        pending = {}

        if value:
            pending[FORMAT_ATTR] = frmt
            pending[VALUE_ATTR] = value

        if pending[VALUE_ATTR] and base64encode:
            pending[B64ENCODED_ATTR] = True
            pending[VALUE_ATTR] = standard_b64encode(
                value if isinstance(value, bytes) else value.encode('utf-8')
            )
        elif pending[VALUE_ATTR] and not base64encode:
            pending[B64ENCODED_ATTR] = False

        if pending[VALUE_ATTR] and isinstance(value, bytes):

            try:
                pending[VALUE_ATTR] = value.decode()
            except (TypeError, Exception) as e:
                _LOG.warning(f'Key:\'{self.kid}\' could not be decoded into'
                             f' a string, due to: "{e}".')
                pending = {}

        base.update(pending)
        return base


class KeyPair:
    def __init__(self, prk: IKey, typ: str, std: str):
        self.prk: IKey = prk
        self.puk: IKey = prk.public_key()
        self.typ = typ
        self.std = std


class KeyManagementService:

    def __init__(
        self, key_management_client: AbstractKeyManagementClient
    ):
        self._key_management_client = key_management_client

    def get_key(self, kid: str, alg: str) -> Optional[ManagedKey]:
        _alg = alg
        alg = self._key_management_client.dissect_alg(alg=alg)
        if not alg:
            return

        # Retrieve type and standard data of a key, hash and signature scheme.
        key_type, key_std = map(
            alg.get, (KEY_TYPE_ATTR, KEY_STD_ATTR)
        )

        data = self._key_management_client.get_key_data(
            key_id=kid
        )
        if not data:
            return

        key = self._key_management_client.get_key(
            key_type=key_type, key_std=key_std, key_data=data
        )
        if key:
            return self.instantiate_managed_key(kid=kid, alg=_alg, key=key)

    def import_key(self, alg: str, key_value: str) -> Optional[IKey]:
        alg = self._key_management_client.dissect_alg(alg=alg)
        if not alg:
            return

        # Retrieve type and standard data of a key, hash and signature scheme.
        key_type, key_std, hash_type, hash_std, sig_scheme = map(
            alg.get, (
                KEY_TYPE_ATTR, KEY_STD_ATTR, HASH_TYPE_ATTR, HASH_STD_ATTR,
                SIG_SCHEME_ATTR
            )
        )

        if not self._key_management_client.is_signature_scheme_accessible(
            sig_scheme=sig_scheme, key_type=key_type, key_std=key_std,
            hash_type=hash_type, hash_std=hash_std
        ):
            return
        return self._key_management_client.construct(
            key_type=key_type, key_std=key_std, key_value=key_value
        )

    def import_key_pair(self, alg: str, private_key: str) -> Optional[KeyPair]:
        alg = self._key_management_client.dissect_alg(alg=alg)
        if not alg:
            return

        # Retrieve type and standard data of a key, hash and signature scheme.
        key_type, key_std, hash_type, hash_std, sig_scheme = map(
            alg.get, (
                KEY_TYPE_ATTR, KEY_STD_ATTR, HASH_TYPE_ATTR, HASH_STD_ATTR,
                SIG_SCHEME_ATTR
            )
        )

        if not self._key_management_client.is_signature_scheme_accessible(
            sig_scheme=sig_scheme, key_type=key_type, key_std=key_std,
            hash_type=hash_type, hash_std=hash_std
        ):
            return
        prk = self._key_management_client.construct(
            key_type=key_type, key_std=key_std, key_value=private_key
        )
        if prk:
            return KeyPair(prk=prk, typ=key_type, std=key_std)

    def create_key_pair(self, key_type: str, key_std: str) -> \
            Optional[KeyPair]:
        prk = self._key_management_client.generate(
            key_type=key_type, key_std=key_std
        )
        if not prk:
            return

        try:
            return KeyPair(prk=prk, typ=key_type, std=key_std)
        except (TypeError, Exception) as e:
            _LOG.warning(f'KeyPair of {key_type}:{key_std} standard'
                         f' could not be instantiated, due to "{e}".')
        return

    def save_key(self, kid: str, key: IKey, frmt: str) -> bool:
        _log = _LOG.info
        action = ' has been persisted.'
        head = f'Key:\'{kid}\''
        persisted = self._key_management_client.save(
            key_id=kid, key=key, key_format=frmt
        )
        if not persisted:
            _log = _LOG.warning
            action = ' could not be persisted.'
        _log(head + action)
        return persisted

    def delete_key(self, kid: str) -> bool:
        _log = _LOG.info
        action = ' has been removed.'
        head = f'Key:\'{kid}\''
        removed = self._key_management_client.delete(key_id=kid)
        if not removed:
            _log = _LOG.warning
            action = ' could not be removed.'
        _log(head + action)
        return removed

    def derive_alg(
        self, key_type: str, key_std: str, hash_type: str, hash_std: str,
        sig_scheme: str
    ) -> Optional[str]:
        if self._key_management_client.is_signature_scheme_accessible(
            sig_scheme=sig_scheme, hash_type=hash_type, hash_std=hash_std,
            key_type=key_type, key_std=key_std
        ):
            return f'{key_type}:{key_std}_{sig_scheme}_{hash_type}:{hash_std}'

    @staticmethod
    def instantiate_managed_key(kid: str, alg: str, key: IKey):
        return ManagedKey(kid=kid, alg=alg, key=key)

