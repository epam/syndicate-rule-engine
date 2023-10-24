from abc import ABC, abstractmethod
from typing import Union, Optional, TypeVar, Generic, Dict
from re import match
from helpers.log_helper import get_logger

K = TypeVar('K')

KEY_TYPE_ATTR = 'key_typ'
KEY_STD_ATTR = 'key_std'
SIG_SCHEME_ATTR = 'sig_scheme'
HASH_TYPE_ATTR = 'hash_type'
HASH_STD_ATTR = 'hash_std'

ALG_PATTERN = f'(?P<{KEY_TYPE_ATTR}>.+):(?P<{KEY_STD_ATTR}>.+)_' \
              f'(?P<{SIG_SCHEME_ATTR}>.+)_' \
              f'(?P<{HASH_TYPE_ATTR}>.+):(?P<{HASH_STD_ATTR}>.+)'


_LOG = get_logger(__name__)


class IKey(Generic[K]):

    @classmethod
    def import_key(cls, encoded: Union[str, bytes], **kwargs) -> K:
        ...

    def export_key(self, format: str, **kwargs) -> Union[str, bytes]:
        ...

    def public_key(self) -> K:
        ...


class AbstractKeyManagementClient(ABC):

    @abstractmethod
    def sign(self, key_id, message: Union[str, bytes], algorithm: str,
             encoding='utf-8') -> Optional[bytes]:
        raise NotImplementedError()

    @abstractmethod
    def verify(self, key_id: str, message: Union[str, bytes], algorithm: str,
               signature: bytes, encoding='utf-8') -> bool:
        raise NotImplementedError()

    @abstractmethod
    def generate(self, key_type: str, key_std: str, **data) -> IKey:
        raise NotImplementedError()

    @abstractmethod
    def save(self, key_id: str, key: IKey, key_format: str, **data):
        raise NotImplementedError()

    @abstractmethod
    def delete(self, key_id: str):
        raise NotImplementedError()

    @abstractmethod
    def get_key(self, key_type: str, key_std: str, key_data: dict) -> \
            Optional[IKey]:
        raise NotImplementedError()

    @abstractmethod
    def get_key_data(self, key_id: str) -> Optional[dict]:
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def construct(cls, key_type: str, key_std: str, key_value: str, **data):
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def is_signature_scheme_accessible(
        cls, sig_scheme: str, key_type: str, key_std: str, hash_type: str,
        hash_std: str
    ):
        raise NotImplemented()

    @staticmethod
    def dissect_alg(alg: str) -> Optional[Dict[str, str]]:
        """
        Returns `alg` value dissected dict-object, with the following keys:
        - key_type: str
        - key_std: str
        - sig_scheme: str
        - hash_type: str
        - hash_std: str
        :return: Optional[Dict[str, str]]
        """
        mapped = None
        try:
            m = match(pattern=ALG_PATTERN, string=alg)
            mapped = m.groupdict()
        except (ValueError, Exception) as e:
            _LOG.warning(f'Alg:\'{alg}\' could not be dissected due to "{e}".')
        return mapped
