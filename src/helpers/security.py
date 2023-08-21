import secrets
import string
from abc import ABC, abstractmethod
from base64 import urlsafe_b64encode
from datetime import datetime
from json import dumps

from helpers.constants import KID_ATTR, ALG_ATTR, TYP_ATTR
from services.clients.abstract_key_management import \
    AbstractKeyManagementClient

EXPIRATION_ATTR = 'exp'
ENCODING = 'utf-8'


class AbstractTokenEncoder(ABC):
    # Token: Header fields
    kid: str
    typ: str
    alg: str

    # Token: Production fields
    key_management: AbstractKeyManagementClient

    # Key Management: reference fields
    prk_id: str

    def __init__(self):
        self._reset()

    @abstractmethod
    def __setitem__(self, key, value):
        raise NotImplemented()

    @abstractmethod
    def expire(self, dnt: datetime):
        raise NotImplemented()

    @abstractmethod
    def _reset(self):
        raise NotImplemented()

    @property
    @abstractmethod
    def product(self):
        message = 'Could not produce a Token, due to improper {} attribute.'
        for key, required_type in self.__annotations__.items():
            obj = getattr(self, key, None)
            assert isinstance(obj, required_type), message.format(key)


class TokenEncoder(AbstractTokenEncoder):

    def __setitem__(self, key, value):
        self._payload[key] = value

    def expire(self, dnt: datetime):
        self._payload[EXPIRATION_ATTR] = dnt.timestamp()

    def _reset(self):
        self._payload = {}

    @property
    def product(self):
        _ = super().product

        # Allows to inject header-data, independently.
        header = {
            TYP_ATTR: self.typ, ALG_ATTR: self.alg, KID_ATTR: self.kid
        }

        payload = self._payload

        message = b'.'.join(
            self._encode(dumps(each, separators=(",", ":")).encode(ENCODING))
            for each in (header, payload)
        )

        signature = self.key_management.sign(
            key_id=self.prk_id, message=message, algorithm=self.alg
        )
        token = message + b'.' + self._encode(signature)
        self._reset()
        return token.decode(ENCODING)

    @staticmethod
    def _encode(data: bytes):
        return urlsafe_b64encode(data).replace(b"=", b"")


def gen_password(digits: int = 20) -> str:
    allowed_punctuation = ''.join(set(string.punctuation) - {'"', "'", "!"})
    chars = string.ascii_letters + string.digits + allowed_punctuation
    while True:
        password = ''.join(secrets.choice(chars) for _ in range(digits)) + '='
        if (any(c.islower() for c in password)
                and any(c.isupper() for c in password)
                and sum(c.isdigit() for c in password) >= 3):
            break
    return password
