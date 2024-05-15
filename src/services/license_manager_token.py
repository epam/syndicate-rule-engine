# separate module in order not to require Crytography for all lambdas
import base64
import json
import time

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import \
    decode_dss_signature
from cryptography.hazmat.primitives.serialization import load_pem_private_key

try:
    from jwcrypto.jwa import _encode_int
except ImportError:
    from binascii import unhexlify


    def _encode_int(n, bits):
        e = '{:x}'.format(n)
        ilen = ((bits + 7) // 8) * 2
        return unhexlify(e.rjust(ilen, '0')[:ilen])


class LicenseManagerToken:
    """
    The thing is: license manager API is authenticated via JWT tokens, but
    those are - custom tokens: they do not follow JOSE standards. They require
    something like that: "ECC:p521_DSS_SHA:256" to be set to `alg` header, so
    neither PyJWT nor jwcrypto support such tokens (as far as I investigated).

    Example header

    .. code-block:: json

        {
            "typ": "client-token",
            "alg": "ECC:p521_DSS_SHA:256",
            "kid": "f20e1939-7d37-48ad-a9eb-35db6723b013"
        }

    Example payload

    .. code-block:: json

        {
            "customer": "EPAM Systems",
            "exp": 1707502018.614004
        }

    We do not require PyJWT lib for application, but jwcrypto is used, though
    not for this token.
    Another thing: initially these tokens were implemented simultaneously by
    one person both for Rule engine and for License Manager. Pycryptodome lib
    is used by LM and was used by Rule Engine (before this implementation).
    I did not like that 'cause we needed to bring Pycryptodome to lambda
    bundles and that lib is huge. Besides, we already had Cryptography as
    requirements. So, to sum up: LM uses Pycryptodome and to hell with that
    (maybe will be rewritten someday). But for Rule engine I removed
    Pycryptodome and use this custom token with Cryptography for signing.

    Currently, LM supports only one type of key-pairs: Elliptic Curve P-521,
    so don't burden myself with other implementations.

    """
    __slots__ = ('kid', 'key', 'customer', 'lifetime')

    def __init__(self, kid: str, private_pem: bytes, customer: str,
                 lifetime: int):
        """
        :param customer:
        :param lifetime: lifetime in minutes
        """
        self.kid = kid
        self.key = load_pem_private_key(private_pem, None)
        self.customer = customer
        self.lifetime = lifetime

    @staticmethod
    def _encode(data: bytes):
        return base64.urlsafe_b64encode(data).replace(b'=', b'')

    def produce(self) -> str:
        header = {
            'typ': 'client-token',
            'alg': 'ECC:p521_DSS_SHA:256',
            'kid': self.kid
        }
        payload = {
            'customer': self.customer,
            'exp': int(time.time()) + self.lifetime * 60
        }
        message = b'.'.join([
            self._encode(json.dumps(part, separators=(',', ':')).encode())
            for part in (header, payload)
        ])
        der_signature = self.key.sign(
            message,
            ec.ECDSA(hashes.SHA256())
        )
        # gives signature in DER format, need to convert to binary, because
        # pycryptodome on LM side requires binary (ECDSA) format
        size = self.key.key_size
        r, s = decode_dss_signature(der_signature)
        signature = _encode_int(r, size) + _encode_int(s, size)
        return b'.'.join((message, self._encode(signature))).decode()
