import time
import random
import os

import pytest
import json
import base64

from services.license_manager_token import LicenseManagerToken
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization

_pycryptodome_installed = True
try:
    # Rule engine does not use Pycryptodome, but License manager does.
    # It requires custom jwt token format, so here we sign a token using
    # Cryptography and verify it using Pycryptodome
    from Crypto.Hash import SHA256
    from Crypto.PublicKey import ECC
    from Crypto.Signature import DSS
except ImportError:
    _pycryptodome_installed = False
    SHA256, ECC, DSS = None, None, None


@pytest.fixture
def private_key() -> bytes:
    """
    Generates elliptic curve P-521 private key using cryptography module and
    returns it in PEM format
    :return:
    """
    return ec.generate_private_key(ec.SECP521R1()).private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )


def test_token_payload(private_key: bytes):
    customer = 'Example'
    lifetime = 10
    kid = 'kid'
    token = LicenseManagerToken(
        customer=customer,
        lifetime=lifetime,
        kid=kid,
        private_pem=private_key
    ).produce()
    assert token
    header, payload, _ = token.split('.')
    header = json.loads(base64.urlsafe_b64decode(header))
    payload = json.loads(base64.urlsafe_b64decode(payload))
    assert header['kid'] == kid
    assert header['alg'] == 'ECC:p521_DSS_SHA:256'
    assert header['typ'] == 'client-token'
    assert payload['customer'] == customer
    assert payload['exp'] <= int(time.time()) + lifetime * 60


@pytest.mark.skipif(not _pycryptodome_installed,
                    reason='Pycryptodome is not installed')
def test_token_verify(private_key: bytes):
    """
    Verifies the token the way LM does
    """
    token = LicenseManagerToken(
        customer='some data',
        kid='example',
        lifetime=10,
        private_pem=private_key
    )

    verifier = DSS.new(ECC.import_key(private_key), 'deterministic-rfc6979')
    for _ in range(50):
        # next line does not matter, just sets some random data
        token.customer = base64.b64encode(os.urandom(random.randint(0, 50))).decode()  # noqa
        message, signature = map(str.encode,
                                 token.produce().rsplit('.', maxsplit=1))
        h = SHA256.new(message)
        try:
            verifier.verify(h, base64.urlsafe_b64decode(signature))
        except ValueError:
            pytest.fail(f'Signature is invalid for {token.customer}')
