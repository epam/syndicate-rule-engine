import base64
import json
import time

import pytest
from jwcrypto import jwk, jwt

from services.clients.jwt_management_client import JWTManagementClient


@pytest.fixture
def ec_key() -> jwk.JWK:
    return jwk.JWK.generate(kty='EC', crv='P-521')


@pytest.fixture
def rsa_key() -> jwk.JWK:
    return jwk.JWK.generate(kty='RSA', size=2048)


@pytest.fixture
def ec_key_pem(ec_key) -> bytes:
    return ec_key.export_to_pem(private_key=True, password=None)


@pytest.fixture
def rsa_key_pem(rsa_key) -> bytes:
    return rsa_key.export_to_pem(private_key=True, password=None)


@pytest.fixture
def ec_key_pem_b64(ec_key_pem) -> str:
    return base64.b64encode(ec_key_pem).decode()


@pytest.fixture
def rsa_key_pem_b64(rsa_key_pem) -> str:
    return base64.b64encode(rsa_key_pem).decode()


def test_init(rsa_key, ec_key):
    cl = JWTManagementClient(rsa_key)
    assert cl.key_type == 'RSA'
    assert cl.key_alg == 'PS256'
    cl = JWTManagementClient(ec_key)
    assert cl.key_type == 'EC'
    assert cl.key_alg == 'ES512'

    with pytest.raises(AssertionError):
        JWTManagementClient(jwk.JWK.generate(kty='oct', size=256))


def test_from_pem(ec_key_pem, rsa_key_pem):
    assert JWTManagementClient.from_pem(ec_key_pem).key_type == 'EC'
    assert JWTManagementClient.from_pem(rsa_key_pem).key_type == 'RSA'


def test_from_pem_b64(ec_key_pem_b64, rsa_key_pem_b64):
    assert JWTManagementClient.from_b64_pem(ec_key_pem_b64).key_type == 'EC'
    assert JWTManagementClient.from_b64_pem(rsa_key_pem_b64).key_type == 'RSA'


def test_sign_verify(ec_key_pem_b64):
    cl = JWTManagementClient.from_b64_pem(ec_key_pem_b64)
    exp = int(time.time()) + 100
    signed = cl.sign({'key': 'value'}, exp=exp)
    dct = json.loads(cl.verify(signed).claims)
    assert dct['key'] == 'value'
    assert dct['exp'] == exp
    assert 'iat' in dct


def test_verify_expired(ec_key_pem_b64):
    cl = JWTManagementClient.from_b64_pem(ec_key_pem_b64)
    exp = int(time.time()) - 100
    signed = cl.sign({'key': 'value'}, exp=exp)
    with pytest.raises(jwt.JWTExpired) as e:
        cl.verify(signed)


def test_encrypt_decrypt_dict(ec_key_pem_b64):
    cl = JWTManagementClient.from_b64_pem(ec_key_pem_b64)
    assert cl.decrypt_dict(cl.encrypt_dict({'key': 'value'})) == {
        'key': 'value'
    }
