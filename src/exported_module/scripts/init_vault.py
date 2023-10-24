import base64
import os

from services import SERVICE_PROVIDER


def generate_256_bit() -> str:
    return base64.b64encode(os.urandom(24)).decode('utf-8')


def init_vault():
    ssm = SERVICE_PROVIDER.ssm_service()
    if ssm.enable_secrets_engine():
        print('Vault engine was enabled')
    else:
        print('Vault engine has been already enabled')
    if ssm.get_secret_value('token'):
        print('Token inside Vault already exists. Skipping...')
        return
    ssm.create_secret_value(
        secret_name='token',
        secret_value={
            "phrase": generate_256_bit()
        }
    )
    print('Token was set to Vault')


if __name__ == '__main__':
    init_vault()
