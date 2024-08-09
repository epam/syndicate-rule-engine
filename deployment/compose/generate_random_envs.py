import argparse
import random
import secrets
import string
import sys


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Random envs generator for docker compose'
    )
    parser.add_argument('--rule-engine', action='store_true',
                        help='Use this flag to generate envs for rule engine')
    parser.add_argument('--dojo', action='store_true',
                        help='Use this flag to generate envs for defect dojo')
    return parser


_population: str = string.digits + string.ascii_letters


def random_pass(n: int = 32) -> str:
    while 1:
        password = ''.join(random.sample(_population, n))
        _lower = any(c.islower() for c in password)
        _upper = any(c.isupper() for c in password)
        _digit = any(c.isdigit() for c in password)
        if _lower and _upper and _digit:
            break
    return password + '='


def random_pass2(n: int = 32) -> str:
    return secrets.token_hex(int(n / 2))


def generate_rule_engine() -> None:
    envs = {
        'CAAS_VAULT_TOKEN': random_pass(),
        'CAAS_MONGO_USERNAME': 'mongouser',
        'CAAS_MONGO_PASSWORD': random_pass(),
        'CAAS_MONGO_DATABASE': 'custodian_as_a_service',
        'CAAS_MINIO_ACCESS_KEY_ID': random_pass(),
        'CAAS_MINIO_SECRET_ACCESS_KEY': random_pass(),
        'CAAS_SYSTEM_USER_PASSWORD': random_pass(),

        'MODULAR_API_SECRET_KEY': random_pass(),
        'MODULAR_API_INIT_PASSWORD': random_pass(),

        'MODULAR_SERVICE_SYSTEM_USER_PASSWORD': random_pass()
    }
    for k, v in envs.items():
        sys.stdout.write(f'{k}={v}\n')


def generate_dojo() -> None:
    envs = {
        'DD_SECRET_KEY': random_pass(),
        'DD_CREDENTIAL_AES_256_KEY': random_pass(),
        'DD_DATABASE_NAME': 'defectdojo',
        'DD_DATABASE_USER': 'defectdojo',
        'DD_DATABASE_PASSWORD': random_pass()
    }
    for k, v in envs.items():
        sys.stdout.write(f'{k}={v}\n')


def generate(rule_engine: bool, dojo: bool) -> None:
    if rule_engine:
        generate_rule_engine()
    elif dojo:
        generate_dojo()
    else:
        sys.stdout.write(random_pass())


def main():
    arguments = build_parser().parse_args()
    generate(**vars(arguments))


if __name__ == '__main__':
    main()
