import re

from helpers.constants import VERSION_NORM_LENGTH

def to_normalized_version(version: str, length: int = VERSION_NORM_LENGTH) -> str:
    """
    Convert plain version string to normalized version:
    1.12.3 -> 000001.000012.000003
    :param version:
    :param length:
    :return:
    """
    pattern = r'\.'.join([r'\d{1,' + str(length) + r'}' for _ in range(3)])
    if not re.fullmatch(pattern, version):
        raise ValueError('Invalid version string')
    
    major, minor, patch = version.split('.')
    return f'{major:>0{length}}.{minor:>0{length}}.{patch:>0{length}}'

def _lstrip_zero(value: str) -> str:
    value = value.lstrip('0')
    return value if value else '0'

def from_normalized_version(version:str) -> str:
    """
    Convert normalized version string to plain version:
    01.000012.003 -> 1.12.3
    :param version:
    :param length:
    :return:
    """
    pattern = r'\.'.join([r'\d+' for _ in range(3)])
    if not re.fullmatch(pattern, version):
        raise ValueError('Invalid version string')
    
    major, minor, patch = version.split('.')
    return f'{_lstrip_zero(major)}.{_lstrip_zero(minor)}.{_lstrip_zero(patch)}'

    