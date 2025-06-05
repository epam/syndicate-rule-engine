import pytest

from helpers.rules import to_normalized_version, from_normalized_version

def test_to_normalized_version():
    assert to_normalized_version('1.2.3', 3) == '001.002.003'
    assert to_normalized_version('100.200.300', 3) == '100.200.300'
    assert to_normalized_version('1.2.0', 3) == '001.002.000'
    with pytest.raises(ValueError):
        to_normalized_version('1.2')
    with pytest.raises(ValueError):
        to_normalized_version('1.2.333', 2)


def test_from_normalized_version():
    assert from_normalized_version('001.2.00003') == '1.2.3'
    assert from_normalized_version('100.200.300') == '100.200.300'
    assert from_normalized_version('001.002.000') == '1.2.0'
    with pytest.raises(ValueError):
        from_normalized_version('000001.000002')
    with pytest.raises(ValueError):
        from_normalized_version('1.2.3.4.5')
