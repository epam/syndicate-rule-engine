import operator
from functools import cmp_to_key

import pytest

from helpers.reports import severity_cmp, keep_highest, Standard


@pytest.fixture
def standards_dict() -> dict:
    """
    Generates standards dict
    :return:
    """
    return {
        'HIPAA': ['v1 (point1,sub-point1,point2)', 'v2'],
        'Cis Controls': ['(sub-point1,sub-point2)'],
    }


class TestStandard:
    def test_hash_eq(self):
        st1 = Standard(name='HIPAA', version='1.0')
        st1.set_points({'1.1', '1.2'})
        st2 = Standard(name='HIPAA', version='1.0')

        assert st1 == st2
        assert st1 == ('HIPAA', '1.0')
        assert st1 != ('HIPAA', '1.0', {'1.2'})
        assert hash(st1) == hash(st2)
        assert hash(st1) == hash(('HIPAA', '1.0'))

    def test_full_name(self):
        assert Standard(name='HIPAA', version='1.0').full_name == 'HIPAA 1.0'
        assert Standard(name='HIPAA').full_name == 'HIPAA'

    def test_deserialize(self, standards_dict: dict):
        standards = set(Standard.deserialize(standards_dict))
        assert len(standards) == 3
        cis, h1, h2 = sorted(standards, key=operator.attrgetter('full_name'))
        assert cis.name == 'Cis Controls'
        assert cis.version is None
        assert cis.version_str == 'null'
        assert cis.get_points() == {'sub-point1', 'sub-point2'}
        assert h1.name == 'HIPAA'
        assert h1.version == 'v1'
        assert h1.get_points() == {'point1', 'sub-point1', 'point2'}
        assert h2.name == 'HIPAA'
        assert h2.version == 'v2'
        assert h2.get_points() == set()


def test_keep_highest():
    a = {1, 2, 3}
    b = {5}
    keep_highest(a, b)
    assert a == {1, 2, 3}
    assert b == {5}

    a = {1, 2, 3}
    b = {3, 5}
    keep_highest(a, b)
    assert a == {1, 2}
    assert b == {3, 5}

    a = {1, 2, 5}
    b = {2}
    c = {1, 5, 6, 7}
    keep_highest(a, b, c)
    assert a == set()
    assert b == {2}
    assert c == {1, 5, 6, 7}


def test_severity_cmp():
    assert severity_cmp('Info', 'Low') < 0
    assert severity_cmp('Medium', 'High') < 0

    assert severity_cmp('Low', 'Info') > 0
    assert severity_cmp('Medium', 'Low') > 0
    assert severity_cmp('High', 'Medium') > 0

    assert severity_cmp('Info', 'Info') == 0
    assert severity_cmp('Medium', 'Medium') == 0
    assert severity_cmp('High', 'High') == 0

    assert severity_cmp('High', 'Not existing') < 0
    assert severity_cmp('Not existing', 'High') > 0

    assert sorted(
        ['High', 'Medium', 'Info'], key=cmp_to_key(severity_cmp)
    ) == ['Info', 'Medium', 'High']
