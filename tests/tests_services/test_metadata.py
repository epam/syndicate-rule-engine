import random
from datetime import date

import msgspec
import pytest

from helpers.constants import Severity
from services.metadata import (
    EMPTY_DOMAIN_METADATA,
    Deprecation,
    Metadata,
    merge_metadata,
)

from ..commons import mock_date_today


def test_merge_metadata(load_metadata):
    one = load_metadata('metrics_metadata', Metadata)
    two = load_metadata('test_collection_data_source', Metadata)
    three = Metadata(domains={'AZURE': EMPTY_DOMAIN_METADATA})
    result = merge_metadata(one, two, three)
    assert len(result.rules) == 13
    assert len(result.domains) == 3


def test_extended_domain_metadata_uses_named_coverage_fields():
    metadata = msgspec.convert(
        {
            'domains': {
                'AWS': [
                    {},
                    {
                        'Standard': {
                            'v1': {
                                '1.1': {
                                    'total_rules': 0,
                                    'control_title': 'Access control',
                                    'sections': [],
                                }
                            }
                        }
                    },
                ]
            }
        },
        type=Metadata,
    )

    domain = metadata.domain('AWS')
    standard = next(iter(domain.full_cov))
    assert domain.full_cov[standard]['1.1'].total_rules == 0
    assert domain.tech_cov == {}


def test_legacy_domain_metadata_is_rejected():
    with pytest.raises(msgspec.ValidationError):
        msgspec.convert(
            {'domains': {'AWS': [{}, {'Standard': {'v1': {'1.1': 1}}}]}},
            type=Metadata,
        )


class TestDeprecation:
    def test_deprecated(self):
        item = msgspec.convert(
            {
                'date': '2025-06-01',
                'is_deprecated': random.choice((True, False)),
            },
            type=Deprecation,
        )
        with mock_date_today(
            'services.metadata.datetime.date', date(2025, 7, 1)
        ):
            assert item.date == date(2025, 6, 1)
            assert item.is_deprecated
            assert not item.is_outdated
            assert item.severity is Severity.HIGH

        item = msgspec.convert({'is_deprecated': True}, type=Deprecation)
        assert not item.date
        assert item.is_deprecated
        assert not item.is_outdated
        assert item.severity is Severity.HIGH

    def test_outdated(self):
        item = msgspec.convert({'is_deprecated': False}, type=Deprecation)
        assert not item.date
        assert not item.is_deprecated
        assert item.is_outdated
        assert item.severity is Severity.INFO

    def test_deprecated_severity(self):
        item = msgspec.convert({'date': '2025-06-01'}, type=Deprecation)
        with mock_date_today(
            'services.metadata.datetime.date', date(2025, 6, 1)
        ):
            assert item.is_deprecated
            assert item.severity is Severity.HIGH

        item = msgspec.convert({'date': '2025-06-01'}, type=Deprecation)
        with mock_date_today(
            'services.metadata.datetime.date', date(2025, 5, 25)
        ):
            assert not item.is_deprecated
            assert item.severity is Severity.HIGH

        with mock_date_today(
            'services.metadata.datetime.date', date(2025, 4, 25)
        ):
            assert not item.is_deprecated
            assert item.severity is Severity.HIGH

        with mock_date_today(
            'services.metadata.datetime.date', date(2025, 3, 25)
        ):
            assert not item.is_deprecated
            assert item.severity is Severity.HIGH

        with mock_date_today(
            'services.metadata.datetime.date', date(2025, 3, 1)
        ):
            assert not item.is_deprecated
            assert item.severity is Severity.HIGH

        with mock_date_today(
            'services.metadata.datetime.date', date(2025, 2, 28)
        ):
            assert not item.is_deprecated
            assert item.severity is Severity.MEDIUM

        with mock_date_today(
            'services.metadata.datetime.date', date(2025, 2, 25)
        ):
            assert not item.is_deprecated
            assert item.severity is Severity.MEDIUM

        with mock_date_today(
            'services.metadata.datetime.date', date(2025, 1, 1)
        ):
            assert not item.is_deprecated
            assert item.severity is Severity.MEDIUM

        with mock_date_today(
            'services.metadata.datetime.date', date(2024, 11, 30)
        ):
            assert not item.is_deprecated
            assert item.severity is Severity.LOW
