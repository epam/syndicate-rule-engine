from unittest.mock import MagicMock

from services.metadata import Metadata
from services.report_delivery_service import ReportDeliveryService


def test_deprecated_rules_are_selected_by_description_marker():
    service = ReportDeliveryService(*(MagicMock() for _ in range(7)))
    meta = {
        'stable-rule-id': {
            'description': '[DEPRECATED] This rule is retired',
        },
        'legacy-deprecated-name': {
            'description': 'This rule used to be deprecated',
        },
    }

    rules = service._iter_deprecated_rules_dict(meta, Metadata())

    assert [rule['id'] for rule in rules] == ['stable-rule-id']
    assert rules[0]['description'] == '[DEPRECATED] This rule is retired'
