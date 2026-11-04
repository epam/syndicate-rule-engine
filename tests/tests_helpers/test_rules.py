import pytest

from helpers.rules import is_deprecated_rule_description


@pytest.mark.parametrize(
    ('description', 'expected'),
    [
        ('[DEPRECATED] This rule is no longer supported', True),
        ('  [DEPRECATED] This rule is no longer supported', True),
        ('This rule is deprecated', False),
        ('[deprecated] This rule is no longer supported', False),
        ('', False),
        (None, False),
    ],
)
def test_is_deprecated_rule_description(description, expected):
    assert is_deprecated_rule_description(description) is expected
