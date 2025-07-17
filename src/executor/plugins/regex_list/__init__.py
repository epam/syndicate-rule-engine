"""
https://github.com/cloud-custodian/cloud-custodian/pull/9608
"""
import re


def _regex_match(value, regex, flags=0):
    """
    This is new regex match function that can be used in value filters.
    """
    if isinstance(value, list):
        for item in value:
            if not isinstance(item, str):
                continue
            if re.match(regex, item, flags=flags):
                return True
    elif isinstance(value, str):
        # Note python 2.5+ internally cache regex
        # would be nice to use re2
        return bool(re.match(regex, value, flags=flags))
    return False


def regex_match(value, regex):
    return _regex_match(value, regex, flags=re.IGNORECASE)


def regex_case_sensitive_match(value, regex):
    return _regex_match(value, regex, flags=0)


def register() -> None:
    """
    changes `regex` operation for value filters. Now it can be matched over a list of items:
    """
    from c7n.filters.core import OPERATORS
    OPERATORS['regex'] = regex_match
    OPERATORS['regex-case'] = regex_case_sensitive_match

