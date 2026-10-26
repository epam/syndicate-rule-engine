DEPRECATED_RULE_DESCRIPTION_PREFIX = '[DEPRECATED]'


def is_deprecated_rule_description(description: object) -> bool:
    return isinstance(description, str) and description.lstrip().startswith(
        DEPRECATED_RULE_DESCRIPTION_PREFIX
    )
