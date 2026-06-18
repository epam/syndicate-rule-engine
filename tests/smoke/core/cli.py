from smoke.core.settings import get_settings


def cmd(command: str, customer: str | None = None) -> str:
    """Prefix a CLI sub-command with the configured entry point."""
    entrypoint = get_settings().cli_entrypoint
    suffix = f' -cid {customer}' if customer else ''
    return f'{entrypoint} {command}{suffix}'
