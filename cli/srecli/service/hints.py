"""Utilities for rendering presigned-report hints in CLI output."""

from __future__ import annotations

from srecli.service.adapter_client import HintType


def format_hints(hints: list[HintType]) -> str:
    """Render hints block for terminal output."""
    ordered = sorted(hints, key=lambda h: h.get('index', 0))
    lines = ['Hints:']
    for hint in ordered:
        lines.append(f'  {hint.get("title", "")}:')
        description = hint.get('description', '')
        if '\n' in description:
            for command_line in description.splitlines():
                lines.append(f'    {command_line}')
        else:
            lines.append(f'    {description}')
        lines.append('')
    while lines and lines[-1] == '':
        lines.pop()
    return '\n'.join(lines)
