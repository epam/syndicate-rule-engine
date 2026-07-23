"""
Click decorator: attach presigned-download hints to :class:`SREResponse`.
"""

from __future__ import annotations

import re
from functools import wraps
from typing import Any, Callable
from urllib.parse import parse_qs, unquote, urlparse

from srecli.service.adapter_client import HintType, SREResponse

_KNOWN_URL_KEYS: tuple[str, ...] = (
    'url',
    'dictionary_url',
    'meta_url',
)
_URL_KEY_TITLES: dict[str, str] = {
    'url': 'Report',
    'dictionary_url': 'Dictionary',
    'meta_url': 'Rules metadata',
}
_DEFAULT_TITLE = 'Download'
_COMBINED_HINT_TITLE = 'Download reports'
_MAX_INDIVIDUAL_HINTS = 3
_DISPOSITION_PARAMS = (
    'response-content-disposition',
    'ResponseContentDisposition',
)
_HTTP_PREFIXES = ('http://', 'https://')
# Report payload body — may contain resource attributes named ``url``.
_SKIP_NESTED_KEYS: frozenset[str] = frozenset({'content'})


def presigned_url_hints_pack(func: Callable) -> Callable:
    """After the command returns, scan the response for presigned URLs and set ``resp.hints``."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        resp = func(*args, **kwargs)
        if isinstance(resp, SREResponse):
            attach_presigned_hints_to_response(resp)
        return resp

    return wrapper


def _extract_presigned_urls(data: dict[str, Any] | None) -> list[str]:
    """
    Collect presigned report URLs from an API response body.

    Walks ``data``, nested envelopes, and ``items`` lists, but only collects
    known report fields (``url``, ``dictionary_url``, ``meta_url``). Does not
    descend into ``content`` (findings/resource payload), so nested keys like
    ``url`` there are ignored.
    """
    if not data:
        return []

    seen: set[str] = set()
    urls: list[str] = []

    def add(url: str) -> None:
        if not _is_http_url(url) or url in seen:
            return
        seen.add(url)
        urls.append(url)

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for key in _KNOWN_URL_KEYS:
                value = node.get(key)
                if isinstance(value, str):
                    add(value)
            for key, value in node.items():
                if key in _KNOWN_URL_KEYS or key in _SKIP_NESTED_KEYS:
                    continue
                if not isinstance(value, str):
                    walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(data)
    return urls


def _build_hints_from_urls(urls: list[str]) -> list[HintType]:
    """Build modular/CLI hint entries with curl + gunzip commands."""
    commands = [
        _download_description(url, _filename_from_url(url)) for url in urls
    ]
    if len(commands) <= _MAX_INDIVIDUAL_HINTS:
        return [
            {
                'index': index,
                'title': _title_for_index(index),
                'description': command,
            }
            for index, command in enumerate(commands)
        ]
    return [
        {
            'index': 0,
            'title': _COMBINED_HINT_TITLE,
            'description': '\n'.join(commands),
        }
    ]


def attach_presigned_hints_to_response(resp: SREResponse) -> SREResponse:
    if not resp.ok or not resp.data:
        return resp
    urls = _extract_presigned_urls(resp.data)
    if urls:
        resp.hints = _build_hints_from_urls(urls)
    return resp


def _is_http_url(value: str) -> bool:
    return value.startswith(_HTTP_PREFIXES)


def _gz_basename(url: str) -> str:
    path = urlparse(url).path.rstrip('/')
    if not path:
        return 'download.gz'
    name = path.rsplit('/', 1)[-1]
    return name or 'download.gz'


def _filename_from_url(url: str) -> str:
    query = parse_qs(urlparse(url).query, keep_blank_values=True)
    for param in _DISPOSITION_PARAMS:
        values = query.get(param)
        if not values:
            continue
        disp = unquote(values[0])
        match = re.search(r'filename="([^"]+)"', disp, re.IGNORECASE)
        if match:
            return match.group(1)
        match = re.search(r'filename=([^;\s]+)', disp, re.IGNORECASE)
        if match:
            return match.group(1).strip('"')
    return _gz_basename(url)


def _title_for_index(index: int) -> str:
    titles = tuple(_URL_KEY_TITLES[k] for k in _KNOWN_URL_KEYS)
    if index < len(titles):
        return titles[index]
    return _DEFAULT_TITLE


def _shell_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"


def _download_description(url: str, file_name: str) -> str:
    gz_name = _gz_basename(url)
    return (
        f'curl -fS -O {_shell_quote(url)} && '
        f'gunzip -c {_shell_quote(gz_name)} > {_shell_quote(file_name)}'
    )
