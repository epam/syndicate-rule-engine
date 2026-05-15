"""
Download presigned report artifacts (gzip-compressed JSON/XLSX from S3) and
write decompressed bytes to disk.
"""

from __future__ import annotations

import gzip
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, BinaryIO
from urllib.error import URLError
from urllib.request import Request, urlopen

import click

from srecli.service.constants import DATA_ATTR, ITEMS_ATTR

if TYPE_CHECKING:
    from srecli.service.adapter_client import SREResponse


_GZIP_MAGIC = b'\x1f\x8b'
_PRESIGNED_URL_KEYS = ('url', 'dictionary_url', 'meta_url')
_CLEAR_TO_EOL = '\033[K'
_PROGRESS_WIDTH = 40


def save_presigned_payloads(
    resp: SREResponse,
    output: str | Path,
) -> list[Path]:
    """
    If *resp* is OK and the body contains presigned URL(s), download and write
    decompressed file(s), then remove presigned URL fields from *resp.data* for
    subsequent CLI output.
    """
    output_path = Path(output)
    if not resp.ok or not resp.data:
        return []

    data = resp.data
    if not isinstance(data, dict):
        return []

    inner = _unwrap_single_payload(data)
    if inner is not None:
        url = inner['url']
        main_path = _resolve_main_path(output_path, inner)
        try:
            saved = _save_one_url(
                url,
                main_path,
                dictionary_url=_optional_str_url(inner, 'dictionary_url'),
                meta_url=_optional_str_url(inner, 'meta_url'),
            )
        except URLError as e:
            raise click.ClickException(
                f'Failed to download report: {e}'
            ) from e
        _echo_saved_paths(saved)
        strip_presigned_urls_from_response_data(resp.data)
        return saved

    items = data.get(ITEMS_ATTR)
    if isinstance(items, list) and items:
        raise click.UsageError('Unsupported multi-report responses.')

    raise click.UsageError('Response has no presigned url to download.')


def strip_presigned_urls_from_response_data(data: dict[str, Any]) -> None:
    """Drop presigned URL fields from API *data* so CLI table/JSON omit them."""
    inner = data.get(DATA_ATTR)
    if isinstance(inner, dict):
        for k in _PRESIGNED_URL_KEYS:
            inner.pop(k, None)
    for k in _PRESIGNED_URL_KEYS:
        data.pop(k, None)
    items = data.get(ITEMS_ATTR)
    if isinstance(items, list):
        for it in items:
            if isinstance(it, dict):
                for k in _PRESIGNED_URL_KEYS:
                    it.pop(k, None)


def _optional_str_url(d: dict[str, Any], key: str) -> str | None:
    v = d.get(key)
    return v if isinstance(v, str) else None


def _human_bytes(n: int) -> str:
    if n < 1024:
        return f'{n} B'
    if n < 1024 * 1024:
        return f'{n / 1024:.1f} KB'
    if n < 1024**3:
        return f'{n / (1024 * 1024):.1f} MB'
    return f'{n / (1024**3):.1f} GB'


def _echo_saved_paths(saved: list[Path]) -> None:
    for path in saved:
        try:
            size = path.stat().st_size
        except OSError:
            size = 0
        click.secho(
            f'✔ Saved to {path} ({_human_bytes(size)})',
            fg='green',
        )
    click.echo()


def _maybe_decompress(body: bytes) -> bytes:
    if len(body) >= 2 and body[:2] == _GZIP_MAGIC:
        return gzip.decompress(body)
    return body


def _read_body_with_progress(
    resp: BinaryIO,
    *,
    label: str,
    chunk_size: int = 65536,
) -> bytes:
    """Read HTTP body with a live progress line on stderr when attached to a TTY."""
    hdrs = getattr(resp, 'headers', None)
    raw_cl: str | None = (
        hdrs.get('Content-Length') if hdrs is not None else None
    )
    total: int | None = None
    if raw_cl is not None:
        cl = str(raw_cl).strip()
        if cl.isdigit():
            total = int(cl)

    out = sys.stderr
    tty = out.isatty()
    buf = bytearray()
    pos = 0
    width = _PROGRESS_WIDTH

    while True:
        chunk = resp.read(chunk_size)
        if not chunk:
            break
        buf.extend(chunk)
        pos += len(chunk)
        if not tty:
            continue
        if total and total > 0:
            pct = min(100.0, 100.0 * pos / total)
            filled = min(width, int(width * pos / total))
            bar = '━' * filled + '╺' * (width - filled)
            line = (
                f'{label} {bar} {pct:3.0f}% '
                f'{_human_bytes(pos)}/{_human_bytes(total)}'
            )
            out.write(f'\r{line}{_CLEAR_TO_EOL}')
            out.flush()
        else:
            out.write(f'\r{label} … {_human_bytes(pos)}{_CLEAR_TO_EOL}')
            out.flush()

    if tty and pos:
        out.write('\n')
        out.flush()
    return bytes(buf)


def _fetch_presigned_bytes(url: str, *, label: str = 'Downloading') -> bytes:
    req = Request(url, method='GET')
    with urlopen(req, timeout=300) as resp:
        return _read_body_with_progress(resp, label=label)


def _extension_for_report_format(fmt: str | None) -> str:
    if fmt and str(fmt).lower() == 'xlsx':
        return '.xlsx'
    return '.json'


def _default_stem(payload: dict[str, Any]) -> str:
    if job_id := payload.get('job_id'):
        return str(job_id)
    if tenant := payload.get('tenant_name'):
        return str(tenant)
    if platform_id := payload.get('platform_id'):
        return str(platform_id)
    return 'report'


def _resolve_main_path(output: Path, payload: dict[str, Any]) -> Path:
    fmt = payload.get('format')
    if isinstance(fmt, str):
        ext = _extension_for_report_format(fmt)
    else:
        ext = '.json'
    if output.is_dir():
        return output / f'{_default_stem(payload)}{ext}'
    return output


def _write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def _save_one_url(
    url: str,
    main_path: Path,
    *,
    dictionary_url: str | None = None,
    meta_url: str | None = None,
) -> list[Path]:
    saved: list[Path] = []
    body = _fetch_presigned_bytes(url)
    content = _maybe_decompress(body)
    _write_bytes(main_path, content)
    saved.append(main_path.resolve())

    if dictionary_url:
        dpath = main_path.with_name(f'{main_path.stem}.dictionary.json')
        dbody = _maybe_decompress(
            _fetch_presigned_bytes(
                dictionary_url, label='Downloading dictionary'
            )
        )
        _write_bytes(dpath, dbody)
        saved.append(dpath.resolve())

    if meta_url:
        mpath = main_path.with_name(f'{main_path.stem}.meta.json')
        mbody = _maybe_decompress(
            _fetch_presigned_bytes(meta_url, label='Downloading meta')
        )
        _write_bytes(mpath, mbody)
        saved.append(mpath.resolve())

    return saved


def _unwrap_single_payload(data: dict[str, Any]) -> dict[str, Any] | None:
    inner = data.get(DATA_ATTR)
    if isinstance(inner, dict) and isinstance(inner.get('url'), str):
        return inner
    if isinstance(data.get('url'), str):
        return data
    return None
