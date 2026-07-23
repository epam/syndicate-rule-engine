"""Temporary helpers to persist and dump Cloud Custodian API response cache.

TODO: DELETE THIS ENTIRE FILE — temporary local debug for CC API cache dump.
"""

from __future__ import annotations

import json
import pickle
import sqlite3
import tempfile
from pathlib import Path
from typing import Any

from executor.helpers.constants import CACHE_FILE
from helpers.log_helper import get_logger

_LOG = get_logger(__name__)


def debug_dir_for_job(job_id: str) -> Path:
    # TODO: DELETE — temporary local debug for CC API cache dump
    path = Path(tempfile.gettempdir()) / 'sre-cc-debug' / job_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def cache_file_for_job(job_id: str) -> Path:
    # TODO: DELETE — temporary local debug for CC API cache dump
    return debug_dir_for_job(job_id) / CACHE_FILE


def dump_path_for_job(job_id: str) -> Path:
    # TODO: DELETE — temporary local debug for CC API cache dump
    return debug_dir_for_job(job_id) / 'cache_dump.json'


def _jsonable(value: Any) -> Any:
    # TODO: DELETE — temporary local debug for CC API cache dump
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, bytes):
        return value.decode('utf-8', errors='replace')
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    return repr(value)


def dump_cc_cache(cache_path: Path, dump_path: Path) -> int:
    """
    Dump SqlKvCache entries to JSON so API list responses can be inspected.

    Returns number of dumped entries.

    TODO: DELETE — temporary local debug for CC API cache dump
    """
    if not cache_path.exists():
        _LOG.warning('DEBUG: CC cache file does not exist: %s', cache_path)
        return 0

    conn = sqlite3.connect(str(cache_path))
    try:
        rows = conn.execute(
            'select key, value, create_date from c7n_cache'
        ).fetchall()
    finally:
        conn.close()

    entries: list[dict[str, Any]] = []
    for raw_key, raw_value, create_date in rows:
        try:
            key = pickle.loads(raw_key)
        except Exception as exc:
            key = f'<unpickleable key: {exc}>'
        try:
            value = pickle.loads(raw_value)
        except Exception as exc:
            value = f'<unpickleable value: {exc}>'

        resource_count = len(value) if isinstance(value, list) else None
        entries.append(
            {
                'create_date': create_date,
                'resource_count': resource_count,
                'key': _jsonable(key),
                'value': _jsonable(value),
            }
        )
        _LOG.info(
            'DEBUG: CC cache entry resources=%s key=%s',
            resource_count,
            _jsonable(key),
        )

    dump_path.write_text(
        json.dumps(entries, indent=2, ensure_ascii=False, default=str),
        encoding='utf-8',
    )
    _LOG.info(
        'DEBUG: dumped %s CC cache entries to %s (sqlite cache: %s)',
        len(entries),
        dump_path,
        cache_path,
    )
    return len(entries)
