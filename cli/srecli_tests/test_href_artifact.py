"""Tests for presigned report download helper (no network)."""

from http import HTTPStatus
from pathlib import Path

import click
import gzip
import pytest

from srecli.service.adapter_client import SREResponse
from srecli.service.presigned_reports import download


def test_maybe_decompress_plain() -> None:
    assert download._maybe_decompress(b'not-gzip') == b'not-gzip'


def test_maybe_decompress_gzip() -> None:
    raw = b'{"ok": true}'
    assert download._maybe_decompress(gzip.compress(raw)) == raw


def test_resolve_main_path_file(tmp_path: Path) -> None:
    p = tmp_path / 'out.json'
    payload = {'job_id': 'j-1', 'format': 'json'}
    assert download._resolve_main_path(p, payload) == p


def test_resolve_main_path_dir(tmp_path: Path) -> None:
    payload = {'job_id': 'j-2', 'format': 'json'}
    got = download._resolve_main_path(tmp_path, payload)
    assert got == tmp_path / 'j-2.json'


def test_save_presigned_single_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture,
) -> None:
    def fake_fetch(url: str, *, label: str = 'Downloading') -> bytes:
        assert url == 'https://example.invalid/blob'
        assert isinstance(label, str)
        return gzip.compress(b'{"saved": true}')

    monkeypatch.setattr(download, '_fetch_presigned_bytes', fake_fetch)

    out = tmp_path / 'report.json'
    resp = SREResponse(
        data={
            'data': {
                'url': 'https://example.invalid/blob',
                'format': 'json',
                'job_id': 'j1',
            }
        },
        code=HTTPStatus.OK,
    )
    saved = download.save_presigned_payloads(resp, out)
    assert saved and saved[0] == out.resolve()
    assert out.read_bytes() == b'{"saved": true}'
    assert 'url' not in (resp.data or {}).get('data', {})
    captured = capsys.readouterr()
    assert 'Saved to' in captured.out
    assert 'report.json' in captured.out


def test_save_presigned_strips_urls_from_resp_after_download(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def fake_fetch(url: str, *, label: str = 'Downloading') -> bytes:
        del url, label
        return gzip.compress(b'{}')

    monkeypatch.setattr(download, '_fetch_presigned_bytes', fake_fetch)
    out = tmp_path / 'r.json'
    inner = {
        'url': 'https://example.invalid/x',
        'format': 'json',
        'job_id': 'j1',
    }
    resp = SREResponse(data={'data': inner}, code=HTTPStatus.OK)
    download.save_presigned_payloads(resp, out)
    assert 'url' not in resp.data['data']


def test_strip_presigned_urls_items() -> None:
    data = {
        'items': [
            {
                'job_id': 'a',
                'url': 'https://u1',
                'dictionary_url': 'https://d1',
                'meta_url': 'https://m1',
                'format': 'json',
            },
            {'job_id': 'b', 'url': 'https://u2'},
        ]
    }
    download.strip_presigned_urls_from_response_data(data)
    for it in data['items']:
        assert 'url' not in it
        assert 'dictionary_url' not in it
        assert 'meta_url' not in it


def test_save_presigned_root_level_url(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def fake_fetch(url: str, *, label: str = 'Downloading') -> bytes:
        assert url == 'https://example.invalid/root'
        del label
        return gzip.compress(b'{"root": true}')

    monkeypatch.setattr(download, '_fetch_presigned_bytes', fake_fetch)
    out = tmp_path / 'out.json'
    resp = SREResponse(
        data={
            'url': 'https://example.invalid/root',
            'format': 'json',
            'job_id': 'jr',
        },
        code=HTTPStatus.OK,
    )
    saved = download.save_presigned_payloads(resp, out)
    assert saved == [out.resolve()]
    assert out.read_bytes() == b'{"root": true}'
    assert 'url' not in resp.data


def test_save_presigned_sidecar_dictionary_and_meta(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    bodies = {
        'https://example.invalid/main': gzip.compress(b'{"main":1}'),
        'https://example.invalid/dict': gzip.compress(b'[1,2]'),
        'https://example.invalid/meta': gzip.compress(b'{"meta":true}'),
    }

    def fake_fetch(url: str, *, label: str = 'Downloading') -> bytes:
        del label
        return bodies[url]

    monkeypatch.setattr(download, '_fetch_presigned_bytes', fake_fetch)
    out = tmp_path / 'bundle.json'
    resp = SREResponse(
        data={
            'data': {
                'url': 'https://example.invalid/main',
                'dictionary_url': 'https://example.invalid/dict',
                'meta_url': 'https://example.invalid/meta',
                'format': 'json',
                'job_id': 'j99',
            }
        },
        code=HTTPStatus.OK,
    )
    saved = download.save_presigned_payloads(resp, out)
    assert len(saved) == 3
    assert out.read_bytes() == b'{"main":1}'
    dict_path = out.with_name(f'{out.stem}.dictionary.json')
    meta_path = out.with_name(f'{out.stem}.meta.json')
    assert dict_path.read_bytes() == b'[1,2]'
    assert meta_path.read_bytes() == b'{"meta":true}'
    inner = resp.data['data']
    assert 'dictionary_url' not in inner
    assert 'meta_url' not in inner


def test_save_presigned_items_unsupported_multi_report_to_directory(
    tmp_path: Path,
) -> None:
    """``items`` batch responses are rejected at save time (use CLI guidance)."""
    resp = SREResponse(
        data={
            'items': [
                {
                    'job_id': 'job-a',
                    'url': 'https://example.invalid/one',
                    'format': 'json',
                },
                {
                    'job_id': 'job-b',
                    'url': 'https://example.invalid/two',
                    'format': 'json',
                },
            ]
        },
        code=HTTPStatus.OK,
    )
    out_dir = tmp_path / 'reports'
    out_dir.mkdir()
    with pytest.raises(click.UsageError, match='Unsupported multi-report'):
        download.save_presigned_payloads(resp, out_dir)


def test_download_unsupported_pack_rejects_flag(tmp_path: Path) -> None:
    from click.testing import CliRunner

    from srecli.service.presigned_reports.click_packs import (
        download_unsupported_pack,
    )

    @click.command('jobs')
    @download_unsupported_pack
    def cmd() -> None:
        pass

    runner = CliRunner()
    result = runner.invoke(cmd, ['--download', str(tmp_path / 'out.json')])
    assert result.exit_code != 0
    assert 'not available' in result.output.lower()
