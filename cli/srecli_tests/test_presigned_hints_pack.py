"""Tests for presigned URL hints."""

from http import HTTPStatus

from srecli.service.adapter_client import SREResponse
from srecli.service.hints import format_hints
from srecli.service.presigned_hints_pack import (
    _build_hints_from_urls,
    attach_presigned_hints_to_response,
)

_MAIN = (
    'http://127.0.0.1:9000/reports/on-demand/tmpmain.gz'
    '?response-content-disposition=attachment%3B%3Bfilename%3D%22report.json%22'
)


def _url(n: int) -> str:
    return (
        f'http://127.0.0.1:9000/reports/on-demand/tmp{n}.gz'
        f'?response-content-disposition=attachment%3B%3Bfilename%3D%22file{n}.json%22'
    )


def test_build_hints_few_urls_stay_separate():
    hints = _build_hints_from_urls([_MAIN, _url(2)])
    assert len(hints) == 2
    assert hints[0]['title'] == 'Report'


def test_build_hints_many_urls_combined():
    hints = _build_hints_from_urls([_url(i) for i in range(6)])
    assert len(hints) == 1
    assert hints[0]['title'] == 'Download reports'
    assert hints[0]['description'].count('curl -fS -O') == 6


def test_format_hints_multiline_description():
    text = format_hints([
        {
            'index': 0,
            'title': 'Download reports',
            'description': "curl -fS -O 'a'\ncurl -fS -O 'b'",
        },
    ])
    assert '    curl -fS -O' in text
    assert text.count('    curl -fS -O') == 2


def test_attach_presigned_hints_many_items_one_hint():
    resp = SREResponse(
        data={'items': [{'url': _url(i)} for i in range(5)]},
        code=HTTPStatus.OK,
    )
    attach_presigned_hints_to_response(resp)
    assert resp.hints is not None
    assert len(resp.hints) == 1
    assert resp.hints[0]['title'] == 'Download reports'
