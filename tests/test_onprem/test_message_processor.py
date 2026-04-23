import json
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from onprem.event_sources_consumer.connectors.base import Message
from onprem.event_sources_consumer.message_processor import (
    _normalize_to_events,
    process_message,
)


def test_normalize_list_of_dicts_filters_non_dicts():
    body = [{"a": 1}, {"b": 2}, "skip", 3, None, []]
    assert _normalize_to_events(body) == [{"a": 1}, {"b": 2}]


def test_normalize_empty_list():
    assert _normalize_to_events([]) == []


def test_normalize_list_no_dicts():
    assert _normalize_to_events(["x", 1, None]) == []


def test_normalize_dict_with_events_key_filters_non_dicts():
    body = {"events": [{"x": 1}, "bad", {"y": 2}]}
    assert _normalize_to_events(body) == [{"x": 1}, {"y": 2}]


def test_normalize_dict_with_empty_events_list():
    assert _normalize_to_events({"events": []}) == []


def test_normalize_dict_events_key_takes_precedence_over_detail():
    body = {"events": [{"a": 1}], "detail": {"k": 1}}
    assert _normalize_to_events(body) == [{"a": 1}]


def test_normalize_dict_with_detail_wrapped_full_event():
    body = {"detail": {"k": 1}, "source": "s"}
    assert _normalize_to_events(body) == [body]


def test_normalize_plain_dict_without_events_or_detail():
    body = {"id": 1}
    assert _normalize_to_events(body) == [body]


def test_normalize_plain_dict_empty():
    assert _normalize_to_events({}) == [{}]


def test_normalize_dict_events_string_values_yield_empty():
    """Iterating a str gives chars; none are dicts."""
    assert _normalize_to_events({"events": "ab"}) == []


def test_normalize_dict_events_dict_iterates_keys_not_dicts():
    assert _normalize_to_events({"events": {"k": "v"}}) == []


def test_normalize_non_dict_non_list_returns_empty():
    assert _normalize_to_events(None) == []  # type: ignore[arg-type]
    assert _normalize_to_events(42) == []  # type: ignore[arg-type]


def test_normalize_events_key_none_raises_type_error():
    """`get('events', [])` returns None when key is present with null value."""
    with pytest.raises(TypeError):
        _normalize_to_events({"events": None})


def test_process_message_dict_body_calls_ingest():
    svc = MagicMock()
    process_message(Message(message_id="m1", body={"x": 1}), svc)
    svc.ingest.assert_called_once_with(raw_events=[{"x": 1}], vendor=None)


def test_process_message_empty_normalize_skips_ingest():
    svc = MagicMock()
    # Message.body is typed str|bytes|dict; list bodies still occur after JSON decode.
    process_message(Message(message_id="m1", body=cast(Any, [])), svc)
    svc.ingest.assert_not_called()


def test_process_message_str_json_array():
    svc = MagicMock()
    process_message(Message(message_id="m1", body='[{"a": 1}]'), svc)
    svc.ingest.assert_called_once_with(raw_events=[{"a": 1}], vendor=None)


def test_process_message_bytes_json_object():
    svc = MagicMock()
    process_message(
        Message(message_id="m1", body=b'{"detail": {}, "source": "s"}'),
        svc,
    )
    svc.ingest.assert_called_once()
    args, kwargs = svc.ingest.call_args
    assert kwargs == {"raw_events": [{"detail": {}, "source": "s"}], "vendor": None}


def test_process_message_invalid_json_raises():
    svc = MagicMock()
    with pytest.raises(json.JSONDecodeError):
        process_message(Message(message_id="m1", body="not-json{"), svc)
    svc.ingest.assert_not_called()
