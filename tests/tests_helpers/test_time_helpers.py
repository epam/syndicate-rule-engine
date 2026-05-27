from helpers.time_helper import utc_datetime, utc_iso
from datetime import datetime, timezone, date


def test_utc_datetime():
    now = utc_datetime()
    assert now.tzinfo == timezone.utc
    assert now <= datetime.now(timezone.utc)

    assert utc_datetime('2024-11-21T11:41:53.203159+00:00') == datetime(
        year=2024,
        month=11,
        day=21,
        hour=11,
        minute=41,
        second=53,
        microsecond=203159,
        tzinfo=timezone.utc
    )
    assert utc_datetime('2024-10-25T11:41:53.203159Z') == datetime(
        year=2024,
        month=10,
        day=25,
        hour=11,
        minute=41,
        second=53,
        microsecond=203159,
        tzinfo=timezone.utc
    )

    assert utc_datetime('2024-10-25T01:41:53.203159+02:00') == datetime(
        year=2024,
        month=10,
        day=24,
        hour=23,
        minute=41,
        second=53,
        microsecond=203159,
        tzinfo=timezone.utc
    )


def test_utc_iso():
    assert utc_iso().endswith('Z')
    assert utc_datetime(utc_iso()).tzinfo == timezone.utc

    date_obj = date(
        year=2024,
        month=10,
        day=24,
    )
    assert utc_iso(date_obj) == '2024-10-24'


def test_utc_iso_utc_datetime():
    dt = datetime(
        year=2024,
        month=10,
        day=24,
        hour=23,
        minute=41,
        second=53,
        microsecond=203159,
        tzinfo=timezone.utc
    )
    assert utc_datetime(utc_iso(dt)) == dt
    assert utc_iso(utc_datetime('2024-10-25T11:41:53.203159Z')) == '2024-10-25T11:41:53.203159Z'
    assert utc_iso(utc_datetime('2024-10-25T01:41:53.203159+02:00')) == '2024-10-24T23:41:53.203159Z'

