from datetime import datetime, timezone, date

from dateutil.parser import isoparse


def utc_datetime(_from: str | None = None, utc: bool = True) -> datetime:
    """
    Returns time-zone aware datetime object in UTC. You can optionally pass
    an existing ISO string. The function will parse it to object and make
    it UTC if it's not
    :params _from: Optional[str]
    :returns: datetime
    """
    obj = datetime.now(timezone.utc) if not _from else isoparse(_from)
    return obj.astimezone(timezone.utc) if utc else obj.astimezone()


def utc_iso(_from: datetime | None = None) -> str:
    """
    Returns time-zone aware datetime ISO string in UTC with military suffix.
    You can optionally pass datetime object. The function will make it
    UTC if it's not and serialize to string
    :param _from: Optional[datetime]
    :returns: str
    """
    obj = _from or utc_datetime()
    return obj.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')


def week_number(_from: datetime | date | None = None) -> int:
    return (_from.day - 1) // 7 + 1
