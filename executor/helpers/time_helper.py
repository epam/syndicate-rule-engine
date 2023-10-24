from datetime import datetime, timezone
from typing import Optional

from dateutil.parser import isoparse


def utc_datetime(_from: Optional[str] = None) -> datetime:
    """
    Returns time-zone aware datetime object in UTC. You can optionally pass
    an existing ISO string. The function will parse it to object and make
    it UTC if it's not
    :params _from: Optional[str]
    :returns: datetime
    """
    obj = datetime.now(timezone.utc) if not _from else isoparse(_from)
    return obj.astimezone(timezone.utc)


def utc_iso(_from: Optional[datetime] = None) -> str:
    """
    Returns time-zone aware datetime ISO string in UTC with military suffix.
    You can optionally pass datetime object. The function will make it
    UTC if it's not and serialize to string
    :param _from: Optional[datetime]
    :returns: str
    """
    obj = _from or utc_datetime()
    return obj.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
