from datetime import datetime, timezone
from time import time, sleep
from typing import Optional, Union

from dateutil.parser import isoparse


def utc_datetime(_from: Optional[str] = None, utc: bool = True) -> datetime:
    """
    Returns time-zone aware datetime object in UTC. You can optionally pass
    an existing ISO string. The function will parse it to object and make
    it UTC if it's not
    :params _from: Optional[str]
    :returns: datetime
    """
    obj = datetime.now(timezone.utc) if not _from else isoparse(_from)
    return obj.astimezone(timezone.utc) if utc else obj.astimezone()


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


def ts_datetime(_from: Optional[float] = None):
    return datetime.fromtimestamp(_from or time(), timezone.utc)


def ts_from_iso(_from: Optional[str] = None) -> str:
    return str((utc_datetime(_from) if _from else ts_datetime()).timestamp())


def wait(seconds: int):
    sleep(seconds)


def make_timestamp_java_compatible(timestamp: Union[int, float]) -> int:
    return round(timestamp * 1000)
