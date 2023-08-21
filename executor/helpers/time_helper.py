from datetime import datetime, timezone
from typing import Optional

from dateutil.parser import isoparse


def utc_datetime(_from: Optional[str] = None) -> datetime:
    """

    """
    obj = datetime.now(timezone.utc) if not _from else isoparse(_from)
    return obj.astimezone(timezone.utc)


def utc_iso(_from: Optional[datetime] = None) -> str:
    """

    """
    obj = _from or utc_datetime()
    return obj.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
