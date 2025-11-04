import datetime
from typing import Optional


def build_mensaje_id(uid: str, dt: Optional[datetime.datetime] = None) -> str:
    """Construye un ID de mensaje en formato UID_YYYY-MM-DDTHH:MM:SS.mmmmmm."""
    if dt is None:
        dt = datetime.datetime.now(datetime.timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    iso = dt.isoformat(timespec="microseconds")
    return f"{uid}_{iso}"
