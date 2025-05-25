# migration/utils/datetime_utils.py

import logging
from datetime import datetime, timezone
from typing import Optional, Union

# It's better to get the logger from the central setup if possible,
# but for a utility module, a local logger might be acceptable if it's lightweight.
# from core.logging_setup import get_logger
# logger = get_logger()
# For simplicity in this split, using a local, basic logger if detailed logging_setup isn't easily imported
_logger_dt_utils = logging.getLogger(__name__)


def iso_format_to_datetime(iso_str: Optional[str]) -> Optional[datetime]:
    """Converts an ISO format string (potentially with 'Z') to a UTC datetime object."""
    if not iso_str: return None
    try:
        if isinstance(iso_str, datetime): # If already a datetime object
            return iso_str.astimezone(timezone.utc) if iso_str.tzinfo else iso_str.replace(tzinfo=timezone.utc)

        # Normalize 'Z' to '+00:00' for wider compatibility with fromisoformat
        dt_str = iso_str.replace('Z', '+00:00')

        try:
            dt = datetime.fromisoformat(dt_str)
        except ValueError:
            # Attempt to fix microseconds if more than 6 digits (common issue)
            if '.' in dt_str:
                dt_part, _, micro_part_full = dt_str.rpartition('.')
                micro_part_no_tz = micro_part_full.split('+')[0].split('-')[0]
                micro_part_trimmed = micro_part_no_tz[:6]

                tz_suffix = ""
                if '+' in micro_part_full:
                    tz_suffix = '+' + micro_part_full.split('+', 1)[1]
                elif '-' in micro_part_full and not micro_part_full.startswith(micro_part_no_tz):
                    potential_tz_part = micro_part_full[len(micro_part_no_tz):]
                    if potential_tz_part.startswith("-"):
                         tz_suffix = potential_tz_part
                dt_str_corrected = f"{dt_part}.{micro_part_trimmed}{tz_suffix}"
                dt = datetime.fromisoformat(dt_str_corrected)
            else:
                raise
        return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError) as e:
        _logger_dt_utils.debug(f"Could not parse ISO string '{iso_str}' to datetime: {e}")
        return None

def datetime_to_iso_format(dt_obj: Optional[datetime]) -> Optional[str]:
    """Converts a datetime object to a UTC ISO 8601 string with 'Z'."""
    if not dt_obj: return None
    aware_dt = dt_obj.astimezone(timezone.utc) if dt_obj.tzinfo else dt_obj.replace(tzinfo=timezone.utc)
    return aware_dt.isoformat().replace('+00:00', 'Z')

def unix_timestamp_to_datetime(ts: Optional[Union[int, float]]) -> Optional[datetime]:
    """Converts a Unix timestamp to a UTC datetime object."""
    if ts is not None and isinstance(ts, (int, float)) and ts >= 0:
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except (OSError, OverflowError, ValueError) as e:
            _logger_dt_utils.debug(f"Invalid timestamp {ts} for datetime conversion: {e}")
            return None
    return None

def datetime_to_unix_timestamp(dt_obj: Optional[datetime]) -> Optional[int]:
    """Converts a datetime object to an integer Unix timestamp."""
    return int(dt_obj.timestamp()) if dt_obj else None
