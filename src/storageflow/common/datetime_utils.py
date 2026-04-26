from datetime import datetime, timezone


def date_to_microseconds_utc(date_str, format="%Y%m%d"):
    """
    Convert a date string in the specified format to microseconds since epoch in UTC.
    :param date_str: Date string
    :param format: Format of the date string
    :return: Microseconds since epoch in UTC
    """
    # Parse the input string as a UTC datetime object
    dt = datetime.strptime(date_str, format).replace(tzinfo=timezone.utc)
    # Convert to timestamp (seconds since epoch), then to microseconds
    microseconds = int(dt.timestamp() * 1_000_000)
    return microseconds
