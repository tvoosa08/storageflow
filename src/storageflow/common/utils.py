from datetime import datetime, timezone
from ulid import ULID

NOW_EPOCH = lambda: int(datetime.now(timezone.utc).timestamp())


def get_job_uid():
    return str(ULID.from_datetime(datetime.now(timezone.utc)))
