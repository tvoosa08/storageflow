from json import JSONEncoder
from datetime import datetime, timedelta, timezone
from typing import Optional, Literal
import ulid
import hashlib
from google.cloud import firestore

from google.cloud import tasks_v2 as tasks

from storageflow.common.env_manager import get_mandatory_env_var

from storageflow.common.logging import get_logger, run_uuid_var
from storageflow.models.jobs import JobUnion, ModeEnum as Mode
from storageflow.models.payloads import CloudEvent

from storageflow.models.payloads import TaskPayload


# Configurable defaults
DEFAULT_DOUBLE_TAP_WINDOW = timedelta(seconds=5)
DEFAULT_COOLDOWN_WINDOW = timedelta(minutes=30)
DEFAULT_TTL_BUFFER = timedelta(hours=12)
DEFAULT_SCHEDULE_DELAY = timedelta(minutes=5)

FIRESTORE_DB = get_mandatory_env_var("FIRESTORE_DB")
FIRESTORE_DOCUMENT_DEDUP = get_mandatory_env_var("FIRESTORE_DOCUMENT_DEDUP")

QUEUE_DISPATCH_DEADLINE = "1200s"  # get_mandatory_env_var("QUEUE_DISPATCH_DEADLINE")

PROJECT_ID = get_mandatory_env_var("PROJECT_ID")
QUEUE_LOCATION = get_mandatory_env_var("QUEUE_LOCATION")
QUEUE_NAME = get_mandatory_env_var("QUEUE_NAME")

HEADERS_RUN_UUID_NAME = get_mandatory_env_var("HEADERS_RUN_UUID_NAME")

LOGGING_LEVEL = get_mandatory_env_var("LOGGING_LEVEL") or "DEBUG"

log = get_logger(level=LOGGING_LEVEL)


db = firestore.Client(project=PROJECT_ID, database=FIRESTORE_DB)
tasks_col = db.collection(FIRESTORE_DOCUMENT_DEDUP)

tasks_client = tasks.CloudTasksClient()


class CustomEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)


def generate_content_hash(
    *,
    bucket: str,
    prefix: str,
    mode: Mode,
    file_name: Optional[str] = None,
    generation: Optional[str] = None,
    md5_hash: Optional[str] = None,
    crc32c: Optional[str] = None,
    manual_hint: Optional[str] = None,
) -> str:
    parts = [bucket, prefix]

    match mode:
        case "marker":
            parts.append(file_name or "_UNKNOWN_MARKER")
            parts.append(generation or "")
            if manual_hint:
                parts.append(manual_hint)

        case "file":
            parts.append(file_name or "_UNKNOWN_FILE")
            hash_val = md5_hash or crc32c or generation or manual_hint
            parts.append(hash_val or "_NO_HASH")

        case "folder":
            if manual_hint:
                parts.append(manual_hint)

        case _:
            raise ValueError(f"Unknown mode: {mode}")

    raw = "::".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def content_changed(
    last: dict, event_uid: Optional[str], content_hash: Optional[str]
) -> bool:
    if event_uid and last.get("event_uid") != event_uid:
        return True
    if content_hash and last.get("content_hash") != content_hash:
        return True
    return False


def respond(status: str, doc: dict):
    log.info(
        "task response",
        extra={
            "status": status,
            "task_key": doc.get("task_key"),
            "source": doc.get("source"),
            "scheduled_at": str(doc.get("scheduled_at")),
        },
    )
    return {
        "status": status,
        "task": doc,
    }


def delete_expired_tasks(older_than: timedelta = timedelta(days=2)):
    threshold = datetime.now(timezone.utc) - older_than
    query = tasks_col.where("created_at", "<", threshold).limit(500)
    deleted = 0

    for doc in query.stream():
        log.info(
            "deleting expired tasks",
            extra={
                "message": "deleting_expired_task",
                "task_id": doc.id,
                "created_at": str(doc.get("created_at")),
            },
        )
        doc.reference.delete()
        deleted += 1

    log.info(
        "expired_task_cleanup_complete",
        extra={"deleted_count": deleted},
    )


def enqueue_cloud_task(task_id: str, run_at: datetime, payload: dict):
    body = TaskPayload(**payload).model_dump_json().encode("utf-8")

    task = tasks.Task(
        http_request=tasks.HttpRequest(
            http_method=tasks.HttpMethod.POST,
            url="https://localhost",  # dummy url used. override in the run task. see cloud_run.tf
            body=body,
            headers={
                HEADERS_RUN_UUID_NAME: run_uuid_var.get(),
            },
        ),
        schedule_time=run_at,
        dispatch_deadline=QUEUE_DISPATCH_DEADLINE,
    )

    tasks_client.create_task(
        tasks.CreateTaskRequest(
            parent=tasks_client.queue_path(PROJECT_ID, QUEUE_LOCATION, QUEUE_NAME),
            task=task,
        )
    )


def admit_task(
    *,
    task_key: str,
    cloud_event: CloudEvent,
    job_uid: str,
    source: Literal["eventarc", "manual"],
    schedule_delay: timedelta,
    event_uid: Optional[str] = None,
    content_hash: Optional[str] = None,
    force_run: bool = False,
    double_tap_window: Optional[timedelta] = None,
    cooldown_window: Optional[timedelta] = None,
    ttl_buffer: Optional[timedelta] = None,
):
    now = datetime.now(timezone.utc)
    dt_window = double_tap_window or DEFAULT_DOUBLE_TAP_WINDOW
    cd_window = cooldown_window or DEFAULT_COOLDOWN_WINDOW
    ttl = ttl_buffer or DEFAULT_TTL_BUFFER
    window_start = now - cd_window

    @firestore.transactional
    def txn(transaction):
        q = (
            tasks_col.where("task_key", "==", task_key)
            .where("created_at", ">=", window_start)
            .order_by("created_at", direction=firestore.Query.DESCENDING)
            .limit(1)
        )
        docs = list(q.get(transaction=transaction))
        last = docs[0].to_dict() if docs else None

        if last and event_uid and last.get("event_uid") == event_uid:
            return respond("duplicate_event", last)

        if last and now < last["created_at"] + dt_window:
            return respond("double_tap", last)

        if last and now < last["scheduled_at"] and not force_run:
            return respond("pending", last)

        if last and now < last["cooldown_until"]:
            if content_changed(last, event_uid, content_hash) or force_run:
                pass
            else:
                return respond("cooldown", last)

        # Accept new task
        task_id = str(ulid.ULID())
        scheduled_at = now if force_run else now + schedule_delay
        cooldown_until = max(now + dt_window + cd_window, scheduled_at)

        new_doc = {
            "task_key": task_key,
            "source": source,
            "event_uid": event_uid,
            "content_hash": content_hash,
            "created_at": now,
            "scheduled_at": scheduled_at,
            "cooldown_until": cooldown_until,
            "expire_at": cooldown_until + ttl,
            "bucket": cloud_event.bucket,
            "name": cloud_event.name,
            "job_uid": job_uid,
        }

        log.debug(
            "accepting_task",
            extra={
                "task_id": task_id,
                "task_key": task_key,
                "source": source,
                "scheduled_at": scheduled_at.isoformat(),
                "cooldown_until": cooldown_until.isoformat(),
            },
        )

        transaction.set(tasks_col.document(task_id), new_doc)
        enqueue_cloud_task(task_id, run_at=scheduled_at, payload=new_doc)
        return respond("accepted", new_doc)

    transaction = db.transaction()
    return txn(transaction)


def create_task(*, job: JobUnion, event: CloudEvent, job_uid: str):

    task_key = f"{event.bucket}/{event.name}"
    source = "manual" if event.is_manual else "eventarc"
    event_uid = str(ulid.ULID())
    content_hash = generate_content_hash(
        bucket=event.bucket,
        prefix=event.name,
        mode=job.mode,
        file_name=event.name,
        generation=event.generation,
        md5_hash=event.md5_hash,
        crc32c=event.crc32c,
        manual_hint=event.manual_hint,
    )
    if job.concurrency_config is None:
        schedule_delay = DEFAULT_SCHEDULE_DELAY
        cooldown_window = DEFAULT_COOLDOWN_WINDOW
        ttl_buffer = DEFAULT_TTL_BUFFER
        double_tap_window = DEFAULT_DOUBLE_TAP_WINDOW
    else:
        schedule_delay = job.concurrency_config.schedule_delay or DEFAULT_SCHEDULE_DELAY
        cooldown_window = (
            job.concurrency_config.cooldown_window or DEFAULT_COOLDOWN_WINDOW
        )
        ttl_buffer = job.concurrency_config.ttl_buffer or DEFAULT_TTL_BUFFER
        double_tap_window = (
            job.concurrency_config.double_tap_window or DEFAULT_DOUBLE_TAP_WINDOW
        )

    response = admit_task(
        job_uid=job_uid,
        cloud_event=event,
        task_key=task_key,
        source=source,
        schedule_delay=schedule_delay,
        event_uid=event_uid,
        content_hash=content_hash,
        force_run=event.force_run,
        double_tap_window=double_tap_window,
        cooldown_window=cooldown_window,
        ttl_buffer=ttl_buffer,
    )

    log.debug("task admitted")

    return response
