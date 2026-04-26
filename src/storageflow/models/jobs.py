# jobs.py

from pydantic import (
    BaseModel,
    Field,
    PrivateAttr,
    field_serializer,
    field_validator,
    ConfigDict,
)
from typing import Annotated, List
from datetime import timedelta
import isodate
import re
from storageflow.models.clusters import ClusterConfigUnion
from enum import StrEnum
from typing import Literal

from pathlib import Path


class JobTypeEnum(StrEnum):
    DUMMY = "dummy"
    PYSPARK = "pyspark"


class ModeEnum(StrEnum):
    FILE = "file"
    FOLDER = "folder"
    MARKER = "marker"


class TaskConcurrencyResolutionEnum(StrEnum):
    SILENT_DISCARD = "silent_discard"
    CONFLICT = "conflict"
    TOO_MANY_REQUESTS = "too_many_requests"


def parse_iso_duration(value: str) -> timedelta:
    return isodate.parse_duration(value)


class ConcurrencyConfig(BaseModel):

    double_tap_window: Annotated[
        timedelta | None,
        Field(
            description="Reject events that are ingested inside this window as duplicates. ISO 8601 duration like 'PT30S'"
        ),
    ]

    cooldown_window: Annotated[
        timedelta | None,
        Field(
            description="Cooldown period after a task is run before accepting another task for the same file/folder/marker. ISO 8601 duration like 'PT30S'"
        ),
    ]

    ttl_buffer: Annotated[
        timedelta | None,
        Field(description="Buffer for task TTL. ISO 8601 duration like 'PT30S'"),
    ]

    schedule_delay: Annotated[
        timedelta,
        Field(
            description="Delay before scheduling the task. ISO 8601 duration like 'PT30S'"
        ),
    ]

    @field_validator("double_tap_window", mode="before")
    @classmethod
    def parse_double_tap_window(cls, v):
        return parse_iso_duration(v)

    @field_serializer("double_tap_window")
    @classmethod
    def serialize_double_tap_window(cls, td: timedelta):
        return isodate.duration_isoformat(td)

    @field_validator("cooldown_window", mode="before")
    @classmethod
    def parse_cooldown_window(cls, v):
        return parse_iso_duration(v)

    @field_serializer("cooldown_window")
    @classmethod
    def serialize_cooldown_window(cls, td: timedelta):
        return isodate.duration_isoformat(td)

    @field_validator("ttl_buffer", mode="before")
    @classmethod
    def parse_ttl_buffer(cls, v):
        return parse_iso_duration(v)

    @field_serializer("ttl_buffer")
    @classmethod
    def serialize_ttl_buffer(cls, td: timedelta):
        return isodate.duration_isoformat(td)

    @field_validator("schedule_delay", mode="before")
    @classmethod
    def parse_schedule_delay(cls, v):
        return parse_iso_duration(v)

    @field_serializer("schedule_delay")
    @classmethod
    def serialize_schedule_delay(cls, td: timedelta):
        return isodate.duration_isoformat(td)


class PysparkConfig(BaseModel):
    main_python_file_uri: str
    args: list[str] = Field(default_factory=list)
    jar_file_uris: list[str] | None = None
    python_file_uris: list[str] | None = None
    archive_uris: list[str] | None = None
    file_uris: list[str] | None = None
    properties: dict[str, str] | None = None

    _resolved_main_python_file_path: Path | None = PrivateAttr(default=None)
    _remote_main_python_file_path: str | None = PrivateAttr(default=None)

    model_config = ConfigDict(extra="forbid")  # Ensure no extra fields in PySparkConfig


class BaseJob(BaseModel):
    name: str
    source_bucket: str
    source_prefix: str
    service_account: str
    mode: ModeEnum
    concurrency_config: ConcurrencyConfig | None = None
    cluster_blueprint_name: str

    _compiled_source_prefix: re.Pattern = PrivateAttr()

    model_config = ConfigDict(extra="forbid")

    # Whether this job can be triggered automatically by events (GCS finalize, etc.)
    # If False, the job can only be executed manually via CLI
    # Defaults to True for backwards compatibility
    auto_trigger: bool = True

    @field_validator("source_prefix")
    @classmethod
    def validate_and_compile_regex(cls, v, info):
        try:
            compiled = re.compile(v)
            info.data["_compiled_source_prefix"] = compiled
        except re.error as e:
            raise ValueError(f"Invalid regex in source_prefix: {e}")
        return v


class PysparkJob(BaseJob):
    job_type: Literal[JobTypeEnum.PYSPARK] = JobTypeEnum.PYSPARK
    task_config: PysparkConfig


JobUnion = Annotated[PysparkJob, Field(discriminator="job_type")]


class ConfigModel(BaseModel):
    jobs: List[JobUnion] = Field(default_factory=list)
    clusters: List[ClusterConfigUnion] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid")
