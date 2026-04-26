# payloads

from typing import Dict, Any, List
from pydantic import BaseModel, Field, ConfigDict
from enum import Enum

from datetime import datetime

from storageflow.models.workflow_operations import WorkflowOperation


class CloudEvent(BaseModel):
    bucket: str
    name: str
    generation: str | None = None
    metageneration: str | None = None
    time_created: str | None = None
    md5_hash: str | None = None
    crc32c: str | None = None
    etag: str | None = None
    media_link: str | None = None
    self_link: str | None = None
    kind: str | None = None
    manual_hint: str | None = None
    force_run: bool = False

    @property
    def is_manual(self) -> bool:
        # If generation is missing, it's likely a manually created event
        return self.generation is None

    @property
    def event_uid(self) -> str:
        return f"{self.bucket}/{self.name}/{self.generation}"

    @property
    def task_key(self) -> str:
        return f"{self.bucket}/{self.name.rsplit('/', 1)[0]}"


class RuntimeMode(str, Enum):
    UNDEFINED = "undefined"
    CLOUD = "cloud"
    CLI = "cli"


class ClusterLabelsConfig(BaseModel):
    """
    Identify an existing cluster by a set of labels.
    """

    model_config = ConfigDict(extra="forbid")

    labels: Dict[str, str] = Field(
        ..., description="Labels to match an existing cluster."
    )


class ClusterJsonConfig(BaseModel):
    """
    Define a new cluster configuration in Dataproc Cluster API format.
    """

    model_config = ConfigDict(extra="forbid")

    project_id: str
    cluster_name: str
    cluster_config: Dict[str, Any]


class JobRunResponseStatus(str, Enum):
    ACCEPTED = "accepted"
    IGNORED = "ignored"


class JobRunResponse(BaseModel):
    run_uuid: str = Field(..., description="Unique identifier for the submitted job.")
    status: JobRunResponseStatus = Field(..., description="Status of the job run.")


class TaskPayload(BaseModel):
    """
    Payload for a task to be processed by the task manager.
    """

    model_config = ConfigDict(extra="forbid")
    task_key: str
    source: str
    event_uid: str
    content_hash: str
    created_at: datetime
    scheduled_at: datetime
    cooldown_until: datetime
    expire_at: datetime
    bucket: str
    name: str
    job_uid: str


class DocumentBodyPayload(BaseModel):
    """
    Payload for the document body to be stored in Firestore.
    """

    model_config = ConfigDict(extra="forbid")
    uuid: str
    event: CloudEvent
    created_at: Any


class WorkflowStatusResponse(BaseModel):
    """
    Response model for workflow status.
    """

    model_config = ConfigDict(extra="forbid")

    workflow_operation: WorkflowOperation
    is_completed: bool = False
    is_successful: bool | None = None


class ControllerWorkflowRequest(BaseModel):
    max_retries: int
    operation_id: str
    poll_interval_seconds: int
    run_uuid: str
