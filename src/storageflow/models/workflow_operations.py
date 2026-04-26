from pydantic import BaseModel, Field
from typing import List


class Operation(BaseModel):
    operation_id: str | None = None
    done: bool | None = None


class Node(BaseModel):
    step_id: str | None = None
    job_id: str | None = None
    state: str | None = None
    error: str | None = None


class Graph(BaseModel):
    nodes: List[Node] = Field(default_factory=list)


class Time(BaseModel):
    seconds: int | None = None
    nanos: int | None = None


class WorkflowOperation(BaseModel):
    create_cluster: Operation | None = None
    graph: Graph | None = None
    delete_cluster: Operation | None = None
    state: str | None = None
    cluster_name: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    cluster_uuid: str | None = None
