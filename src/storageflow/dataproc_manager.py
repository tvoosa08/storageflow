import copy
import json
from importlib import resources

from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage
from google.cloud.dataproc_v1.types import WorkflowMetadata
from google.longrunning.operations_pb2 import GetOperationRequest
from google.protobuf.json_format import MessageToDict

from storageflow.common.env_manager import get_mandatory_env_var
from storageflow.common.logging import get_logger, run_uuid_var
from storageflow.common.rendering import (
    InlineWorkflowTemplateRenderer,
    render_jinja_in_list,
)
from storageflow.common.storage import is_gcs_folder, strip_filename, upload_local_file
from storageflow.config_manager import ConfigManager
from storageflow.models.jobs import BaseJob, ModeEnum
from storageflow.models.payloads import ControllerWorkflowRequest, WorkflowStatusResponse
from storageflow.models.workflow_operations import WorkflowOperation

LOGGING_LEVEL = get_mandatory_env_var("LOGGING_LEVEL", "DEBUG")
WORKFLOW_NAME = get_mandatory_env_var("WORKFLOW_NAME")
WORKFLOW_LOCATION = get_mandatory_env_var("WORKFLOW_LOCATION")
PROJECT_ID = get_mandatory_env_var("PROJECT_ID")

WORKFLOW_MAX_RETRIES = get_mandatory_env_var("WORKFLOW_MAX_RETRIES")
WORKFLOW_POLL_INTERVAL_SECONDS = get_mandatory_env_var("WORKFLOW_POLL_INTERVAL_SECONDS")


log = get_logger(level=LOGGING_LEVEL)


def add_or_update_run_uuid(args):
    """
    Adds or updates the --run_uuid argument in a list of arguments.

    Args:
      args: A list of strings representing command-line arguments.

    Returns:
      A new list of strings with the --run_uuid argument added or updated.
    """
    new_args = [arg for arg in args if not arg.startswith("--run_uuid=")]
    new_args.append(f"--run_uuid={run_uuid_var.get()}")
    return new_args


async def run_controller_workflow(*, run_uuid: str, operation_id: str):
    from google.cloud.workflows.executions_v1 import ExecutionsClient
    from google.cloud.workflows.executions_v1.types import Execution

    wf_client = ExecutionsClient()
    parent = (
        f"projects/{PROJECT_ID}/locations/{WORKFLOW_LOCATION}/workflows/{WORKFLOW_NAME}"
    )

    payload = ControllerWorkflowRequest(
        max_retries=int(WORKFLOW_MAX_RETRIES),
        run_uuid=run_uuid,
        poll_interval_seconds=int(WORKFLOW_POLL_INTERVAL_SECONDS),
        operation_id=operation_id,
    )

    response = wf_client.create_execution(
        request={
            "parent": parent,
            "execution": Execution(argument=payload.model_dump_json()),
        }
    )

    return response.name


async def handle_run(
    *,
    bucket: str,
    name: str,
    config: ConfigManager,
    job: BaseJob | None = None,
    dry_run: bool = False,
    skip_controller_workflow: bool = False
) -> str:

    PROJECT_ID = get_mandatory_env_var("PROJECT_ID")
    DATAPROC_REGION = get_mandatory_env_var("DATAPROC_REGION")
    GCS_TEMP_BUCKET = get_mandatory_env_var("GCS_TEMP_BUCKET")

    # If job not provided, select it based on bucket and name
    if job is None:
        log.debug("No job provided, selecting by bucket and name")
        original_job = config.find_job_by_bucket_and_name(bucket, name)
        assert original_job is not None, f"Job not found for {bucket}/{name}"
        job = copy.deepcopy(original_job)
    else:
        # Job was pre-selected (e.g., by --job_name in CLI)
        log.debug("Using pre-selected job: %s", job.name)
        job = copy.deepcopy(job)

    log.info(
        "requested execution of job",
        extra={"job": job.name, "bucket": bucket, "prefix": name},
    )
    cluster = config.get_cluster_for_job(job)
    log.info(
        "using cluster",
        extra={
            "cluster": cluster.model_dump(),
        },
    )

    storage_client = storage.Client(PROJECT_ID)

    # Skip folder validation if path contains wildcards
    has_wildcards = '*' in name or '?' in name

    if job.mode == ModeEnum.FOLDER:
        if not has_wildcards and not is_gcs_folder(bucket, name, storage_client):
            raise ValueError(
                f"Folder mode was specified and {bucket}/{name} is not a folder"
            )
    elif job.mode == ModeEnum.MARKER:
        if not has_wildcards and is_gcs_folder(bucket, name, storage_client):
            raise ValueError(
                f"Marker mode was specified and {bucket}/{name} is a folder"
            )
        # remove the file and convert it to folder mode
        name = strip_filename(name)
    else:
        if not has_wildcards and is_gcs_folder(bucket, name, storage_client):
            raise ValueError(f"File mode was specified and {bucket}/{name} is a folder")

    # modify the args in the job
    ctx = {
        "source_bucket": bucket,
        "source_prefix": name,
    }

    log.debug("Template context: %s", ctx)

    config_args = render_jinja_in_list(job.task_config.args, ctx)

    config_args = add_or_update_run_uuid(config_args)

    log.info(
        "handle_run config args after rendering",
        extra={
            "config_args": config_args,
            "bucket": bucket,
            "prefix": name,
            "ctx": ctx,
            "task_args": job.task_config.args,
        },
    )

    job.task_config.args = config_args

    script_path = job.task_config._resolved_main_python_file_path
    log.debug("Local script path: %s", script_path)

    bucket = GCS_TEMP_BUCKET
    job.task_config._remote_main_python_file_path = upload_local_file(
        script_path, job.name, bucket, storage_client
    )
    log.debug("Remote script path: %s", job.task_config._remote_main_python_file_path)

    template_path = resources.files("storageflow.templates") / "wft.jinja2"

    renderer = InlineWorkflowTemplateRenderer(
        template_path=str(template_path), job=job, cluster=cluster
    )
    template = json.loads(renderer.render())

    log.debug("Rendered workflow JSON", extra={"template": template})

    if dry_run:
        log.info(
            "DRY RUN: Skipping workflow submission",
            extra={
                "project": PROJECT_ID,
                "region": DATAPROC_REGION,
                "script": job.task_config._remote_main_python_file_path,
            },
        )
        return "DRY-RUN-NO-OPERATION"

    client = dataproc.WorkflowTemplateServiceClient(
        client_options={
            "api_endpoint": f"{DATAPROC_REGION}-dataproc.googleapis.com:443"
        }
    )

    parent = f"projects/{PROJECT_ID}/regions/{DATAPROC_REGION}"

    operation = client.instantiate_inline_workflow_template(
        request={"parent": parent, "template": template}
    )
    log.debug(
        "Operation started",
        extra={
            "operation_name": operation.operation.name,
        },
    )

    path_string = operation.operation.name
    parts = path_string.split("/")
    operation_id = parts[-1]

    if not skip_controller_workflow:
        try:
            # now run controller workflow_operation
            await run_controller_workflow(
                run_uuid=str(run_uuid_var.get()), operation_id=operation_id
            )
        except Exception as e:
            log.exception("Failed to run controller workflow", extra={"error": str(e)})
            # do nothing. this is bad but shouldn't stop the main data load
    else:
        log.debug("Skipping controller workflow (CLI mode)")

    return operation_id


async def get_job_status(operation_id: str) -> WorkflowStatusResponse:
    """
    Get the status of a Dataproc operation by its ID.

    Args:
        operation_id (str): The ID of the Dataproc operation.

    Returns:
        dataproc.Operation: The operation object containing status information.
    """

    PROJECT_ID = get_mandatory_env_var("PROJECT_ID")
    DATAPROC_REGION = get_mandatory_env_var("DATAPROC_REGION")

    client = dataproc.WorkflowTemplateServiceClient(
        client_options={
            "api_endpoint": f"{DATAPROC_REGION}-dataproc.googleapis.com:443"
        }
    )

    operation_name = (
        f"projects/{PROJECT_ID}/regions/{DATAPROC_REGION}/operations/{operation_id}"
    )

    request = GetOperationRequest(name=operation_name)

    operation = client.get_operation(request=request)

    metadata = WorkflowMetadata()._pb
    if operation.metadata.Is(type(metadata).DESCRIPTOR):
        operation.metadata.Unpack(metadata)
    else:
        raise ValueError("Operation metadata is not of type WorkflowMetadata")

    msg_dict = MessageToDict(metadata, preserving_proto_field_name=True)

    operation = WorkflowOperation.model_validate(msg_dict)

    response = WorkflowStatusResponse(workflow_operation=operation)

    response.is_completed = operation.state == "DONE"

    if response.is_completed:
        response.is_successful = True
        for node in response.workflow_operation.graph.nodes:
            if node.state == "FAILED":
                log.error(
                    "Workflow node failed",
                    extra={
                        "node": node.model_dump_json(),
                        "operation_id": operation_id,
                    },
                )
                response.is_successful = False
                break

    return response
