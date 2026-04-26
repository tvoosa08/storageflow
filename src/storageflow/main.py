import os
from contextlib import asynccontextmanager
from pathlib import Path

import tomllib
from typing import Annotated
from fastapi import FastAPI, HTTPException, Request, status, Header
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from google.api_core.exceptions import AlreadyExists
from google.cloud.exceptions import NotFound

from storageflow.common.env_manager import get_mandatory_env_var
from storageflow.common.logging import get_logger, run_uuid_var
from storageflow.common.utils import get_job_uid
from storageflow.config_manager import ConfigManager
from storageflow.dataproc_manager import get_job_status, handle_run
from storageflow.models.app_config import AppConfig
from storageflow.models.jobs import ConfigModel
from storageflow.models.payloads import (
    CloudEvent,
    JobRunResponse,
    TaskPayload,
    WorkflowStatusResponse,
    JobRunResponseStatus,
)


from storageflow.tasker import create_task
from storageflow.models.exceptions import JobExecutionFailed


PROJECT_ID = get_mandatory_env_var("PROJECT_ID")
USER_CONFIG_PATH = get_mandatory_env_var("USER_CONFIG_PATH")
ROOT_CONFIG_PATH = get_mandatory_env_var("ROOT_CONFIG_PATH")
LOGGING_LEVEL = get_mandatory_env_var("LOGGING_LEVEL") or "DEBUG"
HEADERS_RUN_UUID_NAME = get_mandatory_env_var("HEADERS_RUN_UUID_NAME")
HEADERS_RUN_UUID_NAME_PYTHON = HEADERS_RUN_UUID_NAME.replace("-", "_")

log = get_logger(level=LOGGING_LEVEL)


def get_app_config() -> AppConfig:
    """load application configuration options from a TOML file as singleton"""
    base_dir = os.path.dirname(os.path.abspath(__file__))

    config_path = Path(base_dir, "app_config.toml")
    with config_path.open("rb") as f:
        raw_data = tomllib.load(f)
    config = AppConfig.model_validate(raw_data)

    return config


def get_user_config() -> ConfigManager:

    config = ConfigManager(
        common_config_dir=ROOT_CONFIG_PATH, user_config_dir=USER_CONFIG_PATH
    )
    # log.debug("Configuration: %s", config.get_config().model_dump())
    return config


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.product_config = get_app_config()
    app.state.user_config = get_user_config()

    yield


app = FastAPI(lifespan=lifespan)


@app.middleware("http")
async def inject_run_uuid(request: Request, call_next):
    run_uuid = request.headers.get(HEADERS_RUN_UUID_NAME)
    if run_uuid:
        run_uuid_var.set(run_uuid)
    return await call_next(request)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    try:
        raw_body = await request.body()
        raw_body_decoded = raw_body.decode("utf-8")
    except Exception:
        raw_body_decoded = "<failed to decode body>"
        log.exception("Could not decode request body for logging")

    validation_details = jsonable_encoder(exc.errors())

    log.error(
        "Pydantic validation error on incoming request",
        exc_info=True,
        extra={
            "raw_body": raw_body_decoded,
            "validation_errors": validation_details,
            "path": request.url.path,
            "method": request.method,
        },
    )

    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=jsonable_encoder({"detail": exc.errors()}),
    )


@app.get("/ping")
def ping():
    return {"message": "PONG: Service is alive"}


@app.get("/config/product")
async def show_product_config() -> AppConfig:
    return app.state.product_config  # type:ignore


@app.get("/config/user", response_model=ConfigModel)
async def show_user_config():
    return app.state.user_config.get_config()  # type:ignore


@app.post("/jobs")
async def run_job(event: CloudEvent) -> JobRunResponse:

    # generate a unique job UID for this run
    run_uuid = get_job_uid()
    # and record it in the context variable
    run_uuid_var.set(run_uuid)

    log.info("job request received", extra={"event": event.model_dump()})

    config: ConfigManager = app.state.user_config
    try:
        # Find all auto-triggerable jobs that match this event
        matching_jobs = config.find_jobs_for_event(event.bucket, event.name)
    except NotFound as e:
        log.info(
            "configuration not found for event. ignoring",
            extra={"event": event.model_dump()},
        )
        return JobRunResponse(run_uuid=run_uuid, status=JobRunResponseStatus.IGNORED)

    log.info(
        "accepted %d job(s) for running",
        len(matching_jobs),
        extra={
            "event": event.model_dump(),
            "jobs": [job.name for job in matching_jobs],
        },
    )

    # Enqueue tasks for all matching jobs
    for job in matching_jobs:
        try:
            create_task(
                job=job,
                event=event,
                job_uid=run_uuid,
            )
            log.info(
                "task created for job",
                extra={"job_name": job.name, "run_uuid": run_uuid},
            )
        except AlreadyExists as e:
            log.warning(
                "task already exists for job, skipping",
                extra={"job_name": job.name, "error": str(e)},
            )
            continue

    return JobRunResponse(
        run_uuid=run_uuid,
        status=JobRunResponseStatus.ACCEPTED,
    )


@app.get("/jobs/{operation_id}", response_model=WorkflowStatusResponse)
async def get_job(
    operation_id: str,
    run_uuid: Annotated[str | None, Header(alias=HEADERS_RUN_UUID_NAME)],
) -> WorkflowStatusResponse:

    run_uuid_var.set(run_uuid)  # set the run UUID in the context variable

    try:
        response = await get_job_status(operation_id)
        log.info(
            "Job status retrieved successfully",
            extra={"response": response.model_dump_json()},
        )

        if response.is_completed:

            extra = {
                "operation_id": operation_id,
                "response": response.model_dump_json(),
            }

            match response.is_successful:
                case False:
                    log.error("Job completed with errors", extra=extra)
                    raise JobExecutionFailed(run_uuid=str(run_uuid))
                case True:
                    log.info("Job completed successfully", extra=extra)
                case _:
                    log.warning("Job completed with unknown status", extra=extra)

        return response
    except JobExecutionFailed as e:
        log.exception(
            "Job execution failed",
            extra={"run_uuid": run_uuid, "error": str(e)},
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail=str(e))
    except NotFound:
        log.exception(f"Job not found for operation ID: {operation_id}", exc_info=True)
        raise HTTPException(
            status_code=404, detail=f"Job not found for operation ID: {operation_id}"
        )
    except Exception as e:
        log.exception(f"Unexpected error retrieving job status: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Unexpected error retrieving job status: {e}"
        )


@app.post("/tasks")
async def run_task(task: TaskPayload):
    try:
        operation_id = await handle_run(
            bucket=task.bucket, name=task.name, config=app.state.user_config
        )
    except ValueError as e:
        log.exception("Error running task", extra={"error": str(e)}, exc_info=True)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        log.exception(
            "Unexpected error running task", extra={"error": str(e)}, exc_info=True
        )
        raise HTTPException(status_code=500, detail=str(e))

    return operation_id
