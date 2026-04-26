import argparse
import asyncio

from storageflow.config_manager import ConfigManager
from storageflow.common.logging import get_logger, run_uuid_var

import os

# initialize these before importing dataproc_manager. ugly but neeeded. TODO: find a better way to initialize these vars
os.environ["WORKFLOW_NAME"] = "dummy"
os.environ["WORKFLOW_LOCATION"] = "dummy"
os.environ["WORKFLOW_MAX_RETRIES"] = "3"
os.environ["WORKFLOW_POLL_INTERVAL_SECONDS"] = "10"


from storageflow.dataproc_manager import handle_run as dataproc_handle_run


from storageflow.common.utils import get_job_uid


from storageflow.models.jobs import ModeEnum


log = get_logger("DEBUG")


def build_parser():

    common_parser = argparse.ArgumentParser(
        add_help=False, description="StorageFlow CLI"
    )
    common_parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level",
    )

    parser = argparse.ArgumentParser(description="StorageFlow CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Run subcommand
    run_parser = subparsers.add_parser(
        "run", help="Run a configured job", parents=[common_parser]
    )
    run_parser.add_argument(
        "--user-config-root", required=False, help="Configuration user root directory"
    )
    run_parser.add_argument(
        "--common-config-root",
        required=True,
        help="Configuration common root directory",
    )
    run_parser.add_argument(
        "--temp-gcs-bucket", required=True, help="Temporary GCS bucket for staging"
    )
    run_parser.add_argument("--project", required=True, help="GCP project ID")
    run_parser.add_argument("--region", required=True, help="GCP region")

    run_parser.add_argument(
        "--job-name",
        required=True,
        help="Name of the job to run (REQUIRED). Use this to specify which configured job to execute.",
    )

    run_parser.add_argument(
        "--path",
        required=True,
        help="GCS path to process (e.g., 'hourly/data/2025103009' or 'hourly/data/2025103009/DONE'). Bucket is taken from job config.",
    )

    run_parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Select job, render workflow, and show configuration without actually running",
    )

    # Inspect subcommand
    inspect_parser = subparsers.add_parser(
        "inspect", help="Inspect which jobs would run for a given path", parents=[common_parser]
    )
    inspect_parser.add_argument(
        "--user-config-root", required=False, help="Configuration user root directory"
    )
    inspect_parser.add_argument(
        "--common-config-root",
        required=True,
        help="Configuration common root directory",
    )
    inspect_parser.add_argument(
        "--bucket",
        required=True,
        help="GCS bucket name (e.g., 'ys-gcp-apollo-prod-use5-select-tier-elig-snapshot')",
    )
    inspect_parser.add_argument(
        "--path",
        required=True,
        help="GCS path to inspect (e.g., 'hourly/data/2025103009/DONE')",
    )

    # Validate subcommand
    validate_parser = subparsers.add_parser(
        "validate", help="Validate config files", parents=[common_parser]
    )
    validate_parser.add_argument(
        "--config-root", required=True, help="Configuration root directory"
    )
    validate_parser.add_argument(
        "--print", help="Print the configuration", action="store_true", default=False
    )

    return parser


async def handle_run(args):

    # set env vars as expected by dataproc_manager
    os.environ["PROJECT_ID"] = args.project
    os.environ["GOOGLE_CLOUD_PROJECT"] = args.project  # for GCS client
    os.environ["DATAPROC_REGION"] = args.region
    os.environ["GCS_TEMP_BUCKET"] = args.temp_gcs_bucket
    os.environ["WORKFLOW_NAME"] = "dummy"
    os.environ["WORKFLOW_LOCATION"] = "dummy"
    os.environ["WORKFLOW_MAX_RETRIES"] = "3"
    os.environ["WORKFLOW_POLL_INTERVAL_SECONDS"] = "10"

    config = ConfigManager(
        user_config_dir=args.user_config_root,
        common_config_dir=args.common_config_root,
    )

    # Select the job by name (mandatory)
    log.info("=" * 80)
    log.info("SELECTING JOB BY NAME: %s", args.job_name)
    log.info("=" * 80)
    job = config.find_job_by_name(args.job_name)

    # Display selected job configuration
    log.info("")
    log.info("SELECTED JOB: %s", job.name)
    log.info("-" * 80)
    log.info("Job Configuration:")
    log.info("  Bucket: %s", job.source_bucket)
    log.info("  Pattern: %s", job.source_prefix)
    log.info("  Mode: %s", job.mode)
    log.info("  Auto-trigger: %s", job.auto_trigger)
    log.info("  Full config: %s", job.model_dump_json(indent=2))
    log.info("-" * 80)

    cluster = config.get_cluster_for_job(job)
    log.info("")
    log.info("SELECTED CLUSTER: %s", cluster.name)
    log.info("-" * 80)
    log.info("Cluster Configuration:")
    log.info("%s", cluster.model_dump_json(indent=2))
    log.info("-" * 80)
    log.info("")

    # Use bucket from job config, path from CLI argument
    operation_id = await dataproc_handle_run(
        bucket=job.source_bucket,
        name=args.path,
        config=config,
        job=job,
        dry_run=args.dry_run,
        skip_controller_workflow=True,  # Skip controller workflow when running from CLI
    )

    if args.dry_run:
        log.info("DRY RUN COMPLETE - No job was submitted")
        return None

    log.info("To poll for completion of operation, run the command below")
    log.info(f"./polling.sh {operation_id} {args.region} {args.project}")

    return operation_id


def handle_inspect(args):
    """Inspect which jobs would run for a given bucket/path"""

    config = ConfigManager(
        user_config_dir=args.user_config_root,
        common_config_dir=args.common_config_root,
    )

    log.info("=" * 80)
    log.info("INSPECTING JOB MATCHES")
    log.info("  Bucket: %s", args.bucket)
    log.info("  Path: %s", args.path)
    log.info("=" * 80)
    log.info("")

    # Analyze all jobs
    results = config.analyze_job_matches(args.bucket, args.path)

    # Display jobs that will run
    if results["will_run"]:
        log.info("✓ JOBS THAT WILL RUN (auto_trigger=true)")
        log.info("-" * 80)
        for job in results["will_run"]:
            log.info("  • %s", job["name"])
            log.info("    Mode: %s", job["mode"])
            log.info("    Pattern: %s", job["pattern"])
            log.info("    Reason: %s", job["reason"])
            if job.get("note"):
                log.info("    Note: %s", job["note"])
            log.info("")
    else:
        log.info("✗ NO JOBS WILL RUN")
        log.info("-" * 80)
        log.info("")

    # Display jobs that match but are ignored
    if results["ignored"]:
        log.info("⊘ JOBS THAT MATCH BUT ARE IGNORED")
        log.info("-" * 80)
        for job in results["ignored"]:
            log.info("  • %s", job["name"])
            log.info("    Mode: %s", job["mode"])
            log.info("    Pattern: %s", job["pattern"])
            log.info("    Reason: %s", job["reason"])
            if job.get("note"):
                log.info("    Note: %s", job["note"])
            log.info("")

    # Display jobs that don't match (optional, can be verbose)
    if results["no_match"]:
        log.info("○ JOBS THAT DON'T MATCH (%d total)", len(results["no_match"]))
        log.info("-" * 80)
        for job in results["no_match"]:
            log.info("  • %s", job["name"])
            log.info("    Reason: %s", job["reason"])
            log.info("")

    log.info("=" * 80)
    log.info("SUMMARY")
    log.info("  Will run: %d", len(results["will_run"]))
    log.info("  Ignored (manual-only): %d", len(results["ignored"]))
    log.info("  No match: %d", len(results["no_match"]))
    log.info("=" * 80)


async def handle_validate(args):
    config = ConfigManager(args.config_root)
    config.get_config()  # just triggers validation
    if args.print:
        print(config.get_config().model_dump_json(indent=2))
    else:
        log.info("Configuration is valid.")


async def main():
    parser = build_parser()
    args = parser.parse_args()

    run_uuid = get_job_uid()
    run_uuid_var.set(run_uuid)  # set the run UUID in the context variable

    if args.log_level:
        log.setLevel(args.log_level)

    log.debug("Arguments: %s", args)

    match args.command:
        case "run":
            await handle_run(args)
        case "inspect":
            handle_inspect(args)
        case "validate":
            await handle_validate(args)
        case _:
            parser.print_help()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"An error occurred: {e}")
        raise
