import tomllib
from pathlib import Path
from typing import Any, Union
from google.cloud.exceptions import NotFound
from pydantic import TypeAdapter
from storageflow.models.jobs import (
    ConfigModel,
    ClusterConfigUnion,
    JobUnion,
    ModeEnum,
    BaseJob,
    JobTypeEnum,
)
import re

from typing import List

from storageflow.common.logging import get_logger
from storageflow.common.env_manager import get_mandatory_env_var
from storageflow.common.storage import is_gcs_folder

LOGGING_LEVEL = get_mandatory_env_var("LOGGING_LEVEL", "DEBUG")

log = get_logger(level=LOGGING_LEVEL)


class ConfigurationError(Exception):
    pass


class ConfigManager:
    def __init__(
        self,
        user_config_dir: Union[str, Path],
        common_config_dir: Union[str, Path, None] = None,
    ):
        if not user_config_dir:
            raise ConfigurationError("User configuration directory is required.")

        self.user_path = Path(user_config_dir).resolve()
        self.common_path = (
            Path(common_config_dir).resolve() if common_config_dir else None
        )
        self._cluster_adapter = TypeAdapter(ClusterConfigUnion)
        self._job_adapter = TypeAdapter(JobUnion)
        self.config = self._load()

    def _load_toml_files_from_dir(
        self, directory: Path
    ) -> list[tuple[Path, dict[str, Any]]]:
        # Load all TOML files recursively from a directory
        return [
            (file, tomllib.loads(file.read_text()))
            for file in directory.rglob("*.toml")
        ]

    def resolve_uri(self, uri: str) -> Path:
        if uri.startswith("user://"):
            return self.user_path / uri[len("user://") :]
        elif uri.startswith("common://"):
            if not self.common_path:
                raise ConfigurationError(
                    "Common path is not configured but 'common://' was used."
                )
            return self.common_path / uri[len("common://") :]
        else:
            # fallback to treating as a raw path or GCS URI if needed
            return Path(uri).resolve()

    def _load_clusters(
        self, configs: list[tuple[Path, dict]]
    ) -> dict[str, ClusterConfigUnion]:
        # Parse and validate all clusters from loaded TOML configs
        clusters: dict[str, ClusterConfigUnion] = {}
        for path, cfg in configs:
            for entry in cfg.get("clusters", []):
                entry = entry.copy()
                name = entry.get("name", None)

                if name in clusters:
                    raise ConfigurationError(
                        f"Duplicate cluster name '{name}' in file {path}"
                    )

                entry["name"] = entry.pop("name", None)
                entry["placement_type"] = entry.pop("type", None)

                try:
                    obj = self._cluster_adapter.validate_python(entry)
                except Exception as e:
                    raise ConfigurationError(
                        f"Error parsing cluster '{name}' in file {path}: {e}"
                    )

                clusters[name] = obj
        return clusters

    def _load_jobs(
        self, configs: list[tuple[Path, dict]], clusters: dict[str, ClusterConfigUnion]
    ) -> dict[str, JobUnion]:
        # Parse and validate all jobs, ensuring each job's referenced cluster exists
        jobs: dict[str, JobUnion] = {}

        for path, cfg in configs:
            for entry in cfg.get("jobs", []):
                name = entry.get("name")
                if name in jobs:
                    raise ConfigurationError(
                        f"Duplicate job name '{name}' in file {path}"
                    )

                cluster_ref = entry.get("cluster_blueprint_name")
                if cluster_ref not in clusters:
                    raise ConfigurationError(
                        f"Job '{name}' in file {path} references unknown cluster '{cluster_ref}'"
                    )

                try:
                    obj = self._job_adapter.validate_python(entry)
                except Exception as e:
                    raise ConfigurationError(
                        f"Error parsing job '{name}' in file {path}: {e}"
                    )

                # Resolve and store local script path for PySpark jobs
                if obj.job_type == JobTypeEnum.PYSPARK:
                    resolved_path = self.resolve_uri(
                        obj.task_config.main_python_file_uri
                    )
                    if not resolved_path.exists():
                        raise ConfigurationError(
                            f"main_python_file_uri for job '{obj.name}' does not exist at resolved path: {resolved_path}"
                        )
                    obj.task_config._resolved_main_python_file_path = resolved_path

                jobs[name] = obj

        return jobs

    def _load(self) -> ConfigModel:
        # Load and merge configurations from common and user directories
        user_tomls = self._load_toml_files_from_dir(self.user_path)
        common_tomls = (
            self._load_toml_files_from_dir(self.common_path) if self.common_path else []
        )

        common_clusters = self._load_clusters(common_tomls) if common_tomls else {}
        user_clusters = self._load_clusters(user_tomls)

        dup_clusters = list(set(common_clusters.keys()) & set(user_clusters.keys()))
        if len(dup_clusters) > 0:
            raise ConfigurationError(
                "Duplicate cluster names found: " + ", ".join(dup_clusters)
            )

        all_clusters = {**common_clusters, **user_clusters}

        all_jobs = self._load_jobs(user_tomls, clusters=all_clusters)

        return ConfigModel(
            jobs=list(all_jobs.values()), clusters=list(all_clusters.values())
        )

    def get_config(self) -> ConfigModel:
        return self.config


    def find_job_by_name(self, name: str) -> BaseJob:
        config = self.get_config()
        matching_job = next((job for job in config.jobs if job.name == name), None)

        if not matching_job:
            available_jobs = [job.name for job in config.jobs]
            raise NotFound(
                f"Job with name '{name}' not found in configuration. Try available jobs: {available_jobs}"
            )

        return matching_job

    def find_jobs_for_event(self, bucket: str, file_name: str) -> List[BaseJob]:
        """
        Find all auto-triggerable jobs that match the given bucket and file name.
        Used for event-driven triggers (GCS events, etc.)
        Returns a list of all matching jobs with auto_trigger=True.
        """
        config = self.get_config()
        matching_jobs = []

        for job in config.jobs:
            if job.source_bucket != bucket:
                continue

            # Only consider auto-triggerable jobs for event-driven mode
            if not job.auto_trigger:
                log.debug(
                    f"Skipping job '{job.name}' with auto_trigger=False (manual-only)"
                )
                continue

            if job.mode in [ModeEnum.FILE, ModeEnum.MARKER]:
                if not file_name:
                    raise ConfigurationError(
                        f"Job '{job.name}' requires a file name but none was provided"
                    )

                if re.search(job._compiled_source_prefix, file_name):
                    matching_jobs.append(job)
            else:  # FOLDER MODE

                if not re.search(job._compiled_source_prefix, file_name or ""):
                    continue

                # Skip folder validation if path contains wildcards
                has_wildcards = '*' in (file_name or '') or '?' in (file_name or '')

                if not has_wildcards and not is_gcs_folder(job.source_bucket, file_name or ""):
                    log.debug(
                        f"Ignoring Job '{job.name}' is in FOLDER mode but the provided file name '{file_name}' is not a folder path"
                    )
                    continue

                matching_jobs.append(job)

        if not matching_jobs:
            raise NotFound(
                f"No auto-triggerable jobs found for bucket '{bucket}' and file '{file_name}'"
            )

        if len(matching_jobs) > 1:
            log.warning(
                "Multiple auto-triggerable jobs matched for bucket '%s' and file '%s': %s. All will be enqueued.",
                bucket,
                file_name,
                [job.name for job in matching_jobs],
            )

        return matching_jobs

    def find_job_by_bucket_and_name(self, bucket: str, file_name: str | None = None):
        """
        Find a single job by bucket and file name (for manual execution).
        Ignores auto_trigger flag. Returns the highest priority match.
        """
        config = self.get_config()
        matching_jobs = []

        for job in config.jobs:
            if job.source_bucket != bucket:
                continue

            if job.mode in [ModeEnum.FILE, ModeEnum.MARKER]:
                if not file_name:
                    raise ConfigurationError(
                        f"Job '{job.name}' requires a file name but none was provided"
                    )

                if re.search(job._compiled_source_prefix, file_name):
                    matching_jobs.append(job)
            else:  # FOLDER MODE

                if not re.search(job._compiled_source_prefix, file_name or ""):
                    continue

                # Skip folder validation if path contains wildcards
                has_wildcards = '*' in (file_name or '') or '?' in (file_name or '')

                if not has_wildcards and not is_gcs_folder(job.source_bucket, file_name or ""):
                    log.warning(
                        f"Ignoring Job '{job.name}' is in FOLDER mode but the provided file name '{file_name}' is not a folder path"
                    )
                    continue

                matching_jobs.append(job)

        if not matching_jobs:
            raise NotFound(
                f"No matching job found for bucket '{bucket}' and file '{file_name}'"
            )

        if len(matching_jobs) > 1:
            raise ConfigurationError(
                f"Multiple jobs matched for bucket '{bucket}' and file '{file_name}': {[job.name for job in matching_jobs]}. "
                "For manual execution, ensure job patterns don't overlap, or specify job by name using --job-name."
            )

        return matching_jobs[0]

    def analyze_job_matches(self, bucket: str, file_name: str) -> dict:
        """
        Analyze all jobs to determine which would match for a given bucket/path.
        Returns detailed information about each job including why it matched or didn't.
        """
        config = self.get_config()
        results = {
            "will_run": [],      # Jobs that will be triggered (auto_trigger=true and match)
            "ignored": [],       # Jobs that match but won't trigger (auto_trigger=false)
            "no_match": [],      # Jobs that don't match
        }

        for job in config.jobs:
            analysis = {
                "name": job.name,
                "bucket": job.source_bucket,
                "pattern": job.source_prefix,
                "mode": job.mode.value,
                "auto_trigger": job.auto_trigger,
                "reason": None,
            }

            # Check bucket match
            if job.source_bucket != bucket:
                analysis["reason"] = f"Bucket mismatch (job expects '{job.source_bucket}')"
                results["no_match"].append(analysis)
                continue

            # Check pattern match
            if not re.search(job._compiled_source_prefix, file_name):
                analysis["reason"] = f"Pattern mismatch (pattern: '{job.source_prefix}')"
                results["no_match"].append(analysis)
                continue

            # For FOLDER mode, note that folder validation would happen at runtime
            if job.mode == ModeEnum.FOLDER:
                has_wildcards = '*' in file_name or '?' in file_name
                if not has_wildcards:
                    # Note: We skip actual GCS folder validation in inspect mode
                    # At runtime, this would check if the path is actually a folder
                    analysis["note"] = "FOLDER mode: path validation skipped (would be checked at runtime)"

            # Job matches! Now check if it would actually run
            if job.auto_trigger:
                analysis["reason"] = "Pattern matches and auto_trigger=true"
                results["will_run"].append(analysis)
            else:
                analysis["reason"] = "Pattern matches but auto_trigger=false (manual-only)"
                results["ignored"].append(analysis)

        return results

    def get_cluster_for_job(self, job: BaseJob):
        """
        Given a job object, return the matching cluster blueprint config
        using the 'cluster_blueprint_name' field.
        """
        cluster_name = job.cluster_blueprint_name

        config = self.get_config()
        cluster = next((c for c in config.clusters if c.name == cluster_name), None)

        if not cluster:
            raise ConfigurationError(
                f"Cluster blueprint '{cluster_name}' not found in config"
            )

        return cluster
