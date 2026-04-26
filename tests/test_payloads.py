import pytest
from datetime import datetime, timezone
from pydantic import ValidationError

from storageflow.models.payloads import (
    ClusterLabelsConfig,
    ClusterJsonConfig,
    JobRunRequest,
)


class TestClusterLabelsConfig:
    def test_valid_labels(self):
        config = ClusterLabelsConfig(labels={"env": "prod"})
        assert config.labels == {"env": "prod"}

    def test_extra_field_forbidden(self):
        with pytest.raises(ValidationError):
            ClusterLabelsConfig(labels={"env": "prod"}, extra_field="not_allowed")


class TestClusterJsonConfig:
    def test_valid_config(self):
        config = ClusterJsonConfig(
            project_id="my-project",
            cluster_name="test-cluster",
            cluster_config={"lifecycle_config": {"idle_delete_ttl": "3600s"}},
        )
        assert config.project_id == "my-project"
        assert config.cluster_name == "test-cluster"
        assert "lifecycle_config" in config.cluster_config

    def test_extra_field_forbidden(self):
        with pytest.raises(ValidationError):
            ClusterJsonConfig(
                project_id="my-project",
                cluster_name="test-cluster",
                config={},
                extra_field="not_allowed",
            )


class TestJobRunRequest:
    def test_valid_with_labels_cluster(self):
        req = JobRunRequest(
            uuid="abc",
            service_account="test@foo.iam.gserviceaccount.com",
            eta=datetime.now(timezone.utc),
            cluster=ClusterLabelsConfig(labels={"env": "prod"}),
            task_config=None,
            tracking_topic=None,
        )
        assert req.service_account.endswith(".iam.gserviceaccount.com")

    def test_invalid_service_account(self):
        with pytest.raises(ValidationError):
            JobRunRequest(
                uuid="abc",
                service_account="test@foo.com",
                eta=datetime.now(timezone.utc),
                cluster=ClusterLabelsConfig(labels={"env": "prod"}),
                task_config=None,
                tracking_topic=None,
            )

    def test_cluster_json_config_requires_auto_delete(self):
        with pytest.raises(ValidationError):
            JobRunRequest(
                uuid="abc",
                service_account="test@foo.iam.gserviceaccount.com",
                eta=datetime.now(timezone.utc),
                cluster=ClusterJsonConfig(
                    project_id="my-project",
                    cluster_name="test-cluster",
                    cluster_config={},
                ),
                task_config=None,
                tracking_topic=None,
            )

    def test_cluster_json_config_with_idle_delete(self):
        req = JobRunRequest(
            uuid="abc",
            service_account="test@foo.iam.gserviceaccount.com",
            eta=datetime.now(timezone.utc),
            cluster=ClusterJsonConfig(
                project_id="my-project",
                cluster_name="test-cluster",
                cluster_config={"lifecycle_config": {"idle_delete_ttl": "3600s"}},
            ),
            task_config=None,
            tracking_topic=None,
        )
        assert (
            req.cluster.cluster_config["lifecycle_config"]["idle_delete_ttl"] == "3600s"
        )
