# clusters.py

from pydantic import BaseModel, Field, ConfigDict
from typing import Dict, Any, Annotated
from enum import StrEnum
from typing import Literal


class PlacementTypeEnum(StrEnum):
    MANAGED_CLUSTER = "managed_cluster"
    CLUSTER_BY_NAME = "cluster_by_name"
    CLUSTER_BY_LABELS = "cluster_by_labels"


class BaseClusterConfig(BaseModel):
    name: str  # The name of the cluster


class ManagedClusterConfig(BaseClusterConfig):
    placement_type: Literal[PlacementTypeEnum.MANAGED_CLUSTER] = (
        PlacementTypeEnum.MANAGED_CLUSTER
    )
    config: Dict[str, Any] | None = Field(
        default_factory=dict,
        description="Partial Dataproc ClusterConfig for the managed cluster.",
    )
    labels: Dict[str, str] | None = Field(
        default_factory=dict,
        description="Labels for clusters defined by this blueprint.",
    )

    model_config = ConfigDict(extra="forbid")


class ClusterByNameConfig(BaseClusterConfig):
    placement_type: Literal[PlacementTypeEnum.CLUSTER_BY_NAME] = (
        PlacementTypeEnum.CLUSTER_BY_NAME
    )
    cluster_name_locator: str  # The name of the existing cluster to select

    model_config = ConfigDict(extra="forbid")


class ClusterByLabelsConfig(BaseClusterConfig):
    placement_type: Literal[PlacementTypeEnum.CLUSTER_BY_LABELS] = (
        PlacementTypeEnum.CLUSTER_BY_LABELS
    )
    cluster_labels: Dict[str, str]  # Labels to match an existing cluster

    model_config = ConfigDict(extra="forbid")


ClusterConfigUnion = Annotated[
    ManagedClusterConfig | ClusterByLabelsConfig | ClusterByNameConfig,
    Field(discriminator="placement_type"),
]
