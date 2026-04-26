from pydantic import BaseModel, model_validator
from jinja2 import Template  # pyright: ignore
import os


PROJECT_ID = os.getenv("PROJECT_ID", "unset!!!")
ENVIRONMENT = os.getenv("ENV", "unset!!!")
CONFIG_PATH = os.getenv("CONFIG_PATH", "unset!!!")


class PathsConfig(BaseModel):
    config_root: str
    jobs_root: str
    common_jobs_path_fragment: str
    per_env_jobs_path_fragment: str
    templates_root: str
    common_templates_path_fragment: str
    per_env_templates_path_fragment: str

    @model_validator(mode="after")
    def interpolate_paths(self) -> "PathsConfig":

        self.config_root = Template(self.config_root).render(config_path=CONFIG_PATH)

        self.jobs_root = Template(self.jobs_root).render(config_root=self.config_root)
        self.common_jobs_path_fragment = Template(
            self.common_jobs_path_fragment
        ).render(jobs_root=self.jobs_root)

        self.per_env_jobs_path_fragment = Template(
            self.per_env_jobs_path_fragment
        ).render(jobs_root=self.jobs_root, env=ENVIRONMENT, project_id=PROJECT_ID)

        self.templates_root = Template(self.templates_root).render(
            config_root=self.config_root
        )
        self.common_templates_path_fragment = Template(
            self.common_templates_path_fragment
        ).render(templates_root=self.templates_root)
        self.per_env_templates_path_fragment = Template(
            self.per_env_templates_path_fragment
        ).render(
            templates_root=self.templates_root, env=ENVIRONMENT, project_id=PROJECT_ID
        )

        return self


class AppConfig(BaseModel):
    paths: PathsConfig
