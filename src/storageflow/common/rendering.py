from jinja2 import Environment, FileSystemLoader, select_autoescape
from storageflow.models.jobs import BaseJob
from storageflow.models.clusters import BaseClusterConfig
import json
from pathlib import Path
from typing import List, Dict, Any
from jinja2 import Template


def render_jinja_in_list(strings: List[str], context: Dict[str, Any]) -> List[str]:
    """
    Renders Jinja2 templates in each string of the list if `{{` is found.
    Leaves strings untouched if no templating expression exists.

    Args:
        strings (List[str]): List of strings to process.
        context (Dict[str, Any]): Context to render Jinja templates.

    Returns:
        List[str]: List with rendered (or untouched) strings.
    """
    result = []
    for s in strings:
        if "{{" in s and "}}" in s:
            try:
                rendered = Template(s).render(context)
                result.append(rendered)
            except Exception as e:
                raise ValueError(f"Error rendering template '{s}': {e}")
        else:
            result.append(s)
    return result


class InlineWorkflowTemplateRenderer:
    def __init__(self, template_path: str, job: BaseJob, cluster: BaseClusterConfig):
        self.template_dir = str(Path(template_path).parent)
        self.template_file = Path(template_path).name
        self.job = job
        self.cluster = cluster

        self.env = Environment(
            loader=FileSystemLoader(self.template_dir),
            autoescape=select_autoescape(["j2", "json"]),
        )
        self.env.filters["tojson"] = lambda val: json.dumps(val, separators=(",", ":"))
        self.template = self.env.get_template(self.template_file)

    def render(self) -> str:
        return self.template.render(job=self.job, cluster=self.cluster)
