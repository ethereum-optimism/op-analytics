import os
from dataclasses import dataclass
from typing import Any

from jinja2 import Environment, FileSystemLoader
from op_analytics.coreutils.logger import structlog

log = structlog.get_logger()


@dataclass
class RenderedSQLQuery:
    """Results of rendering a TemplatedSQLQuery"""

    template_name: str
    query: str


@dataclass
class TemplatedSQLQuery:
    """SQL Query Template.

    Models can utilize templated sql queries as part of their processing logic."""

    template_name: str
    context: dict[str, Any]

    @property
    def template_file(self):
        return self.template_name + ".sql.j2"

    def render(self) -> RenderedSQLQuery:
        # Load templates from the templates folder.
        tempates_dir = os.path.join(os.path.dirname(__file__), "../templates")
        env = Environment(loader=FileSystemLoader(tempates_dir))

        # Load and render the template
        template = env.get_template(self.template_file)

        log.info("Rendering query", template=self.template_name)
        rendered = template.render(self.context) + "\n"

        return RenderedSQLQuery(
            template_name=self.template_name,
            query=rendered,
        )


class SQLRenderError(Exception):
    pass
