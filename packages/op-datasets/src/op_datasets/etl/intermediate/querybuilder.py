import os
from dataclasses import dataclass
from typing import Any

from jinja2 import Environment, FileSystemLoader
from op_coreutils.env.aware import OPLabsEnvironment, current_environment
from op_coreutils.logger import structlog

log = structlog.get_logger()


@dataclass
class RenderedSQLQuery:
    """Results of rendering a TemplatedSQLQuery"""

    name: str
    query: str


@dataclass
class TemplatedSQLQuery:
    """SQL Query Template.

    Models can utilize templated sql queries as part of their processing logic."""

    template_name: str
    result_name: str
    context: dict[str, Any]

    @property
    def template_file(self):
        return self.template_name + ".sql.j2"

    @property
    def rendered_file(self):
        rendered_dir = os.path.join(os.path.dirname(__file__), "rendered")
        return os.path.join(rendered_dir, self.template_name + ".rendered.sql")

    @property
    def rendered_actual_file(self):
        rendered_dir = os.path.join(os.path.dirname(__file__), "rendered")
        return os.path.join(rendered_dir, self.template_name + ".actual.sql")

    def render(self) -> RenderedSQLQuery:
        # Load templates from the templates folder.
        tempates_dir = os.path.join(os.path.dirname(__file__), "templates")
        env = Environment(loader=FileSystemLoader(tempates_dir))

        # Load and render the template
        template = env.get_template(self.template_file)

        log.info("Rendering query", template=self.template_name)
        rendered = template.render(self.context)

        with open(self.rendered_file, "r") as fobj:
            expected = fobj.read()

        if rendered != expected:
            with open(self.rendered_actual_file, "w") as fobj:
                fobj.write(rendered)

            msg = f"Rendered SQL template does not match expectation. Please update: {self.rendered_file}"
            if current_environment() == OPLabsEnvironment.UNITTEST:
                raise SQLRenderError(msg)
            else:
                log.warning(msg)

        return RenderedSQLQuery(
            name=self.result_name,
            query=rendered,
        )


class SQLRenderError(Exception):
    pass
