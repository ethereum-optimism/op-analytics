import importlib
import os
import time

import typer
from op_coreutils.logger import LOGGER

from op_analytics.cli.mainhelpers import find_apps

log = LOGGER.get_logger()

app = typer.Typer(no_args_is_help=True)


typer.main.get_command_name = lambda name: name


# Python modules under the "subcommands" directory are included into the CLI as subcommands.
# By convention a subcommand module must define an "app" Typer() instance.
for subapp in find_apps(os.path.join(os.path.dirname(__file__), "subcommands")):
    start = time.time()

    # Import the moduel and add the "app" instance to the top-level typer app.
    subapp_module = importlib.import_module(f"op_analytics.cli.subcommands.{subapp}")
    app.add_typer(subapp_module.app, name=subapp, no_args_is_help=True)

    elapsed = time.time() - start
    log.info(f"Loaded subcommand: {subapp} ... {elapsed:.2f} seconds")


def entrypoint():
    """This function is used in pyproject.toml as the entrypoint for the opdata CLI."""
    app()
