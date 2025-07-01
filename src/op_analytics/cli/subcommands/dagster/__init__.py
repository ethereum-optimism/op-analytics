# This file makes the dagster subcommand directory a Python package

"""
Dagster CLI subcommands for opdata.
"""

import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import typer
from typing_extensions import Annotated

from op_analytics.coreutils.logger import structlog

log = structlog.get_logger()

app = typer.Typer(
    help="Dagster deployment utilities.",
    add_completion=False,
    pretty_exceptions_show_locals=False,
)


def get_current_version():
    """Get the current Dagster image version from Makefile."""
    makefile_path = Path("Makefile")
    if not makefile_path.exists():
        raise FileNotFoundError("Makefile not found")

    with open(makefile_path, "r") as f:
        content = f.read()

    match = re.search(
        r"IMAGE_TAG_DAGSTER = ghcr\.io/ethereum-optimism/op-analytics-dagster:(.+)", content
    )
    if not match:
        raise ValueError("Could not find IMAGE_TAG_DAGSTER in Makefile")

    return match.group(1)


def update_version_in_file(file_path, old_version, new_version):
    """Update version in a file."""
    with open(file_path, "r") as f:
        content = f.read()

    # Replace the version in the file
    updated_content = content.replace(old_version, new_version)

    with open(file_path, "w") as f:
        f.write(updated_content)

    log.info(f"Updated version in {file_path}: {old_version} → {new_version}")


def generate_next_version(current_version):
    """Generate the next version by incrementing the patch number."""
    # Parse current version (e.g., v20250620.002)
    match = re.match(r"v(\d{8})\.(\d+)", current_version)
    if not match:
        raise ValueError(
            f"Invalid version format: {current_version}. Expected format: vYYYYMMDD.NNN"
        )

    date_part = match.group(1)
    patch_part = int(match.group(2))

    # Check if we need to update the date
    today = datetime.now().strftime("%Y%m%d")
    if date_part != today:
        # New date, reset patch to 001
        return f"v{today}.001"
    else:
        # Same date, increment patch
        return f"v{date_part}.{patch_part + 1:03d}"


def run_command(command, description):
    """Run a shell command and handle errors."""
    log.info(f"{description}...")
    log.debug(f"Running: {command}")

    result = subprocess.run(command, shell=True, capture_output=True, text=True)

    if result.returncode != 0:
        log.error(f"Error running: {command}")
        log.error(f"stdout: {result.stdout}")
        log.error(f"stderr: {result.stderr}")
        raise typer.Exit(1)

    log.info(f"{description} completed successfully")
    if result.stdout.strip():
        log.debug(f"Output: {result.stdout.strip()}")


@app.command(name="bump-version")
def bump_version(
    version: Annotated[
        str | None,
        typer.Option(
            "--version",
            help="New version to set (e.g., v20250620.003). If not provided, will auto-increment.",
        ),
    ] = None,
    auto_bump: Annotated[
        bool,
        typer.Option("--auto-bump", help="Auto-increment the patch version (default behavior)"),
    ] = False,
    skip_build: Annotated[
        bool, typer.Option("--skip-build", help="Skip building and pushing the Docker image")
    ] = False,
    skip_deploy: Annotated[
        bool, typer.Option("--skip-deploy", help="Skip deploying to Helm")
    ] = False,
):
    """
    Bump the Dagster image version and deploy it.

    This command will:
    1. Bump the version in both Makefile and helm/dagster/values.yaml
    2. Build and push the new Docker image
    3. Update the Helm deployment

    Examples:
        opdata dagster bump-version --version v20250620.003
        opdata dagster bump-version --auto-bump  # Auto-increment patch version
        opdata dagster bump-version --skip-build  # Only update files, skip Docker build
    """
    # Get current version
    try:
        current_version = get_current_version()
        log.info(f"Current Dagster version: {current_version}")
    except Exception as e:
        log.error(f"Error getting current version: {e}")
        raise typer.Exit(1)

    # Determine new version
    if version:
        new_version = version
        if not new_version.startswith("v"):
            new_version = f"v{new_version}"
    else:
        new_version = generate_next_version(current_version)

    log.info(f"New Dagster version: {new_version}")

    # Confirm the change
    if not version and not auto_bump:
        response = typer.confirm(f"Proceed with version bump {current_version} → {new_version}?")
        if not response:
            log.info("Version bump cancelled")
            raise typer.Exit(0)

    # Update versions in files
    try:
        update_version_in_file("Makefile", current_version, new_version)
        update_version_in_file("helm/dagster/values.yaml", current_version, new_version)
    except Exception as e:
        log.error(f"Error updating version in files: {e}")
        raise typer.Exit(1)

    # Build and push Docker image
    if not skip_build:
        run_command("make docker-dagster", "Building and pushing Dagster Docker image")

    # Deploy to Helm
    if not skip_deploy:
        run_command("make helm-dagster", "Deploying to Helm")

    log.info(f"Successfully bumped Dagster version to {new_version}!")
    log.info(f"Previous version: {current_version}")
    log.info(f"New version: {new_version}")

    if not skip_build and not skip_deploy:
        log.info("Summary of actions completed:")
        log.info("  1. ✅ Updated version in Makefile")
        log.info("  2. ✅ Updated version in helm/dagster/values.yaml")
        log.info("  3. ✅ Built and pushed Docker image")
        log.info("  4. ✅ Deployed to Helm")
    elif skip_build:
        log.info("Summary of actions completed:")
        log.info("  1. ✅ Updated version in Makefile")
        log.info("  2. ✅ Updated version in helm/dagster/values.yaml")
        log.info("  3. ⏭️  Skipped Docker build (--skip-build)")
        log.info("  4. ✅ Deployed to Helm")
    elif skip_deploy:
        log.info("Summary of actions completed:")
        log.info("  1. ✅ Updated version in Makefile")
        log.info("  2. ✅ Updated version in helm/dagster/values.yaml")
        log.info("  3. ✅ Built and pushed Docker image")
        log.info("  4. ⏭️  Skipped Helm deployment (--skip-deploy)")


@app.command()
def status():
    """Show the current Dagster deployment status."""
    try:
        current_version = get_current_version()
        log.info(f"Current Dagster version: {current_version}")

        # Try to get deployment status from kubectl
        try:
            result = subprocess.run(
                "kubectl get pods -n dagster -l app.kubernetes.io/name=dagster",
                shell=True,
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                log.info("Dagster pods status:")
                log.info(result.stdout)
            else:
                log.warning("Could not get pod status (kubectl not available or not authenticated)")
        except Exception as e:
            log.warning(f"Could not get pod status: {e}")

    except Exception as e:
        log.error(f"Error getting status: {e}")
        raise typer.Exit(1)
