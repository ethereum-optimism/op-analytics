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


def update_makefile_version(old_version, new_version):
    """Update version specifically in Makefile."""
    with open("Makefile", "r") as f:
        content = f.read()

    # Replace the IMAGE_TAG_DAGSTER line
    pattern = rf"IMAGE_TAG_DAGSTER = ghcr\.io/ethereum-optimism/op-analytics-dagster:{re.escape(old_version)}"
    replacement = (
        f"IMAGE_TAG_DAGSTER = ghcr.io/ethereum-optimism/op-analytics-dagster:{new_version}"
    )

    updated_content = re.sub(pattern, replacement, content)

    with open("Makefile", "w") as f:
        f.write(updated_content)

    log.info(f"Updated version in Makefile: {old_version} → {new_version}")


def update_helm_values_version(old_version, new_version):
    """Update version specifically in helm/dagster/values.yaml."""
    with open("helm/dagster/values.yaml", "r") as f:
        content = f.read()

    # Replace the tag field for the op-analytics-dagster image
    pattern = rf'(repository: "ghcr\.io/ethereum-optimism/op-analytics-dagster"\s*\n\s*tag: "){re.escape(old_version)}(")'
    replacement = rf"\1{new_version}\2"

    updated_content = re.sub(pattern, replacement, content)

    with open("helm/dagster/values.yaml", "w") as f:
        f.write(updated_content)

    log.info(f"Updated version in helm/dagster/values.yaml: {old_version} → {new_version}")


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

    # For Docker builds, show real-time output
    if "docker" in command.lower():
        log.info("Starting Docker build - this may take 10-20 minutes...")
        log.info("You'll see build progress below:")

        # Run with real-time output for Docker commands
        process = subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
        )

        # Stream output in real-time
        for line in process.stdout:
            print(line.rstrip())

        process.wait()
        result = subprocess.CompletedProcess(
            args=command,
            returncode=process.returncode,
            stdout="",  # Already printed above
            stderr="",
        )
    else:
        # For non-Docker commands, use the original approach
        result = subprocess.run(command, shell=True, capture_output=True, text=True)

    if result.returncode != 0:
        log.error(f"Error running: {command}")
        if "docker" not in command.lower():  # Docker errors already printed above
            log.error(f"stdout: {result.stdout}")
            log.error(f"stderr: {result.stderr}")
        raise typer.Exit(1)

    log.info(f"{description} completed successfully")
    if result.stdout.strip() and "docker" not in command.lower():
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

    # Store original content for rollback
    original_makefile_content = None
    original_values_content = None

    try:
        # Read original content
        with open("Makefile", "r") as f:
            original_makefile_content = f.read()
        with open("helm/dagster/values.yaml", "r") as f:
            original_values_content = f.read()

        # Temporarily update Makefile for Docker build
        log.info("Temporarily updating Makefile for Docker build...")
        update_makefile_version(current_version, new_version)

        # Build and push Docker image
        if not skip_build:
            run_command("make docker-dagster", "Building and pushing Dagster Docker image")

        # If we get here, the build succeeded, so commit the version changes
        log.info("Docker build succeeded! Committing version changes...")

        # Update helm values file (Makefile is already updated)
        update_helm_values_version(current_version, new_version)

        # Deploy to Helm
        if not skip_deploy:
            if check_gcloud_auth():
                run_command("make helm-dagster", "Deploying to Helm")
            else:
                if prompt_gcloud_auth():
                    run_command("make helm-dagster", "Deploying to Helm")
                else:
                    log.warning("Skipping Helm deployment due to authentication issues")
                    skip_deploy = True

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

    except Exception as e:
        log.error(f"Error during version bump: {e}")

        # Rollback changes if we have original content
        if original_makefile_content is not None:
            log.info("Rolling back Makefile changes...")
            with open("Makefile", "w") as f:
                f.write(original_makefile_content)

        if original_values_content is not None:
            log.info("Rolling back helm/dagster/values.yaml changes...")
            with open("helm/dagster/values.yaml", "w") as f:
                f.write(original_values_content)

        log.error("Version bump failed and changes have been rolled back.")
        raise typer.Exit(1)


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


def check_gcloud_auth():
    """Check if user is authenticated with Google Cloud and prompt if not."""
    log.info("Checking Google Cloud authentication...")

    # Check if gcloud is installed
    try:
        subprocess.run(["gcloud", "--version"], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        log.error("gcloud CLI is not installed or not in PATH")
        log.info("Please install gcloud CLI: https://cloud.google.com/sdk/docs/install")
        raise typer.Exit(1)

    # Check if user is authenticated and has valid tokens
    try:
        # First check if we have any active accounts
        result = subprocess.run(
            ["gcloud", "auth", "list", "--filter=status:ACTIVE", "--format=value(account)"],
            capture_output=True,
            text=True,
            check=True,
        )

        if not result.stdout.strip():
            log.warning("❌ No active Google Cloud authentication found")
            return False

        account = result.stdout.strip()
        log.info(f"✅ Found active account: {account}")

        # Test if we can actually get credentials (this is what Helm will try to do)
        log.info("Testing credential access...")
        test_result = subprocess.run(
            ["gcloud", "config", "config-helper", "--format=json"],
            capture_output=True,
            text=True,
            timeout=30,  # Add timeout to prevent hanging
        )

        if test_result.returncode != 0:
            log.warning("❌ Credential test failed - tokens may be expired")
            log.warning(f"Error: {test_result.stderr}")
            return False

        log.info("✅ Credential test passed")
        return True

    except subprocess.CalledProcessError as e:
        log.warning(f"❌ Google Cloud authentication check failed: {e}")
        return False
    except subprocess.TimeoutExpired:
        log.warning("❌ Credential test timed out")
        return False


def prompt_gcloud_auth():
    """Prompt user to authenticate with Google Cloud."""
    log.info("Google Cloud authentication required for Helm deployment")
    log.info("")
    log.info("You may need to re-authenticate if your tokens have expired.")
    log.info("")
    log.info("Please run the following commands:")
    log.info("")
    log.info("  gcloud auth login")
    log.info("  gcloud config set project oplabs-tools-data")
    log.info("  gcloud auth application-default login")
    log.info("")
    log.info("Note: The last command sets up application default credentials")
    log.info("which are required for Helm to access the Kubernetes cluster.")
    log.info("")

    response = typer.confirm("Would you like to run these commands now?")
    if response:
        log.info("Running gcloud auth login...")
        try:
            subprocess.run(["gcloud", "auth", "login"], check=True)
            log.info("Setting project to oplabs-tools-data...")
            subprocess.run(["gcloud", "config", "set", "project", "oplabs-tools-data"], check=True)
            log.info("Setting up application default credentials...")
            subprocess.run(["gcloud", "auth", "application-default", "login"], check=True)
            log.info("✅ Google Cloud authentication completed!")
            return True
        except subprocess.CalledProcessError as e:
            log.error(f"❌ Google Cloud authentication failed: {e}")
            log.info("Please run the commands manually and then re-run this script")
            return False
    else:
        log.info("Please authenticate manually and then re-run the command")
        return False
