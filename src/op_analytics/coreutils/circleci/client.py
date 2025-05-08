from dataclasses import dataclass
from typing import Any, Optional, Union
import requests

from op_analytics.coreutils.logger import structlog

log = structlog.get_logger()


@dataclass
class CircleCIClient:
    """CircleCI client for interacting with CircleCI API."""

    api_token: str
    base_url: str = "https://circleci.com/api/v2"

    def __post_init__(self):
        """Post-initialization setup."""
        self.headers = {
            "circle-token": self.api_token,
            "accept": "application/json",
        }

    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict[str, Any]] = None,
        json_data: Optional[dict[str, Any]] = None,
    ) -> Optional[dict[str, Any]]:
        """Make a request to the CircleCI API.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint to call
            params: Query parameters
            json_data: JSON data for POST requests

        Returns:
            API response as JSON or None if request failed
        """
        url = f"{self.base_url}{endpoint}"
        request_params = params or {}

        # Ensure token is included in all requests
        if "circle-token" not in request_params:
            request_params["circle-token"] = self.api_token

        log.info(f"Making {method} request to CircleCI API", endpoint=endpoint)

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                params=request_params,
                json=json_data,
                timeout=30,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            log.error(
                "CircleCI API request failed",
                endpoint=endpoint,
                status_code=getattr(e.response, "status_code", None),
                error=str(e),
            )
            return None

    def get_pipelines(
        self,
        project_slug: str,
        branch: Optional[str] = None,
        page_token: Optional[str] = None,
        limit: int = 20,
    ) -> Optional[dict[str, Any]]:
        """Get pipelines for a project.

        Args:
            project_slug: Project slug (e.g., 'gh/org/repo')
            branch: Optional branch name to filter by
            page_token: Optional pagination token
            limit: Number of results per page (default: 20)

        Returns:
            Pipeline data or None if request failed
        """
        params = {"limit": limit}

        if branch:
            params["branch"] = branch

        if page_token:
            params["page-token"] = page_token

        return self._make_request("GET", f"/project/{project_slug}/pipeline", params=params)

    def get_latest_pipeline_id(
        self, project_slug: str, branch: Optional[str] = None
    ) -> Optional[str]:
        """Get the ID of the latest pipeline for a project.

        Args:
            project_slug: Project slug (e.g., 'gh/org/repo')
            branch: Optional branch name to filter by

        Returns:
            Pipeline ID or None if not found
        """
        pipelines = self.get_pipelines(project_slug, branch, limit=1)

        if not pipelines or "items" not in pipelines or not pipelines["items"]:
            log.error("No pipelines found", project_slug=project_slug, branch=branch)
            return None

        latest_pipeline = pipelines["items"][0]
        log.info(
            "Found latest pipeline",
            pipeline_id=latest_pipeline["id"],
            created_at=latest_pipeline.get("created_at"),
        )
        return latest_pipeline["id"]

    def get_workflows(
        self, pipeline_id: str, page_token: Optional[str] = None, limit: int = 20
    ) -> Optional[dict[str, Any]]:
        """Get workflows for a pipeline.

        Args:
            pipeline_id: Pipeline ID
            page_token: Optional pagination token
            limit: Number of results per page (default: 20)

        Returns:
            Workflow data or None if request failed
        """
        params = {"limit": limit}

        if page_token:
            params["page-token"] = page_token

        return self._make_request("GET", f"/pipeline/{pipeline_id}/workflow", params=params)

    def get_workflow_by_name(
        self, pipeline_id: str, workflow_name: str
    ) -> Optional[dict[str, Any]]:
        """Get a workflow by name from a pipeline.

        Args:
            pipeline_id: Pipeline ID
            workflow_name: Workflow name to find

        Returns:
            Workflow data or None if not found
        """
        workflows = self.get_workflows(pipeline_id)

        if not workflows or "items" not in workflows:
            log.error("No workflows found", pipeline_id=pipeline_id)
            return None

        for workflow in workflows["items"]:
            if workflow["name"] == workflow_name:
                log.info(
                    "Found workflow",
                    workflow_id=workflow["id"],
                    workflow_name=workflow["name"],
                    workflow_status=workflow["status"],
                )
                return workflow

        log.error("Workflow not found", pipeline_id=pipeline_id, workflow_name=workflow_name)
        return None

    def get_jobs(
        self, workflow_id: str, page_token: Optional[str] = None, limit: int = 20
    ) -> Optional[dict[str, Any]]:
        """Get jobs for a workflow.

        Args:
            workflow_id: Workflow ID
            page_token: Optional pagination token
            limit: Number of results per page (default: 20)

        Returns:
            Job data or None if request failed
        """
        params = {"limit": limit}

        if page_token:
            params["page-token"] = page_token

        return self._make_request("GET", f"/workflow/{workflow_id}/job", params=params)

    def get_job_by_name(self, workflow_id: str, job_name: str) -> Optional[dict[str, Any]]:
        """Get a job by name from a workflow.

        Args:
            workflow_id: Workflow ID
            job_name: Job name to find

        Returns:
            Job data or None if not found
        """
        jobs = self.get_jobs(workflow_id)

        if not jobs or "items" not in jobs:
            log.error("No jobs found", workflow_id=workflow_id)
            return None

        for job in jobs["items"]:
            if job["name"] == job_name:
                log.info(
                    "Found job", job_id=job["id"], job_name=job["name"], job_status=job["status"]
                )
                return job

        log.error("Job not found", workflow_id=workflow_id, job_name=job_name)
        return None

    def get_artifacts(
        self,
        project_slug: str,
        job_number: Union[str, int],
        page_token: Optional[str] = None,
        limit: int = 20,
    ) -> Optional[dict[str, Any]]:
        """Get artifacts for a job.

        Args:
            project_slug: Project slug (e.g., 'gh/org/repo')
            job_number: Job number
            page_token: Optional pagination token
            limit: Number of results per page (default: 20)

        Returns:
            Artifact data or None if request failed
        """
        params = {"limit": limit}

        if page_token:
            params["page-token"] = page_token

        return self._make_request(
            "GET", f"/project/{project_slug}/{job_number}/artifacts", params=params
        )

    def get_artifact_by_path(
        self, project_slug: str, job_number: Union[str, int], artifact_path: str
    ) -> Optional[dict[str, Any]]:
        """Get an artifact by path.

        Args:
            project_slug: Project slug (e.g., 'gh/org/repo')
            job_number: Job number
            artifact_path: Artifact path to find (can be partial match)

        Returns:
            Artifact data or None if not found
        """
        artifacts = self.get_artifacts(project_slug, job_number)

        if not artifacts or "items" not in artifacts:
            log.error("No artifacts found", project_slug=project_slug, job_number=job_number)
            return None

        for artifact in artifacts["items"]:
            if artifact_path in artifact["path"]:
                log.info(
                    "Found artifact", artifact_path=artifact["path"], artifact_url=artifact["url"]
                )
                return artifact

        log.error(
            "Artifact not found",
            project_slug=project_slug,
            job_number=job_number,
            artifact_path=artifact_path,
        )
        return None

    def download_artifact_content(
        self, artifact_url: str, binary: bool = False
    ) -> Optional[Union[str, bytes]]:
        """Download artifact content.

        Args:
            artifact_url: URL of the artifact to download
            binary: Whether to return binary content (default: False)

        Returns:
            Artifact content as string/bytes or None if download failed
        """
        log.info("Downloading artifact", url=artifact_url)

        try:
            response = requests.get(artifact_url, timeout=30)
            response.raise_for_status()

            log.info("Artifact downloaded successfully")
            return response.content if binary else response.text
        except requests.exceptions.RequestException as e:
            log.error(
                "Failed to download artifact",
                url=artifact_url,
                status_code=getattr(e.response, "status_code", None),
                error=str(e),
            )
            return None

    def get_artifact_content_by_path(
        self,
        project_slug: str,
        job_number: Union[str, int],
        artifact_path: str,
        binary: bool = False,
    ) -> Optional[Union[str, bytes]]:
        """Get the content of an artifact by path.

        Args:
            project_slug: Project slug (e.g., 'gh/org/repo')
            job_number: Job number
            artifact_path: Artifact path to find (can be partial match)
            binary: Whether to return binary content (default: False)

        Returns:
            Artifact content as string/bytes or None if not found/download failed
        """
        artifact = self.get_artifact_by_path(project_slug, job_number, artifact_path)

        if not artifact:
            return None

        return self.download_artifact_content(artifact["url"], binary)
