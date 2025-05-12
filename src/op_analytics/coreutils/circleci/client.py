from typing import Optional, Union
import requests
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.request import new_session, get_data

log = structlog.get_logger()


class CircleCIClient:
    """CircleCI client for GET operations, using coreutils.request.get_data directly."""

    circleci_api_url_template: str = (
        "https://circleci.com/api/v2/project/gh/{owner}/{repo}/pipeline"
    )

    def __init__(self, repo_owner: str, circleci_token: str):
        """Initialize the CircleCI client."""
        self.repo_owner = repo_owner
        self.circleci_token = circleci_token

        self.session = new_session()
        self.session.headers.update(
            {
                "Content-Type": "application/json",
                "Circle-Token": self.circleci_token,
                "accept": "application/json",
            }
        )

        self.base_url = "https://circleci.com/api/v2"
        self.circleci_api_url = self.circleci_api_url_template.format(
            owner=self.repo_owner, repo="{repo}"
        )

    def get_latest_pipeline_id(
        self, project_slug: str, branch: Optional[str] = None
    ) -> Optional[str]:
        """Get ID of the latest pipeline for a project and branch."""
        endpoint = f"/project/{project_slug}/pipeline"
        url = f"{self.base_url}{endpoint}"
        api_params = {"limit": 1, "branch": branch}

        log.info(
            "Requesting latest pipeline ID from CircleCI", endpoint=endpoint, params=api_params
        )
        processed_headers: dict[str, str] = {
            k: v.decode("utf-8") if isinstance(v, bytes) else v
            for k, v in self.session.headers.items()
        }
        pipelines_data = get_data(
            session=self.session, url=url, headers=processed_headers, params=api_params
        )

        items = pipelines_data.get("items")
        if not items:
            log.warn(
                "No pipeline items found in API response",
                project_slug=project_slug,
                branch=branch,
                response_keys=pipelines_data.keys(),
            )
            return None

        latest_pipeline = items[0]
        pipeline_id = latest_pipeline.get("id")
        if not pipeline_id:
            log.error("Latest pipeline item is missing an ID", pipeline_item=latest_pipeline)
            return None

        log.info(
            "Found latest pipeline",
            pipeline_id=pipeline_id,
            created_at=latest_pipeline.get("created_at"),
        )
        return pipeline_id

    def get_workflow_id_by_name(self, pipeline_id: str, workflow_name: str) -> Optional[str]:
        """Get a workflow ID by name from a pipeline."""
        endpoint = f"/pipeline/{pipeline_id}/workflow"
        url = f"{self.base_url}{endpoint}"

        log.info(
            "Requesting workflow ID by name from CircleCI",
            endpoint=endpoint,
            workflow_name=workflow_name,
        )
        processed_headers_workflow: dict[str, str] = {
            k: v.decode("utf-8") if isinstance(v, bytes) else v
            for k, v in self.session.headers.items()
        }
        workflows_data = get_data(session=self.session, url=url, headers=processed_headers_workflow)

        for workflow in workflows_data.get("items", []):
            if workflow.get("name") == workflow_name:
                workflow_id = workflow.get("id")
                if workflow_id:
                    log.info(
                        "Found workflow",
                        workflow_id=workflow_id,
                        name=workflow_name,
                        status=workflow.get("status"),
                    )
                    return workflow_id
                else:
                    log.warn(
                        "Found matching workflow by name but it has no ID",
                        workflow_details=workflow,
                    )

        log.warn("Workflow not found by name", pipeline_id=pipeline_id, workflow_name=workflow_name)
        return None

    def get_job_number_by_name(self, workflow_id: str, job_name: str) -> Optional[Union[str, int]]:
        """Get a job number by name from a workflow."""
        endpoint = f"/workflow/{workflow_id}/job"
        url = f"{self.base_url}{endpoint}"

        log.info(
            "Requesting job number by name from CircleCI", endpoint=endpoint, job_name=job_name
        )
        processed_headers_job: dict[str, str] = {
            k: v.decode("utf-8") if isinstance(v, bytes) else v
            for k, v in self.session.headers.items()
        }
        jobs_data = get_data(session=self.session, url=url, headers=processed_headers_job)

        for job in jobs_data.get("items", []):
            if job.get("name") == job_name:
                job_number = job.get("job_number")
                if job_number is not None:
                    log.info(
                        "Found job",
                        id=job.get("id"),
                        name=job_name,
                        status=job.get("status"),
                        job_number=job_number,
                    )
                    return job_number
                else:
                    log.warn("Found matching job by name but it has no job_number", job_details=job)

        log.warn("Job not found by name", workflow_id=workflow_id, job_name=job_name)
        return None

    def download_artifact_text_by_filename(
        self, project_slug: str, job_number: Union[str, int], artifact_filename: str
    ) -> Optional[str]:
        """Download text content of a specific artifact by filename from a job."""
        endpoint = f"/project/{project_slug}/{job_number}/artifacts"
        url = f"{self.base_url}{endpoint}"

        log.info("Requesting artifact list from CircleCI", endpoint=endpoint)
        processed_headers_artifacts: dict[str, str] = {
            k: v.decode("utf-8") if isinstance(v, bytes) else v
            for k, v in self.session.headers.items()
        }
        artifacts_data = get_data(
            session=self.session, url=url, headers=processed_headers_artifacts
        )

        artifact_to_download = None
        for artifact_item in artifacts_data.get("items", []):
            item_path = artifact_item.get("path")
            if isinstance(item_path, str) and item_path.endswith(artifact_filename):
                artifact_to_download = artifact_item
                break

        if not artifact_to_download:
            log.warn(
                "Artifact not found by filename",
                project_slug=project_slug,
                job_number=job_number,
                filename=artifact_filename,
            )
            return None

        artifact_url = artifact_to_download.get("url")
        if not artifact_url:
            log.error(
                "Found artifact by path, but it is missing a URL",
                artifact_details=artifact_to_download,
            )
            return None

        log.info(
            "Found artifact, attempting download",
            path=artifact_to_download.get("path"),
            url=artifact_url,
        )

        response = requests.get(artifact_url, timeout=60)
        response.raise_for_status()
        log.info("Artifact downloaded successfully", url=artifact_url)
        return response.text
