import pytest
from unittest.mock import patch, MagicMock
import requests
from typing import Any
from op_analytics.coreutils.circleci.client import CircleCIClient
import requests.structures

REPO_OWNER = "test_owner"
CIRCLECI_TOKEN = "test_token"
PROJECT_SLUG = "gh/test_owner/test_repo"
PIPELINE_ID = "test_pipeline_id"
WORKFLOW_NAME = "test_workflow"
WORKFLOW_ID = "test_workflow_id"
JOB_NAME = "test_job"
JOB_NUMBER = 123
ARTIFACT_FILENAME = "test_artifact.txt"

EXPECTED_PROCESSED_HEADERS = {
    "Content-Type": "application/json",
    "Circle-Token": CIRCLECI_TOKEN,
    "accept": "application/json",
}


@pytest.fixture
def mock_session():
    """Fixture for a mocked requests.Session."""
    session_mock = MagicMock(spec=requests.Session)
    session_mock.headers = MagicMock(spec=requests.structures.CaseInsensitiveDict)
    session_mock.headers.items.return_value = EXPECTED_PROCESSED_HEADERS.items()
    return session_mock


@pytest.fixture
def client(mock_session):
    """Fixture for CircleCIClient with a mocked session."""
    with patch("op_analytics.coreutils.circleci.client.new_session", return_value=mock_session):
        return CircleCIClient(repo_owner=REPO_OWNER, circleci_token=CIRCLECI_TOKEN)


class TestCircleCIClient:
    def test_get_latest_pipeline_id_success(self, client, mock_session):
        """Test successfully retrieving the latest pipeline ID."""
        mock_response_data: dict[str, Any] = {
            "items": [{"id": PIPELINE_ID, "created_at": "2023-01-01T00:00:00Z"}]
        }
        with patch(
            "op_analytics.coreutils.circleci.client.get_data", return_value=mock_response_data
        ) as mock_get_data:
            pipeline_id = client.get_latest_pipeline_id(project_slug=PROJECT_SLUG, branch="main")
            assert pipeline_id == PIPELINE_ID
            mock_get_data.assert_called_once_with(
                session=mock_session,
                url=f"{client.base_url}/project/{PROJECT_SLUG}/pipeline",
                headers=EXPECTED_PROCESSED_HEADERS,
                params={"limit": 1, "branch": "main"},
            )

    def test_get_latest_pipeline_id_no_items(self, client, mock_session):
        """Test handling when no pipeline items are returned."""
        mock_response_data: dict[str, Any] = {"items": []}
        with patch(
            "op_analytics.coreutils.circleci.client.get_data", return_value=mock_response_data
        ) as mock_get_data:
            pipeline_id = client.get_latest_pipeline_id(project_slug=PROJECT_SLUG, branch="main")
            assert pipeline_id is None
            mock_get_data.assert_called_once_with(
                session=mock_session,
                url=f"{client.base_url}/project/{PROJECT_SLUG}/pipeline",
                headers=EXPECTED_PROCESSED_HEADERS,
                params={"limit": 1, "branch": "main"},
            )

    def test_get_latest_pipeline_id_missing_id(self, client, mock_session):
        """Test handling when a pipeline item is missing the 'id' field."""
        mock_response_data: dict[str, Any] = {"items": [{"created_at": "2023-01-01T00:00:00Z"}]}
        with patch(
            "op_analytics.coreutils.circleci.client.get_data", return_value=mock_response_data
        ) as mock_get_data:
            pipeline_id = client.get_latest_pipeline_id(project_slug=PROJECT_SLUG, branch="main")
            assert pipeline_id is None
            mock_get_data.assert_called_once_with(
                session=mock_session,
                url=f"{client.base_url}/project/{PROJECT_SLUG}/pipeline",
                headers=EXPECTED_PROCESSED_HEADERS,
                params={"limit": 1, "branch": "main"},
            )

    def test_get_workflow_id_by_name_success(self, client, mock_session):
        """Test successfully retrieving a workflow ID by name."""
        mock_response_data: dict[str, Any] = {
            "items": [
                {"name": "other_workflow", "id": "other_id", "status": "success"},
                {"name": WORKFLOW_NAME, "id": WORKFLOW_ID, "status": "success"},
            ]
        }
        with patch(
            "op_analytics.coreutils.circleci.client.get_data", return_value=mock_response_data
        ) as mock_get_data:
            workflow_id = client.get_workflow_id_by_name(
                pipeline_id=PIPELINE_ID, workflow_name=WORKFLOW_NAME
            )
            assert workflow_id == WORKFLOW_ID
            mock_get_data.assert_called_once_with(
                session=mock_session,
                url=f"{client.base_url}/pipeline/{PIPELINE_ID}/workflow",
                headers=EXPECTED_PROCESSED_HEADERS,
            )

    def test_get_workflow_id_by_name_not_found(self, client, mock_session):
        """Test handling when the workflow name is not found."""
        mock_response_data: dict[str, Any] = {
            "items": [{"name": "other_workflow", "id": "other_id", "status": "success"}]
        }
        with patch(
            "op_analytics.coreutils.circleci.client.get_data", return_value=mock_response_data
        ) as mock_get_data:
            workflow_id = client.get_workflow_id_by_name(
                pipeline_id=PIPELINE_ID, workflow_name="non_existent_workflow"
            )
            assert workflow_id is None
            mock_get_data.assert_called_once_with(
                session=mock_session,
                url=f"{client.base_url}/pipeline/{PIPELINE_ID}/workflow",
                headers=EXPECTED_PROCESSED_HEADERS,
            )

    def test_get_workflow_id_by_name_missing_id(self, client, mock_session):
        """Test handling when a matching workflow is missing the 'id' field."""
        mock_response_data: dict[str, Any] = {
            "items": [{"name": WORKFLOW_NAME, "status": "success"}]
        }
        with patch(
            "op_analytics.coreutils.circleci.client.get_data", return_value=mock_response_data
        ) as mock_get_data:
            workflow_id = client.get_workflow_id_by_name(
                pipeline_id=PIPELINE_ID, workflow_name=WORKFLOW_NAME
            )
            assert workflow_id is None
            mock_get_data.assert_called_once_with(
                session=mock_session,
                url=f"{client.base_url}/pipeline/{PIPELINE_ID}/workflow",
                headers=EXPECTED_PROCESSED_HEADERS,
            )

    def test_get_job_number_by_name_success(self, client, mock_session):
        """Test successfully retrieving a job number by name."""
        mock_response_data: dict[str, Any] = {
            "items": [
                {"name": "other_job", "job_number": 999, "id": "other_job_id", "status": "success"},
                {
                    "name": JOB_NAME,
                    "job_number": JOB_NUMBER,
                    "id": "test_job_id",
                    "status": "success",
                },
            ]
        }
        with patch(
            "op_analytics.coreutils.circleci.client.get_data", return_value=mock_response_data
        ) as mock_get_data:
            job_number = client.get_job_number_by_name(workflow_id=WORKFLOW_ID, job_name=JOB_NAME)
            assert job_number == JOB_NUMBER
            mock_get_data.assert_called_once_with(
                session=mock_session,
                url=f"{client.base_url}/workflow/{WORKFLOW_ID}/job",
                headers=EXPECTED_PROCESSED_HEADERS,
            )

    def test_get_job_number_by_name_not_found(self, client, mock_session):
        """Test handling when the job name is not found."""
        mock_response_data: dict[str, Any] = {
            "items": [
                {"name": "other_job", "job_number": 999, "id": "other_job_id", "status": "success"}
            ]
        }
        with patch(
            "op_analytics.coreutils.circleci.client.get_data", return_value=mock_response_data
        ) as mock_get_data:
            job_number = client.get_job_number_by_name(
                workflow_id=WORKFLOW_ID, job_name="non_existent_job"
            )
            assert job_number is None
            mock_get_data.assert_called_once_with(
                session=mock_session,
                url=f"{client.base_url}/workflow/{WORKFLOW_ID}/job",
                headers=EXPECTED_PROCESSED_HEADERS,
            )

    def test_get_job_number_by_name_missing_job_number(self, client, mock_session):
        """Test handling when a matching job is missing the 'job_number' field."""
        mock_response_data: dict[str, Any] = {
            "items": [{"name": JOB_NAME, "id": "test_job_id", "status": "success"}]
        }
        with patch(
            "op_analytics.coreutils.circleci.client.get_data", return_value=mock_response_data
        ) as mock_get_data:
            job_number = client.get_job_number_by_name(workflow_id=WORKFLOW_ID, job_name=JOB_NAME)
            assert job_number is None
            mock_get_data.assert_called_once_with(
                session=mock_session,
                url=f"{client.base_url}/workflow/{WORKFLOW_ID}/job",
                headers=EXPECTED_PROCESSED_HEADERS,
            )

    def test_download_artifact_text_by_filename_success(self, client, mock_session):
        """Test successfully downloading artifact text by filename."""
        artifact_url = f"https://example.com/artifacts/{ARTIFACT_FILENAME}"
        mock_artifacts_list_response: dict[str, Any] = {
            "items": [
                {"path": "some/other/file.txt", "url": "https://example.com/artifacts/other.txt"},
                {"path": f"path/to/{ARTIFACT_FILENAME}", "url": artifact_url},
            ]
        }
        mock_artifact_content = "This is the artifact content."

        mock_get_data_response = MagicMock()
        mock_get_data_response.text = mock_artifact_content

        with (
            patch(
                "op_analytics.coreutils.circleci.client.get_data",
                return_value=mock_artifacts_list_response,
            ) as mock_get_data_list,
            patch("requests.get", return_value=mock_get_data_response) as mock_requests_get,
        ):
            content = client.download_artifact_text_by_filename(
                project_slug=PROJECT_SLUG,
                job_number=JOB_NUMBER,
                artifact_filename=ARTIFACT_FILENAME,
            )

            assert content == mock_artifact_content
            mock_get_data_list.assert_called_once_with(
                session=mock_session,
                url=f"{client.base_url}/project/{PROJECT_SLUG}/{JOB_NUMBER}/artifacts",
                headers=EXPECTED_PROCESSED_HEADERS,
            )
            mock_requests_get.assert_called_once_with(artifact_url, timeout=60)
            mock_get_data_response.raise_for_status.assert_called_once()

    def test_download_artifact_text_by_filename_not_found(self, client, mock_session):
        """Test handling when the artifact filename is not found in the list."""
        mock_artifacts_list_response: dict[str, Any] = {
            "items": [
                {"path": "some/other/file.txt", "url": "https://example.com/artifacts/other.txt"}
            ]
        }
        with (
            patch(
                "op_analytics.coreutils.circleci.client.get_data",
                return_value=mock_artifacts_list_response,
            ) as mock_get_data_list,
            patch("requests.get") as mock_requests_get,
        ):
            content = client.download_artifact_text_by_filename(
                project_slug=PROJECT_SLUG,
                job_number=JOB_NUMBER,
                artifact_filename="non_existent_artifact.txt",
            )

            assert content is None
            mock_get_data_list.assert_called_once_with(
                session=mock_session,
                url=f"{client.base_url}/project/{PROJECT_SLUG}/{JOB_NUMBER}/artifacts",
                headers=EXPECTED_PROCESSED_HEADERS,
            )
            mock_requests_get.assert_not_called()

    def test_download_artifact_text_by_filename_missing_url(self, client, mock_session):
        """Test handling when found artifact item is missing the 'url' field."""
        mock_artifacts_list_response: dict[str, Any] = {
            "items": [{"path": f"path/to/{ARTIFACT_FILENAME}"}]
        }
        with (
            patch(
                "op_analytics.coreutils.circleci.client.get_data",
                return_value=mock_artifacts_list_response,
            ) as mock_get_data_list,
            patch("requests.get") as mock_requests_get,
        ):
            content = client.download_artifact_text_by_filename(
                project_slug=PROJECT_SLUG,
                job_number=JOB_NUMBER,
                artifact_filename=ARTIFACT_FILENAME,
            )

            assert content is None
            mock_get_data_list.assert_called_once_with(
                session=mock_session,
                url=f"{client.base_url}/project/{PROJECT_SLUG}/{JOB_NUMBER}/artifacts",
                headers=EXPECTED_PROCESSED_HEADERS,
            )
            mock_requests_get.assert_not_called()

    def test_download_artifact_text_by_filename_download_fails(self, client, mock_session):
        """Test handling when downloading the artifact content fails."""
        artifact_url = f"https://example.com/artifacts/{ARTIFACT_FILENAME}"
        mock_artifacts_list_response: dict[str, Any] = {
            "items": [{"path": f"path/to/{ARTIFACT_FILENAME}", "url": artifact_url}]
        }

        mock_get_data_response = MagicMock()
        mock_get_data_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "Download failed"
        )

        with (
            patch(
                "op_analytics.coreutils.circleci.client.get_data",
                return_value=mock_artifacts_list_response,
            ) as mock_get_data_list,
            patch("requests.get", return_value=mock_get_data_response) as mock_requests_get,
        ):
            with pytest.raises(requests.exceptions.HTTPError, match="Download failed"):
                client.download_artifact_text_by_filename(
                    project_slug=PROJECT_SLUG,
                    job_number=JOB_NUMBER,
                    artifact_filename=ARTIFACT_FILENAME,
                )

            mock_get_data_list.assert_called_once_with(
                session=mock_session,
                url=f"{client.base_url}/project/{PROJECT_SLUG}/{JOB_NUMBER}/artifacts",
                headers=EXPECTED_PROCESSED_HEADERS,
            )
            mock_requests_get.assert_called_once_with(artifact_url, timeout=60)
            mock_get_data_response.raise_for_status.assert_called_once()

    def test_client_initialization(self):
        """Test client initialization and attribute setup."""
        owner = "my_org"
        token = "my_token"

        temp_mock_session = MagicMock(spec=requests.Session)
        temp_mock_session.headers = MagicMock(spec=requests.structures.CaseInsensitiveDict)

        instance_expected_headers = {
            "Content-Type": "application/json",
            "Circle-Token": token,
            "accept": "application/json",
        }

        with patch(
            "op_analytics.coreutils.circleci.client.new_session", return_value=temp_mock_session
        ) as mock_new_session:
            client_instance = CircleCIClient(repo_owner=owner, circleci_token=token)

            assert client_instance.repo_owner == owner
            assert client_instance.circleci_token == token
            assert client_instance.base_url == "https://circleci.com/api/v2"
            assert (
                client_instance.circleci_api_url
                == f"https://circleci.com/api/v2/project/gh/{owner}/{{repo}}/pipeline"
            )

            mock_new_session.assert_called_once()
            temp_mock_session.headers.update.assert_called_once_with(instance_expected_headers)
