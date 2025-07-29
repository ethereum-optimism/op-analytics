"""
Unit tests for the functional ingestors in the chain metadata pipeline.
"""

import pytest
import polars as pl
from unittest.mock import patch, MagicMock
import pandas as pd

from op_analytics.datapipeline.chains import ingestors


@pytest.fixture
def sample_csv_path(tmp_path):
    csv_content = """chain_name,other_col
test_chain_1,val1
test_chain_2,val2
"""
    csv_file = tmp_path / "sample_chain_metadata.csv"
    csv_file.write_text(csv_content)
    return str(csv_file)


@pytest.fixture
def mock_l2beat_api():
    with patch(
        "op_analytics.datasources.l2beat.projects.L2BeatProjectsSummary.fetch"
    ) as mock_fetch:
        mock_df = pl.DataFrame(
            {
                "id": ["l2beat_chain_1"],
                "name": ["L2Beat Chain 1"],
                "stage": ["Stage 1"],
                "da_badge": ["DA Badge"],
                "category": ["Category"],
                "vm_badge": ["VM Badge"],
            }
        )
        mock_response = MagicMock()
        mock_response.summary_df = mock_df
        mock_fetch.return_value = mock_response
        yield mock_fetch


@pytest.fixture
def mock_defillama_api():
    with patch(
        "op_analytics.datasources.defillama.chaintvl.metadata.ChainsMetadata.fetch"
    ) as mock_fetch:
        mock_df = pl.DataFrame({"chain_name": ["defillama_chain_1"], "symbol": ["DLLAMA"]})
        mock_response = MagicMock()
        mock_response.df = mock_df
        mock_fetch.return_value = mock_response
        yield mock_fetch


@pytest.fixture
def mock_dune_api():
    with patch("op_analytics.datasources.dune.dextrades.DuneDexTradesSummary.fetch") as mock_fetch:
        mock_response = MagicMock()
        mock_response.df = pl.DataFrame(
            {
                "blockchain": ["dune_chain_1"],
                "project": ["Dune Project"],
                "version": ["v1"],
                "trades": [100],
            }
        )
        mock_fetch.return_value = mock_response
        yield mock_fetch


@pytest.fixture
def mock_bigquery_client():
    with patch("op_analytics.coreutils.bigquery.client._CLIENT") as mock_client:
        yield mock_client


def test_ingest_from_csv(sample_csv_path):
    df = ingestors.ingest_from_csv(sample_csv_path)
    assert df.shape[0] == 2
    assert "chain_key" in df.columns


def test_ingest_from_l2beat(mock_l2beat_api):
    df = ingestors.ingest_from_l2beat()
    assert df.shape[0] == 1
    assert "display_name" in df.columns
    assert df["display_name"][0] == "L2Beat Chain 1"


def test_ingest_from_defillama(mock_defillama_api):
    df = ingestors.ingest_from_defillama()
    assert df.shape[0] == 1
    assert "gas_token" in df.columns
    assert df["gas_token"][0] == "DLLAMA"


def test_ingest_from_dune(mock_dune_api):
    df = ingestors.ingest_from_dune()
    assert df.shape[0] == 1
    assert "display_name" in df.columns


def test_ingest_from_bq_op_stack(mock_bigquery_client):
    mock_query_job = MagicMock()
    mock_query_job.to_dataframe.return_value = pd.DataFrame(
        {
            "chain_name": ["bq_op_stack_1"],
            "mainnet_chain_id": [123],
            "public_mainnet_launch_date": ["2022-01-01"],
        }
    )
    mock_bigquery_client.query.return_value = mock_query_job

    df = ingestors.ingest_from_bq_op_stack(project_id="p", dataset_id="d")
    assert df.shape[0] == 1


def test_ingest_from_bq_goldsky(mock_bigquery_client):
    mock_query_job = MagicMock()
    mock_query_job.to_dataframe.return_value = pd.DataFrame({"chain_name": ["bq_goldsky_1"]})
    mock_bigquery_client.query.return_value = mock_query_job

    df = ingestors.ingest_from_bq_goldsky(project_id="p", dataset_id="d")
    assert df.shape[0] == 1


def test_empty_dataframe_handling(monkeypatch):
    monkeypatch.setattr(pl, "read_csv", lambda *args, **kwargs: pl.DataFrame())
    with pytest.raises(ValueError, match="Empty DataFrame from CSV Data"):
        ingestors.ingest_from_csv("dummy_path")
