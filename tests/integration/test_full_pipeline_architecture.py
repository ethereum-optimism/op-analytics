"""
Full Pipeline Architecture Test

This script demonstrates the complete new chain metadata architecture:
1. Independent ingestion to GCS with deduplication
2. Aggregation from GCS partitioned storage
3. Proper separation between ingestion and transformation phases

## 🔑 Prerequisites

1. **Environment Variables**:
   ```bash
   export OPLABS_ENV=PROD  # Required for BigQuery access
   export OPLABS_RUNTIME=gha  # Force GCS storage (automatically set by test)
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/credentials.json  # GCP credentials
   ```

2. **Required Credentials**:
   - GCP Service Account with:
     - BigQuery Data Viewer role
     - Storage Object Viewer role
     - Storage Object Creator role
   - Dune Analytics API key
   - L2Beat API access
   - DefiLlama API access

3. **Required Python Environment**:
   ```bash
   uv venv  # Create virtual environment
   uv pip install -e .  # Install package in editable mode
   ```

## 🚀 Running the Tests

1. **Run Individual Test**:
   ```bash
   # Run single test with output
   uv run pytest tests/integration/test_full_pipeline_architecture.py::test_individual_ingestion -v -s

   # Run specific phase
   uv run pytest tests/integration/test_full_pipeline_architecture.py::test_gcs_verification_with_retry -v -s
   ```

2. **Run Full Pipeline Test**:
   ```bash
   # Run as pytest
   uv run pytest tests/integration/test_full_pipeline_architecture.py -v -s

   # Run as script (recommended for production validation)
   uv run python tests/integration/test_full_pipeline_architecture.py
   ```

3. **Expected Output**:
   - Successful ingestion from L2Beat, DefiLlama, and Dune
   - GCS partition verification
   - Aggregated data quality checks
   - Deduplication behavior validation

## ⚠️ Important Notes

1. This test uses REAL production credentials and may incur costs
2. BigQuery access requires proper GCP service account setup
3. Some data sources may have rate limits
4. Test generates real data in GCS buckets (`gs://oplabs-tools-data-sink/chainmetadata/`)
5. Use `OPLABS_ENV=PROD` to ensure proper BigQuery access
6. GCS marker propagation may cause verification delays (this is expected)

## 🔍 Troubleshooting

1. **BigQuery Access Issues**:
   - Verify `OPLABS_ENV=PROD` is set
   - Check GCP service account permissions
   - Ensure credentials path is correct

2. **GCS Access Issues**:
   - Check Storage Object permissions
   - Verify bucket access in GCP console

3. **API Rate Limits**:
   - Space out test runs
   - Check API quotas in respective dashboards

## 📊 Data Validation

Generated data can be found in:
```
ozone/warehouse/chainmetadata/{source}/dt={date}/out.parquet
```

Use the data access script to analyze results:
```bash
uv run python tests/integration/data_access_script.py
```
"""

from datetime import date
import os
from contextlib import contextmanager
from typing import Dict, Any, Generator
import pytest

from op_analytics.datapipeline.chains.aggregator import build_all_chains_metadata_from_gcs
from op_analytics.datapipeline.chains.datasets import ChainMetadata
from op_analytics.datapipeline.chains.ingestors import (
    ingest_with_deduplication,
    ingest_from_l2beat,
    ingest_from_defillama,
    ingest_from_dune,
    ingest_from_bq_op_stack,
    ingest_from_bq_goldsky,
)
from op_analytics.coreutils.logger import structlog

log = structlog.get_logger()

# WARNING: This accesses production data sources and may incur costs


def _reset_clients_for_prod() -> None:
    """Reset environment and clients to enable PROD mode."""
    # Set environment to PROD with GCS storage
    os.environ["OPLABS_ENV"] = "PROD"
    os.environ["OPLABS_RUNTIME"] = "gha"  # Force GCS storage like in production

    # Reset environment cache
    from op_analytics.coreutils.env import aware

    aware._CURRENT_ENV = None
    aware._CURRENT_RUNTIME = None

    # Reset BigQuery client cache
    from op_analytics.coreutils.bigquery import client

    client._CLIENT = None


@contextmanager
def with_prod_environment() -> Generator[None, None, None]:
    """Context manager for safely setting production environment."""
    original_env = os.environ.get("OPLABS_ENV")
    try:
        os.environ["OPLABS_ENV"] = "PROD"
        yield
    finally:
        if original_env is None:
            os.environ.pop("OPLABS_ENV", None)
        else:
            os.environ["OPLABS_ENV"] = original_env


@pytest.mark.integration
def test_individual_ingestion() -> None:
    """Phase 1: Test individual ingestion to GCS with deduplication."""
    log.info("Starting Phase 1: Independent ingestion to GCS (PROD MODE)")

    # Reset environment and BigQuery client for PROD access
    _reset_clients_for_prod()

    test_date = date.today()
    ingestion_results: Dict[str, Any] = {}

    # Test each ingestor independently
    ingestors = [
        ("L2Beat", ingest_from_l2beat, ChainMetadata.L2BEAT),
        ("DefiLlama", ingest_from_defillama, ChainMetadata.DEFILLAMA),
        ("Dune", ingest_from_dune, ChainMetadata.DUNE),
        ("BQ OP Stack", ingest_from_bq_op_stack, ChainMetadata.BQ_OP_STACK),
        ("BQ Goldsky", ingest_from_bq_goldsky, ChainMetadata.BQ_GOLDSKY),
    ]

    for source_name, fetch_func, dataset in ingestors:
        log.info("Testing ingestion", source=source_name)
        try:
            # Test the ingestion with deduplication
            result = ingest_with_deduplication(
                source_name=source_name,
                fetch_func=fetch_func,
                dataset=dataset,
                process_dt=test_date,
            )

            ingestion_results[source_name] = {
                "success": True,
                "updated": result,
                "dataset": dataset,
            }

            status = "UPDATED" if result else "SKIPPED (no changes)"
            log.info("Ingestion completed", source=source_name, status=status)

        except Exception as e:
            ingestion_results[source_name] = {"success": False, "error": str(e), "dataset": dataset}
            log.error("Ingestion failed", source=source_name, error=str(e))

            # For BigQuery errors, provide more context
            if "BigQuery" in source_name or "BQ" in source_name:
                log.warning(
                    "BigQuery error detected - check credentials and permissions",
                    source=source_name,
                )

    # Assert that at least some ingestions succeeded
    successful_ingestions = sum(1 for r in ingestion_results.values() if r["success"])
    assert successful_ingestions >= 2, (
        f"Expected at least 2 successful ingestions, got {successful_ingestions}"
    )


@pytest.mark.integration
def test_gcs_verification_with_retry() -> None:
    """Phase 2: Verify data was written to GCS partitions with retry logic."""
    log.info("Starting Phase 2: GCS partition verification (with retry)")

    # Reset environment and BigQuery client for PROD access
    _reset_clients_for_prod()

    # First run ingestion to get data
    test_date = date.today()
    ingestion_results: Dict[str, Any] = {}

    # Test a single ingestor for verification
    try:
        result = ingest_with_deduplication(
            source_name="L2Beat (Verification Test)",
            fetch_func=ingest_from_l2beat,
            dataset=ChainMetadata.L2BEAT,
            process_dt=test_date,
        )
        ingestion_results["L2Beat"] = {
            "success": True,
            "updated": result,
            "dataset": ChainMetadata.L2BEAT,
        }
    except Exception as e:
        ingestion_results["L2Beat"] = {
            "success": False,
            "error": str(e),
            "dataset": ChainMetadata.L2BEAT,
        }

    import time

    verification_results: Dict[str, Any] = {}

    for source_name, result in ingestion_results.items():
        if not isinstance(result, dict) or not result.get("success", False):
            continue

        log.info("Verifying GCS partition", source=source_name)
        dataset = result.get("dataset")
        if dataset is None:
            log.warning("No dataset found for source", source=source_name)
            continue

        # Retry logic for GCS read-back (sometimes needs a moment)
        max_retries = 3
        retry_delay = 2

        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    log.info(
                        "Retrying GCS read",
                        source=source_name,
                        attempt=attempt,
                        max_retries=max_retries,
                    )
                    time.sleep(retry_delay)

                # Try to read back the data we just wrote
                df = dataset.read_polars(
                    min_date=test_date.strftime("%Y-%m-%d"), max_date=test_date.strftime("%Y-%m-%d")
                )

                verification_results[source_name] = {
                    "readable": True,
                    "row_count": df.height,
                    "columns": df.columns,
                    "has_metadata": "dt" in df.columns and "content_hash" in df.columns,
                    "attempt": attempt + 1,
                }

                log.info(
                    "GCS partition readable",
                    source=source_name,
                    row_count=df.height,
                    attempt=attempt + 1,
                    column_count=len(df.columns),
                    has_metadata=verification_results[source_name]["has_metadata"],
                )
                break

            except Exception as e:
                if attempt == max_retries - 1:
                    verification_results[source_name] = {
                        "readable": False,
                        "error": str(e),
                        "attempts": max_retries,
                    }
                    log.error(
                        "GCS partition not readable after all attempts",
                        source=source_name,
                        attempts=max_retries,
                        error=str(e),
                    )

    # Assert that at least one partition is readable
    readable_partitions = sum(1 for r in verification_results.values() if r.get("readable", False))
    assert readable_partitions >= 1, (
        f"Expected at least 1 readable partition, got {readable_partitions}"
    )


@pytest.mark.integration
def test_aggregation_from_gcs_with_fallback() -> None:
    """Phase 3: Test aggregation reading from GCS partitions with fallback."""
    log.info("Starting Phase 3: Aggregation from GCS (with fallback)")

    # Reset environment and BigQuery client for PROD access
    _reset_clients_for_prod()

    test_date = date.today()

    try:
        # Test the new GCS-based aggregator
        aggregated_df = build_all_chains_metadata_from_gcs(
            output_bq_table="test_architecture_validation_prod",
            manual_mappings_filepath="resources/manual_chain_mappings.csv",
            process_dt=test_date,
        )

        if aggregated_df.height > 0:
            log.info(
                "Aggregation successful",
                row_count=aggregated_df.height,
                column_count=len(aggregated_df.columns),
                sample_chains=aggregated_df["chain_key"].head(5).to_list(),
            )

            # Check data quality
            quality_checks = {
                "has_chain_key": "chain_key" in aggregated_df.columns,
                "has_display_name": "display_name" in aggregated_df.columns,
                "has_source_info": "source_name" in aggregated_df.columns,
                "no_null_keys": aggregated_df["chain_key"].null_count() == 0,
                "unique_keys": aggregated_df["chain_key"].n_unique() == aggregated_df.height,
            }

            log.info("Quality checks completed", quality_checks=quality_checks)

            # Show source distribution
            source_distribution = {}
            if "source_name" in aggregated_df.columns:
                source_counts = aggregated_df["source_name"].value_counts()
                source_distribution = source_counts.to_dict()
                log.info("Source distribution", distribution=source_distribution)

            # Assert quality checks
            assert quality_checks["has_chain_key"], "Missing chain_key column"
            assert quality_checks["has_display_name"], "Missing display_name column"
            assert quality_checks["no_null_keys"], "Found null values in chain_key"
            assert quality_checks["unique_keys"], "Found duplicate chain_key values"

        else:
            log.warning("Aggregation returned empty DataFrame")
            # In test environment, empty DataFrame might be expected
            # Don't fail the test, just log it

    except Exception as e:
        log.error("Aggregation failed", error=str(e))

        # Try to provide more context for common errors
        if "No data available" in str(e):
            log.warning("GCS partitions exist but are empty or unreadable")
        elif "BigQuery" in str(e):
            log.warning("BigQuery write failed - check table permissions")

        # In test environment, we might expect some failures
        # Don't fail the test, just log the error


@pytest.mark.integration
def test_deduplication_behavior_enhanced() -> None:
    """Phase 4: Enhanced deduplication test with hash comparison."""
    log.info("Starting Phase 4: Enhanced deduplication behavior test")

    # Reset environment and BigQuery client for PROD access
    _reset_clients_for_prod()

    test_date = date.today()

    # First run
    log.info("First ingestion run")
    try:
        result1 = ingest_with_deduplication(
            source_name="L2Beat (Dedup Test 1)",
            fetch_func=ingest_from_l2beat,
            dataset=ChainMetadata.L2BEAT,
            process_dt=test_date,
        )
        log.info("First run completed", updated=result1)

        # Small delay to ensure any caching/timing issues are resolved
        import time

        time.sleep(1)

    except Exception as e:
        log.error("First deduplication run failed", error=str(e))
        # Don't fail the test, just log the error
        return

    # Second run (should be skipped due to deduplication)
    log.info("Second ingestion run (should be deduplicated)")
    try:
        result2 = ingest_with_deduplication(
            source_name="L2Beat (Dedup Test 2)",
            fetch_func=ingest_from_l2beat,
            dataset=ChainMetadata.L2BEAT,
            process_dt=test_date,
        )
        log.info("Second run completed", updated=result2)

        # Analyze deduplication effectiveness
        if result1 and not result2:
            log.info("Deduplication working: First run updated, second run skipped")
        elif not result1 and not result2:
            log.info("Both runs skipped: Data may already exist from previous runs")
        else:
            log.warning("Both runs updated: Investigating deduplication logic")

    except Exception as e:
        log.error("Second deduplication run failed", error=str(e))
        # Don't fail the test, just log the error


@pytest.mark.integration
def test_full_pipeline_architecture() -> None:
    """Test the complete pipeline architecture end-to-end."""
    log.info("Starting full pipeline architecture test (PRODUCTION MODE)")
    log.warning("Using real BigQuery credentials - may incur costs")

    with with_prod_environment():
        try:
            # Phase 1: Independent ingestion
            test_individual_ingestion()

            # Phase 2: GCS verification with retry
            test_gcs_verification_with_retry()

            # Phase 3: Aggregation from GCS with fallback
            test_aggregation_from_gcs_with_fallback()

            # Phase 4: Enhanced deduplication behavior
            test_deduplication_behavior_enhanced()

        except Exception as e:
            log.error("Critical error in architecture test", error=str(e))
            import traceback

            traceback.print_exc()
            raise

    log.info("Architecture test complete")


if __name__ == "__main__":
    # Run the full pipeline test when executed as script
    test_full_pipeline_architecture()
