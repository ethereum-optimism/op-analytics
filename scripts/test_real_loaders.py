import os
import polars as pl
from op_analytics.datapipeline.chains.aggregator import build_all_chains_metadata

# WARNING:
# This script accesses production data sources (BigQuery, Goldsky, DefiLlama, L2Beat, Dune).
# It requires valid credentials and may incur costs.
# To run this test, you must set the OPLABS_ENV=PROD environment variable.
# Example: OPLABS_ENV=PROD python scripts/test_real_loaders.py


def test_real_aggregator():
    """
    Tests the chain metadata aggregator with real, live data sources.
    """
    print("=== Testing Chain Metadata Aggregator with real data ===")

    # Check if the environment variable is set
    if os.getenv("OPLABS_ENV") != "PROD":
        print("Skipping test: OPLABS_ENV is not set to PROD.")
        print("Please set OPLABS_ENV=PROD to run this test.")
        return

    try:
        # Run the full aggregation pipeline
        aggregated_df = build_all_chains_metadata()

        # Basic validation
        assert aggregated_df is not None, "Aggregated DataFrame should not be None"
        assert isinstance(aggregated_df, pl.DataFrame), "Output should be a Polars DataFrame"
        assert not aggregated_df.is_empty(), "Aggregated DataFrame should not be empty"
        assert "chain_key" in aggregated_df.columns, "chain_key should be in the columns"

        print("Chain metadata aggregator test passed.")
        print("Aggregated DataFrame head:")
        print(aggregated_df.head(5))
        print(f"Total chains aggregated: {len(aggregated_df)}")

    except Exception as e:
        print(f"Chain Metadata Aggregator Test Error: {e}")
        # Re-raise the exception to make it clear that the test failed
        raise


if __name__ == "__main__":
    test_real_aggregator()
