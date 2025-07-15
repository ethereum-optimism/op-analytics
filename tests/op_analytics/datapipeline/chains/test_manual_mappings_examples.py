import polars as pl
import pytest
from op_analytics.datapipeline.chains.mapping_utils import apply_mapping_rules

EXAMPLES_CSV = ".cursor/features/chain_metadata_aggregator/manual_chain_mappings_examples.csv"


def test_apply_all_example_mappings():
    # Load all example mappings (even if disabled, for test coverage)
    df = pl.read_csv(EXAMPLES_CSV)
    mappings = df.to_dicts()

    # Create a dummy DataFrame with all possible relevant columns
    dummy = pl.DataFrame(
        {
            "chain_name": ["arbitrum", "celo", "kardia", "test", "example_slug"],
            "chain_id": ["1234", "5678", "24", "9999", "9999"],
            "display_name": [
                "Arbitrum",
                "Polygon PoS",
                "OP Mainnet",
                "Test Chain",
                "Example Chain",
            ],
            "provider": ["old", "old", "old", "old", "old"],
            "provider_entity": ["old", "old", "old", "old", "old"],
            "source_name": ["op_labs", "l2beat", "dune", "growthepie", "defillama"],
            "slug": ["molten", "example_slug", "other", "other", "other"],
            "layer": ["L2", "L2", "L1", "L2", "L2"],
            "is_evm": [False, False, False, False, False],
        }
    )

    # Should not raise any exceptions
    try:
        result = apply_mapping_rules(dummy, mappings)
    except Exception as e:
        pytest.fail(f"Applying example mappings raised an exception: {e}")

    # Optionally: check that the DataFrame shape is unchanged
    assert result.shape == dummy.shape
