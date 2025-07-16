import polars as pl
import pytest
from op_analytics.datapipeline.chains.mapping_utils import apply_mapping_rules

EXAMPLES_CSV = ".cursor/features/chain_metadata_aggregator/manual_chain_mappings_examples.csv"


def test_apply_all_example_mappings():
    # Load all example mappings (even if disabled, for test coverage)
    df = pl.read_csv(EXAMPLES_CSV, truncate_ragged_lines=True)
    mappings = df.to_dicts()

    # Create a dummy DataFrame with all possible relevant columns
    dummy = pl.DataFrame(
        {
            "chain_name": ["arbitrum", "celo", "kardia", "test", "example_slug", "molten"],
            "chain_id": ["1234", "5678", "24", "9999", "9999", "999"],
            "display_name": [
                "Arbitrum",
                "Polygon PoS",
                "OP Mainnet",
                "Test Chain",
                "Example Chain",
                "Molten",
            ],
            "provider": ["old", "old", "old", "old", "old", "old"],
            "provider_entity": ["old", "old", "old", "old", "old", "old"],
            "source_name": ["op_labs", "l2beat", "dune", "growthepie", "defillama", "defillama"],
            "slug": ["molten", "example_slug", "other", "other", "other", "molten"],
            "layer": ["L2", "L2", "L1", "L2", "L2", "L2"],
            "is_evm": [False, False, False, False, False, False],
            "date": [
                "2023-12-31",
                "2024-01-01",
                "2024-06-01",
                "2024-12-31",
                "2025-01-01",
                "2024-01-01",
            ],
        }
    )

    # Should not raise any exceptions
    try:
        result = apply_mapping_rules(dummy, mappings)
    except Exception as e:
        pytest.fail(f"Applying example mappings raised an exception: {e}")

    # Molten should be mapped to '360' (not None)
    if "molten" in dummy["slug"].to_list():
        idx = dummy["slug"].to_list().index("molten")
        assert result["chain_id"][idx] == "360"

    # Optionally: check that the DataFrame shape is unchanged
    assert result.shape == dummy.shape


def test_date_range_mapping_logic():
    # Test all date range scenarios
    mappings = [
        # Applies always
        {
            "mapping_type": "field_override",
            "identifier_type": "chain_name",
            "identifier_value": "arbitrum",
            "target_field": "provider",
            "new_value": "new",
            "source_filter": None,
            "conditions": None,
            "row_index": 0,
        },
        # Only after 2024-01-01
        {
            "mapping_type": "field_override",
            "identifier_type": "chain_name",
            "identifier_value": "celo",
            "target_field": "provider",
            "new_value": "new",
            "start_date": "2024-01-01",
            "source_filter": None,
            "conditions": None,
            "row_index": 1,
        },
        # Only until 2024-06-01
        {
            "mapping_type": "field_override",
            "identifier_type": "chain_name",
            "identifier_value": "kardia",
            "target_field": "provider",
            "new_value": "new",
            "end_date": "2024-06-01",
            "source_filter": None,
            "conditions": None,
            "row_index": 2,
        },
        # Only between 2024-01-01 and 2024-12-31
        {
            "mapping_type": "field_override",
            "identifier_type": "chain_name",
            "identifier_value": "test",
            "target_field": "provider",
            "new_value": "new",
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "source_filter": None,
            "conditions": None,
            "row_index": 3,
        },
    ]
    dummy = pl.DataFrame(
        {
            "chain_name": ["arbitrum", "celo", "kardia", "test", "test"],
            "provider": ["old", "old", "old", "old", "old"],
            "date": ["2023-12-31", "2024-01-01", "2024-06-01", "2024-01-01", "2025-01-01"],
        }
    )
    result = apply_mapping_rules(dummy, mappings)
    # Check results
    assert result["provider"].to_list() == ["new", "new", "new", "new", "old"]


def test_date_range_edge_cases():
    # Test missing date column (should not error, mapping should NOT apply if date range is specified)
    mappings = [
        {
            "mapping_type": "field_override",
            "identifier_type": "chain_name",
            "identifier_value": "arbitrum",
            "target_field": "provider",
            "new_value": "new",
            "start_date": "2024-01-01",
            "source_filter": None,
            "conditions": None,
            "row_index": 0,
        },
    ]
    dummy = pl.DataFrame(
        {
            "chain_name": ["arbitrum"],
            "provider": ["old"],
        }
    )
    result = apply_mapping_rules(dummy, mappings)
    # Should remain unchanged
    assert result["provider"].to_list() == ["old"]

    # Test invalid date format (should not error, mapping does not apply)
    mappings = [
        {
            "mapping_type": "field_override",
            "identifier_type": "chain_name",
            "identifier_value": "arbitrum",
            "target_field": "provider",
            "new_value": "new",
            "start_date": "notadate",
            "source_filter": None,
            "conditions": None,
            "row_index": 0,
        },
    ]
    dummy = pl.DataFrame(
        {
            "chain_name": ["arbitrum"],
            "provider": ["old"],
            "date": ["2024-01-01"],
        }
    )
    try:
        result = apply_mapping_rules(dummy, mappings)
        # Should not raise, but mapping will not apply
        assert result["provider"].to_list() == ["old"]
    except Exception as e:
        pytest.fail(f"Mapping with invalid date format raised: {e}")
