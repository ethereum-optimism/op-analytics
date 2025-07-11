import polars as pl
import pytest

from op_analytics.datapipeline.chains.mapping_utils import (
    load_manual_mappings,
    apply_mapping_rules,
    _validate_mapping_rule,
)


def test_load_manual_mappings_file_not_found():
    """Test that missing mapping file returns empty list."""
    result = load_manual_mappings("nonexistent.csv")
    assert result == []


def test_load_manual_mappings_with_disabled_rules(tmp_path):
    """Test that disabled rules are filtered out."""
    csv_content = """mapping_type,identifier_type,identifier_value,source_filter,target_field,new_value,conditions,description,enabled
chain_id_override,chain_name,test1,,chain_id,123,,Test enabled rule,true
chain_id_override,chain_name,test2,,chain_id,456,,Test disabled rule,false
"""
    csv_file = tmp_path / "test_mappings.csv"
    csv_file.write_text(csv_content)

    result = load_manual_mappings(str(csv_file))
    assert len(result) == 1
    assert result[0]["identifier_value"] == "test1"


def test_validate_mapping_rule_valid():
    """Test validation of a valid mapping rule."""
    rule = {
        "mapping_type": "chain_id_override",
        "identifier_type": "chain_name",
        "identifier_value": "test",
        "target_field": "chain_id",
        "new_value": "123",
    }

    result = _validate_mapping_rule(rule, 0)
    assert result["mapping_type"] == "chain_id_override"
    assert result["identifier_type"] == "chain_name"


def test_validate_mapping_rule_missing_required_field():
    """Test validation fails for missing required fields."""
    rule = {
        "mapping_type": "chain_id_override",
        "identifier_type": "chain_name",
        # missing identifier_value and target_field
    }

    with pytest.raises(ValueError, match="Missing required field"):
        _validate_mapping_rule(rule, 0)


def test_validate_mapping_rule_invalid_mapping_type():
    """Test validation fails for unsupported mapping type."""
    rule = {
        "mapping_type": "invalid_type",
        "identifier_type": "chain_name",
        "identifier_value": "test",
        "target_field": "chain_id",
    }

    with pytest.raises(ValueError, match="Unsupported mapping_type"):
        _validate_mapping_rule(rule, 0)


def test_apply_mapping_rules_empty_mappings():
    """Test that empty mappings list returns original DataFrame."""
    df = pl.DataFrame(
        {
            "chain_name": ["test"],
            "chain_id": ["123"],
        }
    )

    result = apply_mapping_rules(df, [])
    assert result.equals(df)


def test_apply_mapping_rules_chain_id_override():
    """Test chain_id override transformation."""
    df = pl.DataFrame(
        {
            "chain_name": ["molten", "other"],
            "chain_id": ["999", "123"],
        }
    )

    mappings = [
        {
            "mapping_type": "chain_id_override",
            "identifier_type": "chain_name",
            "identifier_value": "molten",
            "source_filter": None,
            "target_field": "chain_id",
            "new_value": None,
            "conditions": None,
            "row_index": 0,
        }
    ]

    result = apply_mapping_rules(df, mappings)

    assert result["chain_id"].to_list() == [None, "123"]


def test_apply_mapping_rules_chain_id_suffix():
    """Test chain_id suffix transformation."""
    df = pl.DataFrame(
        {
            "chain_name": ["celo", "other"],
            "chain_id": ["42220", "123"],
            "layer": ["L2", "L1"],
        }
    )

    mappings = [
        {
            "mapping_type": "chain_id_suffix",
            "identifier_type": "chain_name",
            "identifier_value": "celo",
            "source_filter": None,
            "target_field": "chain_id",
            "new_value": "-l2",
            "conditions": "layer=L2",
            "row_index": 0,
        }
    ]

    result = apply_mapping_rules(df, mappings)

    assert result["chain_id"].to_list() == ["42220-l2", "123"]


def test_apply_mapping_rules_display_name_preference():
    """Test display name preference transformation."""
    df = pl.DataFrame(
        {
            "display_name": ["Arbitrum", "Other Chain"],
            "chain_id": ["42161", "123"],
        }
    )

    mappings = [
        {
            "mapping_type": "display_name_preference",
            "identifier_type": "display_name",
            "identifier_value": "arbitrum",
            "source_filter": None,
            "target_field": "display_name",
            "new_value": "Arbitrum One",
            "conditions": None,
            "row_index": 0,
        }
    ]

    result = apply_mapping_rules(df, mappings)

    assert result["display_name"].to_list() == ["Arbitrum One", "Other Chain"]


def test_apply_mapping_rules_with_source_filter():
    """Test mapping rule with source filter."""
    df = pl.DataFrame(
        {
            "chain_name": ["test", "test"],
            "chain_id": ["123", "456"],
            "source_name": ["l2beat", "dune"],
        }
    )

    mappings = [
        {
            "mapping_type": "field_override",
            "identifier_type": "chain_name",
            "identifier_value": "test",
            "source_filter": "l2beat",
            "target_field": "chain_id",
            "new_value": "999",
            "conditions": None,
            "row_index": 0,
        }
    ]

    result = apply_mapping_rules(df, mappings)

    # Only the l2beat source should be affected
    assert result["chain_id"].to_list() == ["999", "456"]


def test_apply_mapping_rules_field_override():
    """Test generic field override transformation."""
    df = pl.DataFrame(
        {
            "chain_name": ["test_chain"],
            "provider": ["old_provider"],
        }
    )

    mappings = [
        {
            "mapping_type": "field_override",
            "identifier_type": "chain_name",
            "identifier_value": "test_chain",
            "source_filter": None,
            "target_field": "provider",
            "new_value": "new_provider",
            "conditions": None,
            "row_index": 0,
        }
    ]

    result = apply_mapping_rules(df, mappings)

    assert result["provider"].to_list() == ["new_provider"]


def test_apply_mapping_rules_multiple_rules():
    """Test applying multiple mapping rules in sequence."""
    df = pl.DataFrame(
        {
            "chain_name": ["arbitrum", "celo"],
            "display_name": ["Arbitrum", "Celo"],
            "chain_id": ["42161", "42220"],
            "layer": ["L2", "L2"],
        }
    )

    mappings = [
        {
            "mapping_type": "display_name_preference",
            "identifier_type": "display_name",
            "identifier_value": "arbitrum",
            "source_filter": None,
            "target_field": "display_name",
            "new_value": "Arbitrum One",
            "conditions": None,
            "row_index": 0,
        },
        {
            "mapping_type": "chain_id_suffix",
            "identifier_type": "chain_name",
            "identifier_value": "celo",
            "source_filter": None,
            "target_field": "chain_id",
            "new_value": "-l2",
            "conditions": "layer=L2",
            "row_index": 1,
        },
    ]

    result = apply_mapping_rules(df, mappings)

    assert result["display_name"].to_list() == ["Arbitrum One", "Celo"]
    assert result["chain_id"].to_list() == ["42161", "42220-l2"]


def test_apply_mapping_rules_no_matching_rows():
    """Test that rules with no matching rows don't modify the DataFrame."""
    df = pl.DataFrame(
        {
            "chain_name": ["existing_chain"],
            "chain_id": ["123"],
        }
    )

    mappings = [
        {
            "mapping_type": "chain_id_override",
            "identifier_type": "chain_name",
            "identifier_value": "nonexistent_chain",
            "source_filter": None,
            "target_field": "chain_id",
            "new_value": "999",
            "conditions": None,
            "row_index": 0,
        }
    ]

    result = apply_mapping_rules(df, mappings)

    # DataFrame should remain unchanged
    assert result.equals(df)


def test_apply_mapping_rules_with_conditions():
    """Test mapping rules with additional conditions."""
    df = pl.DataFrame(
        {
            "chain_name": ["test", "test"],
            "chain_id": ["123", "456"],
            "layer": ["L1", "L2"],
        }
    )

    mappings = [
        {
            "mapping_type": "chain_id_suffix",
            "identifier_type": "chain_name",
            "identifier_value": "test",
            "source_filter": None,
            "target_field": "chain_id",
            "new_value": "-l2",
            "conditions": "layer=L2",
            "row_index": 0,
        }
    ]

    result = apply_mapping_rules(df, mappings)

    # Only the row with layer=L2 should be affected
    assert result["chain_id"].to_list() == ["123", "456-l2"]


def test_validate_mapping_rule_chain_id_override_validation():
    """Test that chain_id_override must target chain_id field."""
    rule = {
        "mapping_type": "chain_id_override",
        "identifier_type": "chain_name",
        "identifier_value": "test",
        "target_field": "display_name",  # Wrong field
        "new_value": "123",
    }

    with pytest.raises(ValueError, match="chain_id_override must target 'chain_id' field"):
        _validate_mapping_rule(rule, 0)


def test_validate_mapping_rule_display_name_preference_validation():
    """Test that display_name_preference must target display_name field."""
    rule = {
        "mapping_type": "display_name_preference",
        "identifier_type": "display_name",
        "identifier_value": "test",
        "target_field": "chain_id",  # Wrong field
        "new_value": "Test Chain",
    }

    with pytest.raises(
        ValueError, match="display_name_preference must target 'display_name' field"
    ):
        _validate_mapping_rule(rule, 0)


def test_validate_mapping_rule_invalid_identifier_type():
    """Test validation fails for invalid identifier type."""
    rule = {
        "mapping_type": "field_override",
        "identifier_type": "invalid_identifier",
        "identifier_value": "test",
        "target_field": "chain_id",
    }

    with pytest.raises(ValueError, match="Invalid identifier_type"):
        _validate_mapping_rule(rule, 0)


def test_apply_mapping_rules_error_handling():
    """Test that errors in individual rules don't stop processing."""
    df = pl.DataFrame(
        {
            "chain_name": ["test"],
            "chain_id": ["123"],
        }
    )

    # Create a mapping that will cause an error (missing column)
    mappings = [
        {
            "mapping_type": "field_override",
            "identifier_type": "chain_name",
            "identifier_value": "test",
            "source_filter": None,
            "target_field": "nonexistent_field",
            "new_value": "value",
            "conditions": None,
            "row_index": 0,
        }
    ]

    # Should not raise an exception, just log the error and continue
    result = apply_mapping_rules(df, mappings)

    # Original DataFrame should be returned unchanged
    assert result.equals(df)
