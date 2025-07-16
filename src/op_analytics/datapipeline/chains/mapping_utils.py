from pathlib import Path
from typing import Any, Dict, List, Optional

import polars as pl

from op_analytics.coreutils.logger import structlog

log = structlog.get_logger()


# Mapping rule types supported by the system
SUPPORTED_MAPPING_TYPES = {
    "chain_id_override",  # Override chain_id values (e.g., molten -> NULL)
    "chain_id_suffix",  # Add suffix to chain_id (e.g., celo -> celo-l2)
    "display_name_preference",  # Set preferred display names
    "field_override",  # Override any field value
    "source_filter",  # Apply rules only to specific sources
    "conditional_transform",  # Apply rules based on conditions
}

# Valid identifier types for targeting records
VALID_IDENTIFIER_TYPES = {
    "chain_name",
    "chain_id",
    "display_name",
    "source_name",
    "slug",
}


def load_manual_mappings(filepath: str) -> List[Dict[str, Any]]:
    """
    Load manual mapping rules from CSV configuration file.

    Args:
        filepath (str): Path to the manual mappings CSV file

    Returns:
        List[Dict[str, Any]]: List of validated mapping rule dictionaries

    Raises:
        FileNotFoundError: If the mapping file doesn't exist
        ValueError: If the CSV structure is invalid or contains unsupported rules
    """
    log.info("Loading manual mapping rules", filepath=filepath)

    # Check if file exists
    if not Path(filepath).exists():
        log.warning("Manual mappings file not found, returning empty list", filepath=filepath)
        return []

    try:
        # Read CSV using polars with proper schema inference
        mappings_df = pl.read_csv(
            filepath,
            schema_overrides={
                "mapping_type": pl.String,
                "identifier_type": pl.String,
                "identifier_value": pl.String,
                "source_filter": pl.String,
                "target_field": pl.String,
                "new_value": pl.String,
                "conditions": pl.String,
                "start_date": pl.String,
                "end_date": pl.String,
                "description": pl.String,
                "enabled": pl.String,
            },
            null_values=["", "NULL", "null", "None"],
        )

        log.info(
            "Successfully loaded mappings CSV", rows=mappings_df.height, columns=mappings_df.width
        )

        # Filter for enabled rules only
        enabled_df = mappings_df.filter(
            pl.col("enabled").is_null() | (pl.col("enabled").str.to_lowercase() == "true")
        )

        # Convert to list of dictionaries for easier processing
        mappings_list = enabled_df.to_dicts()

        # Validate each mapping rule
        validated_mappings = []
        for i, mapping in enumerate(mappings_list):
            try:
                validated_mapping = _validate_mapping_rule(mapping, row_index=i)
                validated_mappings.append(validated_mapping)
            except ValueError as e:
                log.error("Invalid mapping rule", row_index=i, error=str(e), mapping=mapping)
                # Continue processing other rules instead of failing completely
                continue

        log.info(
            "Successfully validated mapping rules",
            total_rules=len(mappings_list),
            valid_rules=len(validated_mappings),
            invalid_rules=len(mappings_list) - len(validated_mappings),
        )

        return validated_mappings

    except Exception as e:
        log.error("Failed to load manual mappings", filepath=filepath, error=str(e))
        raise ValueError(f"Error loading manual mappings from {filepath}: {e}") from e


def _validate_mapping_rule(mapping: Dict[str, Any], row_index: int) -> Dict[str, Any]:
    """
    Validate a single mapping rule for required fields and supported values.

    Args:
        mapping (Dict[str, Any]): Raw mapping rule dictionary
        row_index (int): Row index for error reporting

    Returns:
        Dict[str, Any]: Validated and cleaned mapping rule

    Raises:
        ValueError: If the mapping rule is invalid
    """
    # Required fields
    required_fields = ["mapping_type", "identifier_type", "identifier_value", "target_field"]

    for field in required_fields:
        if not mapping.get(field):
            raise ValueError(f"Missing required field '{field}' at row {row_index}")

    # Validate mapping_type
    mapping_type = mapping["mapping_type"].lower()
    if mapping_type not in SUPPORTED_MAPPING_TYPES:
        raise ValueError(
            f"Unsupported mapping_type '{mapping_type}' at row {row_index}. "
            f"Supported types: {list(SUPPORTED_MAPPING_TYPES)}"
        )

    # Validate identifier_type
    identifier_type = mapping["identifier_type"].lower()
    if identifier_type not in VALID_IDENTIFIER_TYPES:
        raise ValueError(
            f"Invalid identifier_type '{identifier_type}' at row {row_index}. "
            f"Valid types: {list(VALID_IDENTIFIER_TYPES)}"
        )

    # Clean and normalize the mapping rule
    cleaned_mapping = {
        "mapping_type": mapping_type,
        "identifier_type": identifier_type,
        "identifier_value": mapping["identifier_value"].strip(),
        "source_filter": mapping.get("source_filter", "").strip()
        if mapping.get("source_filter")
        else None,
        "target_field": mapping["target_field"].strip(),
        "new_value": mapping.get("new_value", "").strip() if mapping.get("new_value") else None,
        "conditions": mapping.get("conditions", "").strip() if mapping.get("conditions") else None,
        "start_date": mapping.get("start_date", "").strip() if mapping.get("start_date") else None,
        "end_date": mapping.get("end_date", "").strip() if mapping.get("end_date") else None,
        "description": mapping.get("description", "").strip()
        if mapping.get("description")
        else None,
        "row_index": row_index,
    }

    # Special validation for specific mapping types
    if mapping_type == "chain_id_override" and cleaned_mapping["target_field"] != "chain_id":
        raise ValueError(f"chain_id_override must target 'chain_id' field at row {row_index}")

    if (
        mapping_type == "display_name_preference"
        and cleaned_mapping["target_field"] != "display_name"
    ):
        raise ValueError(
            f"display_name_preference must target 'display_name' field at row {row_index}"
        )

    return cleaned_mapping


def apply_mapping_rules(df: pl.DataFrame, mappings: List[Dict[str, Any]]) -> pl.DataFrame:
    """
    Apply manual mapping rules to a DataFrame.

    Args:
        df (pl.DataFrame): Input DataFrame to transform
        mappings (List[Dict[str, Any]]): List of validated mapping rules

    Returns:
        pl.DataFrame: Transformed DataFrame with mapping rules applied
    """
    if not mappings:
        log.info("No mapping rules to apply")
        return df

    log.info("Applying manual mapping rules", total_rules=len(mappings), input_rows=df.height)

    result_df = df.clone()
    rules_applied = 0
    rows_affected = 0

    for mapping in mappings:
        try:
            result_df, affected = _apply_single_mapping_rule(result_df, mapping)
            rules_applied += 1
            rows_affected += affected

            log.debug(
                "Applied mapping rule",
                mapping_type=mapping["mapping_type"],
                identifier=f"{mapping['identifier_type']}={mapping['identifier_value']}",
                target_field=mapping["target_field"],
                rows_affected=affected,
            )

        except Exception as e:
            log.error("Failed to apply mapping rule", mapping=mapping, error=str(e))
            # Continue with other rules instead of failing completely
            continue

    log.info(
        "Completed applying mapping rules",
        rules_applied=rules_applied,
        total_rows_affected=rows_affected,
        final_rows=result_df.height,
    )

    return result_df


def _apply_single_mapping_rule(
    df: pl.DataFrame, mapping: Dict[str, Any]
) -> tuple[pl.DataFrame, int]:
    """
    Apply a single mapping rule to the DataFrame.

    Args:
        df (pl.DataFrame): Input DataFrame
        mapping (Dict[str, Any]): Single mapping rule to apply

    Returns:
        tuple[pl.DataFrame, int]: Transformed DataFrame and count of affected rows
    """
    mapping_type = mapping["mapping_type"]
    identifier_type = mapping["identifier_type"]
    identifier_value = mapping["identifier_value"]
    source_filter = mapping["source_filter"]
    target_field = mapping["target_field"]
    new_value = mapping["new_value"]
    conditions = mapping["conditions"]

    # Build the base condition for identifying target records
    base_condition = _build_identifier_condition(identifier_type, identifier_value)

    # Add source filter if specified
    if source_filter:
        source_condition = pl.col("source_name").str.to_lowercase() == source_filter.lower()
        base_condition = base_condition & source_condition

    # Add additional conditions if specified
    if conditions:
        additional_condition = _parse_conditions(conditions)
        if additional_condition is not None:
            base_condition = base_condition & additional_condition

    # Add date range condition if specified
    date_col = "date"
    start_date = mapping.get("start_date")
    end_date = mapping.get("end_date")
    if start_date or end_date:
        if date_col not in df.columns:
            # If date range is specified but no date column, skip mapping
            return df, 0
        # Assume date column is string or datetime; parse as needed
        date_expr = pl.col(date_col)
        if start_date:
            base_condition = base_condition & (date_expr >= pl.lit(start_date))
        if end_date:
            base_condition = base_condition & (date_expr <= pl.lit(end_date))

    # Count matching rows before transformation
    matching_rows = df.filter(base_condition).height

    if matching_rows == 0:
        return df, 0

    # Apply the specific mapping transformation
    if mapping_type == "chain_id_override":
        result_df = _apply_chain_id_override(df, base_condition, new_value)
    elif mapping_type == "display_name_preference":
        result_df = _apply_display_name_preference(df, base_condition, new_value)
    elif mapping_type == "field_override":
        result_df = _apply_field_override(df, base_condition, target_field, new_value)
    else:
        # Generic field replacement for other mapping types
        result_df = _apply_field_override(df, base_condition, target_field, new_value)

    return result_df, matching_rows


def _build_identifier_condition(identifier_type: str, identifier_value: str) -> pl.Expr:
    """Build a polars condition expression for identifying target records."""
    if identifier_type == "chain_name":
        return pl.col("chain_name").str.to_lowercase() == identifier_value.lower()
    elif identifier_type == "chain_id":
        return pl.col("chain_id") == identifier_value
    elif identifier_type == "display_name":
        return pl.col("display_name").str.to_lowercase() == identifier_value.lower()
    elif identifier_type == "source_name":
        return pl.col("source_name").str.to_lowercase() == identifier_value.lower()
    elif identifier_type == "slug":
        # Handle various slug column names - check which columns actually exist
        # This function will be called on the DataFrame, so we need to check existence at runtime
        # For now, create a condition that checks the main 'slug' column
        return pl.col("slug").str.to_lowercase() == identifier_value.lower()
    else:
        raise ValueError(f"Unsupported identifier_type: {identifier_type}")


def _parse_conditions(conditions_str: str) -> Optional[pl.Expr]:
    """
    Parse additional conditions string into polars expression.

    Simple implementation for basic conditions like 'layer=L2' or 'provider=OP Stack'.
    Can be extended for more complex condition parsing.
    """
    try:
        if "=" in conditions_str:
            field, value = conditions_str.split("=", 1)
            field = field.strip()
            value = value.strip().strip("\"'")
            return pl.col(field) == value
        else:
            log.warning("Unsupported condition format", conditions=conditions_str)
            return None
    except Exception as e:
        log.warning("Failed to parse conditions", conditions=conditions_str, error=str(e))
        return None


def _apply_chain_id_override(
    df: pl.DataFrame, condition: pl.Expr, new_value: Optional[str]
) -> pl.DataFrame:
    """Apply chain_id override transformation."""
    if new_value and new_value.lower() in ["null", "none", ""]:
        new_value = None

    return df.with_columns(
        pl.when(condition).then(pl.lit(new_value)).otherwise(pl.col("chain_id")).alias("chain_id")
    )


def _apply_field_override(
    df: pl.DataFrame, condition: pl.Expr, target_field: str, new_value: Optional[str]
) -> pl.DataFrame:
    """Apply generic field override transformation, supporting templated new_value."""
    if new_value and new_value.lower() in ["null", "none", ""]:
        new_value = None

    # Support templated new_value, e.g., {chain_id}-l2
    def _template_func(row):
        if new_value is None:
            return None
        if "{" in new_value and "}" in new_value:
            # Replace {field} with row[field] for all fields present
            out = new_value
            for col in df.columns:
                out = out.replace(f"{{{col}}}", str(row[col]) if row[col] is not None else "")
            return out
        return new_value

    # Apply the override with templating only to matching rows
    # Use map_elements for matching rows, otherwise keep original
    return df.with_columns(
        pl.when(condition)
        .then(pl.struct(df.columns).map_elements(_template_func))
        .otherwise(pl.col(target_field))
        .alias(target_field)
    )
