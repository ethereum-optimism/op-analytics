"""
Token utilities for chain metadata.

This module provides functions to extract token IDs from chain metadata
for use across different price data sources (CoinGecko, DeFiLlama, etc.).
"""

from typing import List
import os
import csv
import json

import polars as pl

from op_analytics.coreutils.logger import structlog
from .load import load_chain_metadata

log = structlog.get_logger()


def get_token_ids_from_metadata() -> List[str]:
    """
    Get list of token IDs from the chain metadata.

    Extracts both CoinGecko API IDs and gas token IDs from the chain metadata.
    Handles both simple string format and dictionary format for cgt_coingecko_api.

    Returns:
        List of token IDs (could be CoinGecko slugs or chain:address format)
    """
    # Load chain metadata
    chain_metadata = load_chain_metadata()

    # Get token IDs from chain metadata - check both coingecko and gas_token fields
    token_ids = []

    # Get CoinGecko IDs - handle both string and dictionary formats
    coingecko_rows = (
        chain_metadata.filter(pl.col("cgt_coingecko_api").is_not_null())
        .select("cgt_coingecko_api")
        .unique()
        .to_series()
        .to_list()
    )

    for api_value in coingecko_rows:
        if api_value:
            # Try to parse as JSON dictionary first
            try:
                parsed_dict = json.loads(api_value)
                if isinstance(parsed_dict, dict):
                    # Extract all values from the dictionary (these are the coingecko API IDs)
                    token_ids.extend(parsed_dict.values())
                    log.info(
                        "parsed_dictionary_format",
                        api_value=api_value,
                        extracted_ids=list(parsed_dict.values()),
                    )
                else:
                    # If it's not a dict, treat as simple string
                    token_ids.append(api_value)
            except (json.JSONDecodeError, TypeError):
                # If JSON parsing fails, treat as simple string
                token_ids.append(api_value)

    # Remove duplicates
    token_ids = list(set(token_ids))

    log.info("found_token_ids", count=len(token_ids))
    return token_ids


def read_token_ids_from_file(filepath: str) -> List[str]:
    """
    Read token IDs from a CSV or TXT file.

    For CSV files, looks for a 'token_id' column. If not found, treats the first
    column as token IDs. For TXT files, assumes one token ID per line.

    Args:
        filepath: Path to the file containing token IDs

    Returns:
        List of token IDs

    Raises:
        FileNotFoundError: If the file doesn't exist
        ValueError: If the file extension is not supported
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")

    token_ids: set[str] = set()
    ext = os.path.splitext(filepath)[1].lower()

    if ext == ".txt":
        with open(filepath, "r") as f:
            for line in f:
                tid = line.strip()
                if tid:
                    token_ids.add(tid)
    elif ext == ".csv":
        with open(filepath, newline="") as csvfile:
            # First try to read as CSV with headers
            reader = csv.DictReader(csvfile)
            if reader.fieldnames and "token_id" in reader.fieldnames:
                for row in reader:
                    tid = row["token_id"].strip()
                    if tid:
                        token_ids.add(tid)
            else:
                # fallback: treat as single-column CSV
                csvfile.seek(0)
                # Use a separate context to avoid type conflicts
                with open(filepath, newline="") as fallback_file:
                    fallback_reader = csv.reader(fallback_file)
                    for row in fallback_reader:  # type: ignore[assignment]
                        if row:  # Check if row is not empty
                            tid = row[0].strip()
                            if tid and tid != "token_id":
                                token_ids.add(tid)
    else:
        raise ValueError(f"Unsupported file extension: {ext}")

    return list(token_ids)


def get_token_ids_from_metadata_and_file(
    extra_token_ids_file: str | None = None,
    include_top_tokens: int = 0,
    top_tokens_fetcher=None,
) -> List[str]:
    """
    Get unique list of token IDs from chain metadata and an optional file.

    Args:
        extra_token_ids_file: Optional path to file with extra token IDs
        include_top_tokens: Number of top tokens by market cap to include (0 for none)
        top_tokens_fetcher: Optional function to fetch top tokens (e.g., from CoinGecko)

    Returns:
        List of unique token IDs
    """
    token_ids: set[str] = set(get_token_ids_from_metadata())

    if extra_token_ids_file:
        extra_ids = read_token_ids_from_file(extra_token_ids_file)
        token_ids.update(extra_ids)

    if include_top_tokens > 0 and top_tokens_fetcher is not None:
        try:
            top_token_ids = top_tokens_fetcher(limit=include_top_tokens)
            token_ids.update(top_token_ids)
            log.info("Added top tokens by market cap", count=len(top_token_ids))
        except Exception as e:
            log.error("Failed to fetch top tokens by market cap", error=str(e))
            # Continue without top tokens if there's an error

    result = list(token_ids)
    log.info("final_token_ids", count=len(result))
    return result


def get_coingecko_token_for_block(chain_name: str, block_number: int) -> str | None:
    """
    Get the appropriate CoinGecko token ID for a specific chain and block number.

    For chains with changing gas tokens, this function will return the correct
    token ID based on the block number.

    Args:
        chain_name: Name of the chain (e.g., "fraxtal")
        block_number: Block number to check

    Returns:
        CoinGecko token ID string, or None if not found
    """
    # Load chain metadata
    chain_metadata = load_chain_metadata()

    # Find the row for this chain
    chain_row = (
        chain_metadata.filter(pl.col("chain_name") == chain_name)
        .select("cgt_coingecko_api")
        .to_series()
        .to_list()
    )

    if not chain_row or not chain_row[0]:
        return None

    api_value = chain_row[0]

    # Try to parse as JSON dictionary
    try:
        parsed_dict = json.loads(api_value)
        if isinstance(parsed_dict, dict):
            # Convert keys to integers for comparison
            block_ranges = {int(k): v for k, v in parsed_dict.items()}

            # Find the appropriate token ID based on block number
            # Get all starting blocks <= current block, then take the maximum
            valid_starts = [start for start in block_ranges.keys() if start <= block_number]
            if valid_starts:
                latest_start = max(valid_starts)
                return block_ranges[latest_start]
            else:
                return None
        else:
            # If it's not a dict, return as simple string
            return api_value
    except (json.JSONDecodeError, TypeError):
        # If JSON parsing fails, return as simple string
        return api_value


def get_coingecko_token_ids() -> List[str]:
    """
    Get only CoinGecko token IDs from chain metadata.
    Handles both string and dictionary formats.

    Returns:
        List of CoinGecko token IDs
    """
    chain_metadata = load_chain_metadata()

    token_ids = []
    coingecko_rows = (
        chain_metadata.filter(pl.col("cgt_coingecko_api").is_not_null())
        .select("cgt_coingecko_api")
        .unique()
        .to_series()
        .to_list()
    )

    for api_value in coingecko_rows:
        if api_value:
            # Try to parse as JSON dictionary first
            try:
                parsed_dict = json.loads(api_value)
                if isinstance(parsed_dict, dict):
                    # Extract all values from the dictionary
                    token_ids.extend(parsed_dict.values())
                else:
                    # If it's not a dict, treat as simple string
                    token_ids.append(api_value)
            except (json.JSONDecodeError, TypeError):
                # If JSON parsing fails, treat as simple string
                token_ids.append(api_value)

    # Remove duplicates
    token_ids = list(set(token_ids))

    log.info("found_coingecko_token_ids", count=len(token_ids))
    return token_ids


def get_gas_token_ids() -> List[str]:
    """
    Get only gas token IDs from chain metadata.

    Returns:
        List of gas token IDs
    """
    chain_metadata = load_chain_metadata()

    token_ids = (
        chain_metadata.filter(pl.col("gas_token").is_not_null())
        .select("gas_token")
        .unique()
        .to_series()
        .to_list()
    )

    log.info("found_gas_token_ids", count=len(token_ids))
    return token_ids
