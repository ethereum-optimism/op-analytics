#!/bin/bash

# =========================================================================
# Block Batch Ingestion Fix Script
# =========================================================================
#
# This script processes a predefined list of blockbatches to fix data ingestion
# issues. The commands are run in --force_complete mode to force re-ingestion or
# re-computation of the data.  
#
# This script will come in handy when we discover that we have ingested data with
# issues from Goldsky and we need to re-compute MODELS.
#
# To gather list of blockbatchews with issues we can copy and paste from the
# go/pipeline dashboard directly.
#
# For each block in the list:
#
# 1. Ingests block data from Goldsky to GCS
# 2. Processes account abstraction prefilter models
# 3. Processes account abstraction models
#
# Usage:
#   ./fix_blockbatch_ingestion.sh
#
# Environment variables:
#   ALLOW_WRITE - Set to true to enable writing to GCS
#   GOLDSKY_FINAL - Set to true to use the FINAL keyword when querying Goldsky data

# Create an array with the block numbers
BLOCKBATCHES=(
28440652
)

# Set the chain
CHAIN="base"

# Process each blockbatch
# Trap Ctrl+C (SIGINT) and exit gracefully
trap 'echo -e "\nScript interrupted by user. Exiting..."; exit 1' INT

for BLOCKBATCH in "${BLOCKBATCHES[@]}"; do
    echo "Processing $CHAIN blockbatch $BLOCKBATCH"
    
    # Run the commands for each block
    ALLOW_WRITE=true GOLDSKY_FINAL=true uv run opdata chains ingest_blocks $CHAIN $BLOCKBATCH:+1 --read_from goldsky --write_to gcs --force_complete --no-fork_process
    ALLOW_WRITE=true uv run opdata chains blockbatch_models $CHAIN account_abstraction_prefilter,account_abstraction $BLOCKBATCH:+1 --write_to gcs --no-fork_process --force_complete
done