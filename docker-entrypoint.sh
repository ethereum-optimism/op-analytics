#!/bin/sh

# Exit immediately if any command exits with a non-zero status
set -e

# Function to print usage
usage() {
    echo "Usage: $0 {healthcheck|ingest|intermediate}"
    exit 1
}

# Check if an argument is passed
if [ -z "$1" ]; then
    usage
fi

# Switch based on the input argument
case "$1" in
    healthcheck)
        echo "Checking docker container health..."
        exec opdata chains health
        ;;
    ingest)
        echo "Executing Ingestion..."
        exec opdata chains ingest_blocks ALL m18hours --read_from goldsky --write_to gcs
        ;;
    intermediate)
        echo "Executing Intermediate..."
        exec opdata chains health
        ;;
    *)
        echo "Invalid argument: $1"
        usage
        ;;
esac
