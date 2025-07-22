from op_analytics.datasources.chainsmeta.bq_chain_metadata import load_bq_chain_metadata
from op_analytics.datasources.goldsky.chain_usage import load_goldsky_chain_usage_data
from op_analytics.datapipeline.chains.loaders.defillama_loader import DefiLlamaChainMetadataLoader
from op_analytics.datapipeline.chains.loaders.l2beat_loader import L2BeatChainMetadataLoader
from op_analytics.datapipeline.chains.loaders.dune_loader import DuneChainMetadataLoader

# Patch current_environment to always return PROD
import op_analytics.coreutils.env.aware as aware

aware.current_environment = lambda: aware.OPLabsEnvironment.PROD

# WARNING:
# This script accesses production data sources (BigQuery, Goldsky, DefiLlama, L2Beat, Dune).
# It requires valid credentials and may incur costs or expose sensitive data.
# DO NOT run this script unless you are authorized and understand the implications.
# No secrets or credentials are included in this script.

if __name__ == "__main__":
    print("=== Testing BQ Chain Metadata Loader ===")
    try:
        bq_chain_metadata_df = load_bq_chain_metadata()
        print(bq_chain_metadata_df.head(5))
    except Exception as e:
        print(f"BQ Chain Metadata Loader Error: {e}")

    print("\n=== Testing Goldsky Loader ===")
    try:
        goldsky_df = load_goldsky_chain_usage_data()
        print(goldsky_df.head(5))
    except Exception as e:
        print(f"Goldsky Loader Error: {e}")

    print("\n=== Testing DefiLlama Loader ===")
    try:
        defillama_df = DefiLlamaChainMetadataLoader().run()
        print(defillama_df.head(5))
    except Exception as e:
        print(f"DefiLlama Loader Error: {e}")

    print("\n=== Testing L2Beat Loader ===")
    try:
        l2beat_df = L2BeatChainMetadataLoader().run()
        print(l2beat_df.head(5))
    except Exception as e:
        print(f"L2Beat Loader Error: {e}")

    print("\n=== Testing Dune Loader ===")
    try:
        dune_df = DuneChainMetadataLoader().run()
        print(dune_df.head(5))
    except Exception as e:
        print(f"Dune Loader Error: {e}")
