import time
import polars as pl
from op_coreutils.bigquery.write import overwrite_partition, overwrite_partitions, overwrite_table
from op_coreutils.logger import structlog
from op_coreutils.request import new_session
from op_coreutils.threads import run_concurrently
from op_coreutils.time import now_dt

log = structlog.get_logger()

SUMMARY_ENDPOINT = "https://stablecoins.llama.fi/stablecoins?includePrices=true"
BREAKDOWN_ENDPOINT = "https://stablecoins.llama.fi/stablecoincharts/{chain}?stablecoin={id}"

BQ_DATASET = "uploads_api"

SUMMARY_TABLE = "defillama_daily_stablecoins_summary"
BREAKDOWN_TABLE = "defillama_daily_stablecoins_breakdown"

def get_data(session, url):
    start = time.time()
    resp = session.request(
        method="GET",
        url=url,
        headers={"Content-Type": "application/json"},
    ).json()
    log.info(f"Fetched from {url}: {time.time() - start:.2f} seconds")
    return resp

def pull():
    """Pull stablecoin data from DeFiLlama.

    - Fetch the stablecoins summary endpoint.
    - For each stablecoin and chain, fetch the breakdown data.
    - Write all results to BigQuery.
    """
    session = new_session()
    summary = get_data(session, SUMMARY_ENDPOINT)
    
    # Parse the summary and store as a dataframe
    summary_df = pl.DataFrame(summary['peggedAssets'])

    # Set up breakdown data http requests
    urls = {}
    for stablecoin in summary['peggedAssets']:
        for chain in stablecoin['chainCirculating'].keys():
            urls[(chain, stablecoin['id'])] = BREAKDOWN_ENDPOINT.format(chain=chain, id=stablecoin['id'])

    # Run requests concurrently
    breakdown_data = run_concurrently(lambda x: get_data(session, x), urls, max_workers=8)
    percent_success = 100.0 * sum(_["success"] for _ in breakdown_data.values()) / len(breakdown_data)
    if percent_success < 80:
        raise Exception("Failed to get DeFiLlama data for >80% of chains/stablecoins")

    dfs = []
    for (chain, stablecoin_id), data in breakdown_data.items():
        if data["success"]:
            # Determine the peg type from the first data point
            peg_type = list(data['data'][0]['totalCirculating'].keys())[0]
            
            breakdown_df = pl.DataFrame(data['data'])
            
            # Extract nested fields
            for field in ['totalCirculating', 'totalUnreleased', 'totalCirculatingUSD', 'totalMintedUSD']:
                if field in breakdown_df.columns:
                    breakdown_df = breakdown_df.with_columns(
                        pl.col(field).struct.field(peg_type).alias(f"{field}_{peg_type}")
                    )
            
            # Add additional columns
            breakdown_df = breakdown_df.with_columns(
                chain=pl.lit(chain),
                stablecoin_id=pl.lit(stablecoin_id),
                peg_type=pl.lit(peg_type),
                dt=pl.from_epoch(pl.col("date")).dt.strftime("%Y-%m-%d"),
            )

            dfs.append(breakdown_df)

    breakdown_df = pl.concat(dfs)

    # Write summary to BQ
    dt = now_dt()
    print(dt.head(5)) # debug
    # overwrite_table(summary_df, BQ_DATASET, f"{SUMMARY_TABLE}_latest")
    # overwrite_partition(summary_df, dt, BQ_DATASET, f"{SUMMARY_TABLE}_history")

    # # Write breakdown to BQ
    # overwrite_partitions(breakdown_df, BQ_DATASET, f"{BREAKDOWN_TABLE}_history")

    return {"summary": summary_df, "breakdown": breakdown_df}
