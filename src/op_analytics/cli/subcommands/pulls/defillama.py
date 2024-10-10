import time
import polars as pl
from op_coreutils.bigquery.write import overwrite_partition, overwrite_partitions, overwrite_table
from op_coreutils.logger import structlog
from op_coreutils.request import new_session
from op_coreutils.time import now_dt

log = structlog.get_logger()

SUMMARY_ENDPOINT = "https://stablecoins.llama.fi/stablecoins?includePrices=true"
BREAKDOWN_ENDPOINT = "https://stablecoins.llama.fi/stablecoin/{id}"

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

def process_breakdown_data(data, stablecoin_id):
    chains = data.get('chainBalances', {})
    rows = []
    peg_currency = data.get('pegType', '').replace('pegged', '')  # e.g., 'USD', 'EUR'
    
    for chain, chain_data in chains.items():
        circulating_history = chain_data.get('circulating', [])
        for entry in circulating_history:
            row = {
                'date': entry['date'],
                'chain': chain,
                'stablecoin_id': stablecoin_id,
                'peg_currency': peg_currency,
            }
            
            # Handle circulating amount in original currency
            if 'circulating' in entry:
                for currency, amount in entry['circulating'].items():
                    row[f'circulating_{currency}'] = amount
            
            # Handle circulating amount in USD
            if 'totalCirculatingUSD' in entry:
                for currency, amount in entry['totalCirculatingUSD'].items():
                    row[f'circulating_usd_{currency}'] = amount
            
            # Handle bridged amount
            if 'bridgedTo' in entry:
                for currency, amount in entry['bridgedTo'].items():
                    row[f'bridged_{currency}'] = amount
            
            rows.append(row)
    
    return pl.DataFrame(rows)

def pull_stables():
    """Pull stablecoin data from DeFiLlama.

    - Fetch the stablecoins summary endpoint.
    - For each stablecoin, fetch the detailed breakdown data.
    - Write all results to BigQuery.
    """
    session = new_session()
    summary = get_data(session, SUMMARY_ENDPOINT)
    
    # Parse the summary and store as a dataframe
    summary_df = pl.DataFrame(summary['peggedAssets'])

    breakdown_dfs = []
    for stablecoin in summary['peggedAssets']:
        stablecoin_id = stablecoin['id']
        url = BREAKDOWN_ENDPOINT.format(id=stablecoin_id)
        data = get_data(session, url)
        
        if data:
            breakdown_df = process_breakdown_data(data, stablecoin_id)
            breakdown_dfs.append(breakdown_df)
        
        # Add a delay of 0.5 seconds between each stablecoin request
        time.sleep(0.5)

    breakdown_df = pl.concat(breakdown_dfs)

    # Add a datetime column
    breakdown_df = breakdown_df.with_columns(
        dt=pl.from_epoch(pl.col("date")).dt.strftime("%Y-%m-%d")
    )

    print('breakdown')
    print(breakdown_df.head(5))
    # # Write summary to BQ
    # dt = now_dt()
    # overwrite_table(summary_df, BQ_DATASET, f"{SUMMARY_TABLE}_latest")
    # overwrite_partition(summary_df, dt, BQ_DATASET, f"{SUMMARY_TABLE}_history")

    # # Write breakdown to BQ
    # overwrite_partitions(breakdown_df, BQ_DATASET, f"{BREAKDOWN_TABLE}_history")

    return {"summary": summary_df, "breakdown": breakdown_df}
