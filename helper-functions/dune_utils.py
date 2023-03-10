# based off of Cow Protocol's Dune Client: https://github.com/cowprotocol/dune-client
import pandas as pd
import dotenv
import os
from loguru import logger
import argparse

from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import Query

query = Query(
    name="Latest OP Distributions by Project",
    query_id=1900056,
)
print("Results available at", query.url())

def get_dune_data(query_id: int, name: str="my_query_results") -> pd.DataFrame:
    """
    Get data via Dune API.
    """
    query = Query(
        name=name,
        query_id=query_id,
    )
    logger.info(f"Results available at {query.url()}")

    dotenv.load_dotenv()
    dune = DuneClient(os.environ["DUNE_API_KEY"])
    results = dune.refresh(query)

    df = pd.DataFrame(results.result.rows)
    df["last_updated"] = results.times.submitted_at

    if not os.path.exists("csv_output"):
        os.makedirs("csv_output")
    
    df.to_csv(f"csv_output/{name}.csv")

    logger.info(f"✨ Results saved as csv_output/{name}.csv, with {len(df)} rows and {len(df.columns)} columns.")

    return df.head()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Call Dune API and pull data from a query. Enter the query_id and name of the file you want to save as.')
    parser.add_argument('--query_id', type=int, help='Enter query_id')
    parser.add_argument('--name', type=str, help='Name of the query, which will also be saved as csv.')

    args = parser.parse_args()

    get_dune_data(args.query_id, args.name)
