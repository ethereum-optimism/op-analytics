# based off of Cow Protocol's Dune Client: https://github.com/cowprotocol/dune-client
import pandas as pd
import dotenv
import os
from loguru import logger
import argparse

from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import Query

def get_dune_data(query_id: int, name: str="my_query_results", path: str="csv_outputs") -> pd.DataFrame:
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

    if not os.path.exists(path):
        os.makedirs(path)
    
    df.to_csv(f"{path}/{name}.csv")

    logger.info(f"✨ Results saved as {path}/{name}.csv, with {len(df)} rows and {len(df.columns)} columns.")

    return df


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Call Dune API and pull data from a query. Enter the query_id and name of the file you want to save as.')
    parser.add_argument('--query_id', type=int, help='Enter query_id')
    parser.add_argument('--name', type=str, help='Name of the query, which will also be saved as csv.')
    parser.add_argument('--path', type=str, help='Path of the csv to be saved.')

    args = parser.parse_args()

    get_dune_data(args.query_id, args.name, arg.path)
