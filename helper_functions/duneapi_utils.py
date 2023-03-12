# Based off of Cow Protocol's Dune Client: https://github.com/cowprotocol/dune-client
import pandas as pd
import dotenv
import os

from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import Query

# Run a Dune API Query and return as a Pandas Dataframe
def df_run_duneapi_query(q_id, q_name = ""):
    dotenv.load_dotenv()
    dune = DuneClient(os.environ["DUNE_API_KEY"])
    #Load Query
    query = Query(
        name= q_name,
        query_id=q_id,
    )
    print("Results available at", query.url())
    #Run Query
    results = dune.refresh(query)

    df = pd.DataFrame(results.result.rows)
    df['last_updated'] = results.times.submitted_at

    return df