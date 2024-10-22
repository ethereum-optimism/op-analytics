from flipside import Flipside
import pandas as pd
import dotenv
import os

api_key = os.environ["FLIPSIDE_API_KEY"]
flipside = Flipside(api_key, "https://api-v2.flipsidecrypto.xyz")

# TODO Handle for Pagination
def query_to_df(query_sql):
        query_result_set = flipside.query(query_sql)
        df = pd.DataFrame(data= query_result_set.rows, columns= query_result_set.columns)
        return df
