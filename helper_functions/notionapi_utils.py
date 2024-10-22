import os
import requests as r
from notion_client import Client
import notion_df
from datetime import datetime
import pandas as pd

notion_api = os.environ["NOTION_API_TOKEN"]
notion = Client(auth=notion_api)

headers = {
        "Authorization": "Bearer " + notion_api,
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28",
        }


def read_notion_database(databaseId):
        # url = "https://api.notion.com/v1/databases/" + databaseId
        # # response = r.get(url, headers=headers)
        # response = notion.databases.retrieve(databaseId)
        # # print(response.text)
        results = notion.databases.query(
        **{
                "database_id": databaseId,
        }
        )

        for result in results["results"]:
                print(result)

def read_notion_database_to_pandas_df(page_url):
        notion_df.pandas() #That's it!
        df = pd.read_notion(page_url, api_key=notion_api)
        return df