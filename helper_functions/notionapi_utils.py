import os
import requests as r
from notion_client import Client

notion = Client(auth=os.environ["NOTION_API_TOKEN"])


def read_notion_database(databaseId):

        url = "https://api.notion.com/v1/databases/" + databaseId

        headers = {
        "accept": "application/json",
        "Notion-Version": "2022-06-28"
        }

        # response = r.get(url, headers=headers)
        response = notion.databases.retrieve(databaseId)

        print(response.text)

        results = notion.databases.query(
        **{
                "database_id": databaseId,
        }
        )

        for result in results["results"]:
                print(result)