import polars as pl
import urllib3

ENDPOINT = "https://l2beat.com/api/scaling/summary"


def pull_l2beat_tvl():
    resp = fetch()
    extracted = extract(resp)
    write(extracted)


def fetch():
    resp = urllib3.request(
        method="GET",
        url=ENDPOINT,
        headers={"Content-Type": "application/json"},
    )
    return resp.json()


def extract(resp: dict):
    projects = list(resp["data"]["projects"].values())

    df = pl.DataFrame(projects)
    return df


def write():
    pass
