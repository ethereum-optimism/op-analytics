from datetime import datetime, date
import requests

from op_analytics.coreutils.logger import structlog, bound_contextvars
from op_analytics.coreutils.clickhouse.oplabs import run_statememt_oplabs

from .schemas import (
    SCHEMAS,
    CPI_SNAPSHOTS_SCHEMA,
    CPI_COUNCIL_PERCENTAGES_SCHEMA,
    CPI_HISTORICAL_SCHEMA,
)


log = structlog.get_logger()

API_KEY = "cpi_355b0293-fe4a-4230-a823-27c12c6c5ef2"
CPI_ENDPOINT = "https://docs.daocpi.com/api/calculate-cpi"
HISTORICAL_ENDPOINT = "https://docs.daocpi.com/api/historic-cpi"


def fetch_cpi_data():
    headers = {"x-api-key": API_KEY}

    log.info("fetching latest CPI data")
    response = requests.get(CPI_ENDPOINT, headers=headers)

    if response.status_code != 200:
        log.error(f"failed to fetch data: {response.status_code} {response.text}")
        return None

    return response.json()


def fetch_historical_data():
    headers = {"x-api-key": API_KEY}

    today = date.today().strftime("%Y-%m-%d")
    params = {"date": today}

    log.info(f"fetching historical CPI data for {today}")
    response = requests.get(HISTORICAL_ENDPOINT, headers=headers, params=params)

    if response.status_code != 200:
        log.error(f"failed to fetch historical CPI data: {response.status_code} {response.text}")
        return None

    return response.json()


def process_cpi_data(json_data):
    snapshots = []
    council_percentages = []

    id_counter = 1  # Simple counter for IDs

    for item in json_data.get("results", []):
        # Generate a numeric ID for this snapshot
        snapshot_id = id_counter
        id_counter += 1

        # Get the snapshot date
        snapshot_date = item.get("date")

        snapshot = {
            "id": snapshot_id,
            "snapshot_date": snapshot_date,
            "cpi_value": item.get("cpi"),
            "active_percent": item.get("councilPercentages", {}).get("active"),
            "inactive_percent": item.get("councilPercentages", {}).get("inactive"),
        }
        snapshots.append(snapshot)

        # Extract council percentages
        redistributed = item.get("councilPercentages", {}).get("redistributed", {})
        original = item.get("councilPercentages", {}).get("originalPercentages", {})

        # Combine all council names from both dictionaries
        all_councils = set(redistributed.keys()) | set(original.keys())

        for council in all_councils:
            council_data = {
                "id": id_counter,
                "snapshot_date": snapshot_date,  # Use snapshot_date instead of snapshot_id
                "council_name": council,
                "original_percentage": original.get(council),
                "redistributed_percentage": redistributed.get(council),
            }
            id_counter += 1
            council_percentages.append(council_data)

    return snapshots, council_percentages


def process_historical_data(json_data):
    historical_records = []
    current_time = datetime.now()

    # The API returns data for a single date
    if json_data and "date" in json_data:
        record = {
            "date": json_data.get("date"),
            "HHI": json_data.get("HHI"),
            "CPI": json_data.get("CPI"),
        }
        historical_records.append(record)

    return historical_records


def insert_data_to_clickhouse(snapshots, council_percentages, historical_records):
    if snapshots:
        values = ", ".join(
            [
                f"({s['id']}, '{s['snapshot_date']}', {s['cpi_value']}, "  # Removed quotes around id
                f"{s['active_percent']}, {s['inactive_percent']})"
                for s in snapshots
            ]
        )

        insert_stmt = f"""
        INSERT INTO {CPI_SNAPSHOTS_SCHEMA.table_name()} 
        (id, snapshot_date, cpi_value, active_percent, inactive_percent)
        VALUES {values}
        """

        result = run_statememt_oplabs(insert_stmt)
        log.info(f"Inserted {len(snapshots)} snapshots", **result)

    if council_percentages:
        values = []
        for cp in council_percentages:
            orig_pct = "NULL" if cp["original_percentage"] is None else cp["original_percentage"]
            redist_pct = (
                "NULL" if cp["redistributed_percentage"] is None else cp["redistributed_percentage"]
            )

            values.append(
                f"({cp['id']}, '{cp['snapshot_date']}', '{cp['council_name']}', "  # Changed to snapshot_date
                f"{orig_pct}, {redist_pct})"
            )

        values_str = ", ".join(values)

        insert_stmt = f"""
        INSERT INTO {CPI_COUNCIL_PERCENTAGES_SCHEMA.table_name()} 
        (id, snapshot_date, council_name, original_percentage, redistributed_percentage)
        VALUES {values_str}
        """

        result = run_statememt_oplabs(insert_stmt)
        log.info(f"Inserted {len(council_percentages)} council percentages", **result)

    if historical_records:
        values = ", ".join([f"('{h['date']}', {h['HHI']}, {h['CPI']})" for h in historical_records])

        insert_stmt = f"""
        INSERT INTO {CPI_HISTORICAL_SCHEMA.table_name()} 
        (date, HHI, CPI)
        VALUES {values}
        """

        result = run_statememt_oplabs(insert_stmt)
        log.info(f"Inserted {len(historical_records)} historical records", **result)


def execute_pull():
    results = {}

    for schema in SCHEMAS:
        with bound_contextvars(table_name=schema.table_name()):
            log.info("Creating table")
            run_statememt_oplabs(schema.create())

    cpi_json_data = fetch_cpi_data()
    historical_json_data = fetch_historical_data()

    if not cpi_json_data and not historical_json_data:
        return {"error": "Failed to fetch data from APIs"}

    snapshots, council_percentages = process_cpi_data(cpi_json_data) if cpi_json_data else ([], [])
    historical_records = (
        process_historical_data(historical_json_data) if historical_json_data else []
    )

    insert_data_to_clickhouse(snapshots, council_percentages, historical_records)

    return {
        "snapshots_count": len(snapshots),
        "council_percentages_count": len(council_percentages),
        "historical_records_count": len(historical_records),
    }
