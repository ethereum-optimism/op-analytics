import dagster as dg
from datetime import datetime

DAILY = dg.DailyPartitionsDefinition(start_date="2025-01-01")  # adjust

def today_str() -> str:
    return datetime.utcnow().date().isoformat()
