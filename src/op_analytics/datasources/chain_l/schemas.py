from dataclasses import dataclass

from op_analytics.coreutils.partitioned.dailydata import DailyDataset

from .dataaccess import Chain_L


@dataclass
class Chain_LCPISchema:
    table: DailyDataset
    schema: dict[str, str]
    order_by: list[str]

    def table_name(self):
        return f"transforms_chain_l.{self.table.table}"

    def schema_type(self, name: str, dtype: str):
        if name in self.order_by:
            return dtype
        else:
            return f"Nullable({dtype})"

    def create(self):
        cols = [f"`{name}` {self.schema_type(name, dtype)}" for name, dtype in self.schema.items()]
        columns = ",\n".join(cols)
        order_by = ", ".join(self.order_by)

        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name()}
        (
            {columns}
        )
        ENGINE = ReplacingMergeTree
        ORDER BY ({order_by})
        """


# Define the schemas for our tables
CPI_SNAPSHOTS_SCHEMA = Chain_LCPISchema(
    table=Chain_L.CPI_SNAPSHOTS,
    schema={
        "id": "Uint64",
        "snapshot_date": "Date",
        "cpi_value": "Float64",
        "active_percent": "Float64",
        "inactive_percent": "Float64",
    },
    order_by=["id"],
)

CPI_COUNCIL_PERCENTAGES_SCHEMA = Chain_LCPISchema(
    table=Chain_L.CPI_COUNCIL_PERCENTAGES,
    schema={
        "id": "Uint64",
        "snapshot_date": "Date",
        "council_name": "String",
        "original_percentage": "Float64",
        "redistributed_percentage": "Float64",
    },
    order_by=["id", "snapshot_date", "council_name"],
)

CPI_HISTORICAL_SCHEMA = Chain_LCPISchema(
    table=Chain_L.CPI_HISTORICAL,
    schema={
        "date": "Date",
        "HHI": "Float64",
        "CPI": "Float64",
    },
    order_by=["date"],
)

SCHEMAS = [CPI_SNAPSHOTS_SCHEMA, CPI_COUNCIL_PERCENTAGES_SCHEMA, CPI_HISTORICAL_SCHEMA]
