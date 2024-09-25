from enum import Enum

from pydantic import BaseModel
from pyiceberg.types import (
    IcebergType,
)


class JsonRPCMethod(str, Enum):
    eth_getBlockByNumber = 1
    eth_getTransactionReceipt = 2


class Column(BaseModel):
    # Iceberg Properties
    field_id: int
    name: str
    field_type: IcebergType
    required: bool

    # Custom Properties
    doc: str | None = None
    json_rpc_method: JsonRPCMethod | None = None
    json_rpc_field_name: str | None = None
    enrichment_function: str | None = None


class Table(BaseModel):
    name: str
    doc: str
    columns: list[Column]
