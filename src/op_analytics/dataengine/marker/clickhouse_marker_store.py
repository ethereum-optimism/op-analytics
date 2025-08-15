from typing import Mapping, Optional

from op_analytics.dataengine.core.interfaces.marker_store import MarkerStore


class ClickHouseMarkerStore(MarkerStore):
    """ClickHouse marker store (stub). Implement with MergeTree + unique keys.

    Recommended schema (table: dataengine_markers):
      - pipeline String, step String
      - partition_keys Map(String, String)
      - schema String, schema_version String
      - fingerprint String
      - created_at DateTime DEFAULT now()
    Primary key: (pipeline, step, fingerprint)
    """
    def get(self, key: Mapping[str, str]) -> Optional[str]:
        raise NotImplementedError
    def put(self, key: Mapping[str, str], fingerprint: str) -> str:
        raise NotImplementedError
