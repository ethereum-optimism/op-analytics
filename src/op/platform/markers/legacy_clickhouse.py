from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, Mapping, Optional, Sequence

import json
import clickhouse_connect

from ...core.interfaces.marker_store import MarkerStore
from ...core.types.file_marker import FileMarker
from ...core.types.run_marker import RunMarker


@dataclass(frozen=True)
class LegacyProductBinding:
    product_id: str               # e.g., "dune.fees:v1"
    table: str                    # legacy CH table name (e.g., ops.blockbatch_markers)
    # how to resolve partition cols
    partition_cols: Dict[str, str]# e.g., {"dt":"dt","chain":"chain"}  (map -> colname)
    # path & counts
    data_path_col: str = "data_path"
    root_path_literal: Optional[str] = None  # if legacy does not store root separately
    row_count_col: str = "row_count"
    # optional span/range fields
    span_min_col: Optional[str] = None       # e.g., "min_block"
    span_max_col: Optional[str] = None       # e.g., "max_block"
    span_kind_literal: Optional[str] = None  # e.g., "block"
    # metadata
    updated_at_col: str = "updated_at"
    process_name_col: Optional[str] = "process_name"
    writer_name_col: Optional[str] = "writer_name"

@dataclass
class LegacyClickHouseMarkerAdapter(MarkerStore):
    client: clickhouse_connect.driver.Client
    env: str
    products: Dict[str, LegacyProductBinding]  # product_id -> binding

    # ---- write API (no-ops) ----
    def begin_run(self, run: RunMarker) -> None: pass
    def finish_run(self, run_id: str, status: str, finished_at, error_msg: str | None) -> None: pass
    def write_file_markers(self, markers: Sequence[FileMarker]) -> None: pass

    # ---- read API ----
    def latest_files(self, product_id: str, where: Mapping[str,str] = {}) -> list[FileMarker]:
        b = self.products[product_id]
        conds = []
        params = {}
        for i,(k,col) in enumerate(b.partition_cols.items()):
            if k in where:
                conds.append(f"{col} = %(p{i})s")
                params[f"p{i}"] = where[k]
        where_sql = ("WHERE " + " AND ".join(conds)) if conds else ""
        cols = [b.data_path_col, b.row_count_col, b.updated_at_col]
        select_parts = [", ".join(cols)]
        # pack partitions into JSON
        part_json = "{" + ",".join([f"'{k}': toString({col})" for k,col in b.partition_cols.items()]) + "}"
        select = f"""
          SELECT
            {b.data_path_col} AS data_path,
            {b.row_count_col} AS row_count,
            {b.updated_at_col} AS updated_at,
            {part_json} AS partition_json
            {", " + b.span_min_col if b.span_min_col else ""}
            {", " + b.span_max_col if b.span_max_col else ""}
            {", " + b.process_name_col if b.process_name_col else ""}
            {", " + b.writer_name_col if b.writer_name_col else ""}
          FROM {b.table}
          {where_sql}
          ORDER BY updated_at DESC
          LIMIT 500
        """
        df = self.client.query_df(select, parameters=params)
        out: list[FileMarker] = []
        for _, r in df.iterrows():
            pj = json.loads(r["partition_json"])
            out.append(FileMarker(
                env=self.env,
                product_id=product_id,
                root_path=b.root_path_literal or "",   # legacy root if known
                data_path=r["data_path"],
                partition=pj,
                row_count=int(r["row_count"]),
                byte_size=None,
                span_min=(str(r[b.span_min_col]) if b.span_min_col else None),
                span_max=(str(r[b.span_max_col]) if b.span_max_col else None),
                span_kind=b.span_kind_literal,
                num_parts=None,
                process_name=(str(r[b.process_name_col]) if b.process_name_col else "legacy"),
                writer_name=(str(r[b.writer_name_col]) if b.writer_name_col else "legacy"),
                updated_at=r["updated_at"],
                run_id="", fingerprint="",
                upstream=(),
            ))
        return out

    def missing_partitions(self, producer: str, consumer: str, dims: Mapping[str,str], lookback_days: int) -> list[str]:
        # Generic implementation: compute sets of partition_keys by dt window on producer,
        # then subtract any seen by consumer.
        now = date.today()
        st = now - timedelta(days=lookback_days)
        prod = self.latest_files(producer, where={**dims})  # could be filtered more by dt below
        cons = self.latest_files(consumer, where={**dims})
        def pk(p: Mapping[str,str]) -> str:
            return "|".join(f"{k}={p[k]}" for k in sorted(p))
        prod_set = {pk(m.partition) for m in prod if "dt" not in m.partition or (st.isoformat() <= m.partition["dt"] <= now.isoformat())}
        cons_set = {pk(m.partition) for m in cons}
        return sorted(list(prod_set - cons_set))

    def coverage(self, product_id: str, dims: Mapping[str,str], window: tuple[date,date]) -> list[dict]:
        rows = self.latest_files(product_id, where=dims)
        st, ed = window
        daily = {}
        for m in rows:
            dt = m.partition.get("dt")
            if dt and (st.isoformat() <= dt <= ed.isoformat()):
                d = daily.setdefault(dt, {"dt": dt, "files": 0, "rows": 0, "last_write": m.updated_at})
                d["files"] += 1
                d["rows"] += int(m.row_count or 0)
                if m.updated_at > d["last_write"]:
                    d["last_write"] = m.updated_at
        return [daily[k] for k in sorted(daily)]
