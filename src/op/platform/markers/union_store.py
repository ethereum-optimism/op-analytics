from __future__ import annotations
from dataclasses import dataclass
from datetime import date
from typing import Mapping, Sequence, List, Dict

from ...core.types.file_marker import FileMarker
from ...core.types.run_marker import RunMarker
from .store import MarkerStore

@dataclass
class UnionMarkerStore(MarkerStore):
    primary: MarkerStore   # v2 (read/write)
    legacy: MarkerStore    # read-only adapter

    # write API -> primary only
    def begin_run(self, run: RunMarker) -> None:
        self.primary.begin_run(run)

    def finish_run(self, run_id: str, status: str, finished_at, error_msg: str | None) -> None:
        self.primary.finish_run(run_id, status, finished_at, error_msg)

    def write_file_markers(self, markers: Sequence[FileMarker]) -> None:
        self.primary.write_file_markers(markers)

    # read API -> union & dedup by (product_id, data_path) preferring primary
    def latest_files(self, product_id: str, where: Mapping[str,str] = {}) -> list[FileMarker]:
        p = self.primary.latest_files(product_id, where)
        l = self.legacy.latest_files(product_id, where)
        seen = {(m.product_id, m.data_path) for m in p}
        merged = list(p)
        for m in l:
            if (m.product_id, m.data_path) not in seen:
                merged.append(m)
        return merged

    def missing_partitions(self, producer: str, consumer: str, dims: Mapping[str,str], lookback_days: int) -> list[str]:
        # Derive via merged sets
        prod = self.latest_files(producer, dims)
        cons = self.latest_files(consumer, dims)
        def pk(m: FileMarker) -> str:
            return "|".join(f"{k}={m.partition[k]}" for k in sorted(m.partition))
        prod_set = {pk(m) for m in prod}
        cons_set = {pk(m) for m in cons}
        return sorted(list(prod_set - cons_set))

    def coverage(self, product_id: str, dims: Mapping[str,str], window: tuple[date,date]) -> list[dict]:
        rows = self.latest_files(product_id, dims)
        st, ed = window
        daily: Dict[str, dict] = {}
        for m in rows:
            dt = m.partition.get("dt")
            if dt and (st.isoformat() <= dt <= ed.isoformat()):
                d = daily.setdefault(dt, {"dt": dt, "files": 0, "rows": 0})
                d["files"] += 1
                d["rows"]  += int(m.row_count or 0)
        return [daily[k] for k in sorted(daily)]
