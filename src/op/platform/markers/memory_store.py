from dataclasses import replace
from datetime import date, datetime, timedelta
from threading import RLock
from typing import Dict, List, Mapping, Sequence, Tuple

from ...core.interfaces.marker_store import MarkerStore
from ...core.types.file_marker import FileMarker
from ...core.types.run_marker import RunMarker


class MemoryMarkerStore(MarkerStore):
    """
    In-memory, thread-safe MarkerStore for unit/integration tests.
    - Replaces by (product_id, data_path) keeping the newest updated_at.
    - Run markers stored by run_id (last write wins).
    """

    def __init__(self) -> None:
        self._lock = RLock()
        self._files: Dict[Tuple[str, str], FileMarker] = {}
        self._runs: Dict[str, RunMarker] = {}

    # ------------- test helpers -------------

    def reset(self) -> None:
        with self._lock:
            self._files.clear()
            self._runs.clear()

    def seed_files(self, markers: Sequence[FileMarker]) -> None:
        self.write_file_markers(markers)

    def seed_runs(self, runs: Sequence[RunMarker]) -> None:
        with self._lock:
            for r in runs:
                self._runs[r.run_id] = r

    def dump_files(self) -> List[FileMarker]:
        with self._lock:
            return sorted(self._files.values(), key=lambda m: (m.product_id, m.updated_at))

    # ------------- MarkerStore API -------------

    def begin_run(self, run: RunMarker) -> None:
        with self._lock:
            self._runs[run.run_id] = run

    def finish_run(self, run_id: str, status: str, finished_at, error_msg: str | None) -> None:
        with self._lock:
            if run_id in self._runs:
                cur = self._runs[run_id]
                self._runs[run_id] = replace(cur, status=status, finished_at=finished_at, error_msg=error_msg)
            else:
                # tolerate out-of-order, insert a minimal record
                self._runs[run_id] = RunMarker(
                    env="unknown",
                    product_id="unknown",
                    partition_key="",
                    run_id=run_id,
                    status=status,
                    started_at=finished_at or datetime.utcnow(),
                    finished_at=finished_at,
                    error_msg=error_msg,
                )

    def write_file_markers(self, markers: Sequence[FileMarker]) -> None:
        if not markers:
            return
        with self._lock:
            for m in markers:
                key = (m.product_id, m.data_path)
                prev = self._files.get(key)
                if prev is None or m.updated_at >= prev.updated_at:
                    self._files[key] = m

    def latest_files(self, product_id: str, where: Mapping[str, str] = {}) -> list[FileMarker]:
        with self._lock:
            out = []
            for m in self._files.values():
                if m.product_id != product_id:
                    continue
                # simple dimension filter on partition JSON
                ok = True
                for k, v in where.items():
                    if str(m.partition.get(k, "")) != str(v):
                        ok = False
                        break
                if ok:
                    out.append(m)
            # newest first
            out.sort(key=lambda x: x.updated_at, reverse=True)
            return out

    # Planning helpers

    def missing_partitions(self, producer: str, consumer: str, dims: Mapping[str, str], lookback_days: int) -> list[str]:
        now = date.today()
        st = now - timedelta(days=lookback_days)
        # Build sets of partition keys "k=v|k2=v2"
        prod_set = set()
        cons_set = set()
        with self._lock:
            for m in self._files.values():
                pk = _canon_pk(m.partition)
                # filter by dims
                if not _match_dims(m.partition, dims):
                    continue
                if m.product_id == producer:
                    if _in_window(m.partition, st, now):
                        prod_set.add(pk)
                elif m.product_id == consumer:
                    cons_set.add(pk)
        missing = sorted(list(prod_set - cons_set))
        return missing

    def coverage(self, product_id: str, dims: Mapping[str, str], window: tuple[date, date]) -> list[dict]:
        st, ed = window
        daily: Dict[str, dict] = {}
        with self._lock:
            for m in self._files.values():
                if m.product_id != product_id:
                    continue
                if not _match_dims(m.partition, dims):
                    continue
                dt = m.partition.get("dt")
                if dt and st.isoformat() <= dt <= ed.isoformat():
                    d = daily.setdefault(dt, {"dt": dt, "files": 0, "rows": 0, "last_write": m.updated_at})
                    d["files"] += 1
                    d["rows"] += int(m.row_count or 0)
                    if m.updated_at > d["last_write"]:
                        d["last_write"] = m.updated_at
        return [daily[k] for k in sorted(daily)]


# ---------- small helpers ----------

def _canon_pk(part: Mapping[str, str]) -> str:
    return "|".join(f"{k}={part[k]}" for k in sorted(part))

def _match_dims(part: Mapping[str, str], dims: Mapping[str, str]) -> bool:
    for k, v in dims.items():
        if str(part.get(k, "")) != str(v):
            return False
    return True

def _in_window(part: Mapping[str, str], st: date, ed: date) -> bool:
    v = part.get("dt")
    if not v:
        return True  # if no date dimension, treat as always in window
    return st.isoformat() <= v <= ed.isoformat()
