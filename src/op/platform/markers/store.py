from datetime import date, timedelta
from typing import Mapping, Sequence
import json
import clickhouse_connect

from ...core.interfaces.marker_store import MarkerStore
from ...core.types.file_marker import FileMarker
from ...core.types.run_marker import RunMarker

class ClickHouseMarkerStore(MarkerStore):
    def __init__(self, host: str, username: str, password: str, database: str = "analytics"):
        self.client = clickhouse_connect.get_client(
            host=host,
            username=username,
            password=password,
            database=database
        )

    # --- Run ledger ---
    def begin_run(self, run: RunMarker) -> None:
        self.client.command("""
            INSERT INTO markers_v2_runs
            (env, product_id, partition_key, run_id, status, started_at, attempt, executor)
            VALUES (%(env)s, %(product_id)s, %(partition_key)s, %(run_id)s, %(status)s, %(started_at)s, %(attempt)s, %(executor)s)
        """, parameters={
            "env": run.env, "product_id": run.product_id, "partition_key": run.partition_key,
            "run_id": run.run_id, "status": run.status, "started_at": run.started_at, "attempt": run.attempt, "executor": run.executor
        })

    def finish_run(self, run_id: str, status: str, finished_at, error_msg: str | None) -> None:
        self.client.command(
            """
            ALTER TABLE markers_v2_runs
            UPDATE status=%(status)s, finished_at=%(finished_at)s, error_msg=%(errmsg)s
            WHERE run_id=%(run_id)s
            """,
            parameters={"status":status, "finished_at":finished_at, "errmsg":error_msg, "run_id":run_id})

    # --- File ledger ---
    def write_file_markers(self, markers: Sequence[FileMarker]) -> None:
        if not markers:
            return
        data = []
        for m in markers:
            data.append({
                "env": m.env, "product_id": m.product_id, "root_path": m.root_path, "data_path": m.data_path,
                "partition_key": "|".join(f"{k}={v}" for k,v in sorted(m.partition.items())),
                "partition_json": json.dumps(m.partition, separators=(",",":")),
                "row_count": m.row_count, "byte_size": m.byte_size or 0,
                "span_min": m.span_min, "span_max": m.span_max, "span_kind": m.span_kind,
                "num_parts": m.num_parts or 1,
                "process_name": m.process_name, "writer_name": m.writer_name,
                "updated_at": m.updated_at, "run_id": m.run_id, "fingerprint": m.fingerprint,
                "upstream": list(m.upstream),
            })
        self.client.insert("markers_v2_files", data, column_names=list(data[0].keys()))

    def latest_files(self, product_id: str, where: Mapping[str,str] = {}) -> list[FileMarker]:
        conds = [ "product_id = %(product_id)s" ]
        params = { "product_id": product_id }
        for i,(k,v) in enumerate(where.items()):
            conds.append(f"JSON_VALUE(partition_json, '$.{k}') = %(p{i})s")
            params[f"p{i}"] = v
        rows = self.client.query_df(f"""
            SELECT
                *
            FROM markers_v2_files
            WHERE {' AND '.join(conds)}
            ORDER BY updated_at DESC
            LIMIT 500
        """, parameters=params).to_dict("records")
        out: list[FileMarker] = []
        for r in rows:
            out.append(FileMarker(
                env=r["env"], product_id=r["product_id"], root_path=r["root_path"], data_path=r["data_path"],
                partition=json.loads(r["partition_json"]), row_count=r["row_count"], byte_size=r.get("byte_size"),
                span_min=r.get("span_min"), span_max=r.get("span_max"), span_kind=r.get("span_kind"),
                num_parts=r.get("num_parts"), process_name=r.get("process_name"), writer_name=r.get("writer_name"),
                updated_at=r["updated_at"], run_id=r["run_id"], fingerprint=r["fingerprint"],
                upstream=tuple(r.get("upstream",[]) or [])
            ))
        return out

    # --- Planning helpers (producer vs consumer coverage on the same dims) ---
    def missing_partitions(self, producer: str, consumer: str, dims: Mapping[str,str], lookback_days: int) -> list[str]:
        # Example: compare producer rows vs consumer rows by (env, product_id, partition_key)
        now = date.today()
        start = now - timedelta(days=lookback_days)
        params = {
            "prod": producer, "cons": consumer,
            "st": start.isoformat(), "ed": now.isoformat(),
        }
        dim_filters = ""
        for i,(k,v) in enumerate(dims.items()):
            dim_filters += f" AND JSON_VALUE(partition_json, '$.{k}') = %(d{i})s"
            params[f"d{i}"] = v

        sql = f"""
        WITH prod AS (
          SELECT partition_key
          FROM markers_v2_files
          WHERE product_id=%(prod)s
            AND JSON_VALUE(partition_json, '$.dt') BETWEEN %(st)s AND %(ed)s {dim_filters}
          GROUP BY partition_key
        ),
        cons AS (
          SELECT partition_key
          FROM markers_v2_files
          WHERE product_id=%(cons)s {dim_filters}
          GROUP BY partition_key
        )
        SELECT partition_key
        FROM prod
        LEFT ANTI JOIN cons USING partition_key
        ORDER BY partition_key
        """
        df = self.client.query_df(sql, parameters=params)
        return list(df["partition_key"]) if not df.empty else []

    def coverage(self, product_id: str, dims: Mapping[str,str], window: tuple[date,date]) -> list[dict]:
        params = {
            "prod": product_id, "st": window[0].isoformat(), "ed": window[1].isoformat()
        }
        dim_filters = ""
        for i,(k,v) in enumerate(dims.items()):
            dim_filters += f" AND JSON_VALUE(partition_json, '$.{k}') = %(d{i})s"
            params[f"d{i}"] = v
        sql = f"""
        SELECT
          JSON_VALUE(partition_json,'$.dt') AS dt,
          count() AS files,
          sum(row_count) AS rows,
          max(updated_at) AS last_write
        FROM markers_v2_files
        WHERE product_id=%(prod)s
          AND JSON_VALUE(partition_json,'$.dt') BETWEEN %(st)s AND %(ed)s
          {dim_filters}
        GROUP BY dt
        ORDER BY dt
        """
        df = self.client.query_df(sql, parameters=params)
        return df.to_dict("records") if not df.empty else []
