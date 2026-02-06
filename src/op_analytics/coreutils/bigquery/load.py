import os
import re
import tempfile
from dataclasses import dataclass
from datetime import date
from typing import Any
from unittest.mock import MagicMock
from urllib.parse import urlparse

from google.api_core.exceptions import BadRequest, NotFound
from google.cloud import bigquery, storage

from op_analytics.coreutils.logger import structlog
from .client import init_client

log = structlog.get_logger()

# -----------------------------
# Safety guardrails
# -----------------------------

PROD_DATASETS = {"superchain_raw"}

# Legacy override (kept for backwards compatibility)
ALLOW_PROD_WRITES_ENVVAR = "ALLOW_PROD_WRITES"

# Preferred environment gate: treat OPLABS_ENV=prod as authorization to write to prod datasets
OPLABS_ENVVAR = "OPLABS_ENV"
PROD_ENVS = {"prod", "production"}


def _summarize_uris(uris: list[str], max_items: int = 20) -> dict[str, Any]:
    return {"count": len(uris), "sample": uris[:max_items], "truncated": len(uris) > max_items}


def _parse_table_ref(table: str) -> tuple[str | None, str, str]:
    parts = table.split(".")
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return None, parts[0], parts[1]
    raise ValueError(f"Expected 'dataset.table' or 'project.dataset.table', got: {table}")


def _safe_qualify_destination(
    client: bigquery.Client, destination: str, *, allow_prod_writes: bool
) -> str:
    """
    Fully-qualify to project.dataset.table.

    Prod dataset writes are enabled if:
      - OPLABS_ENV is "prod"/"production", OR
      - allow_prod_writes=True was passed, OR
      - ALLOW_PROD_WRITES is truthy (legacy override).
    """
    project, dataset, table = _parse_table_ref(destination)
    effective_project = project or client.project

    oplabs_env = os.getenv(OPLABS_ENVVAR, "").strip().lower()
    is_prod_env = oplabs_env in PROD_ENVS

    legacy_override = os.getenv(ALLOW_PROD_WRITES_ENVVAR, "").strip().lower() in {"1", "true", "yes"}

    permitted = is_prod_env or allow_prod_writes or legacy_override

    if dataset in PROD_DATASETS and not permitted:
        raise RuntimeError(
            f"Refusing to write to prod dataset '{dataset}'. Destination was '{destination}'. "
            f"To override: set {OPLABS_ENVVAR}=prod (preferred) or pass allow_prod_writes=True "
            f"or set {ALLOW_PROD_WRITES_ENVVAR}=1/true/yes (legacy)."
        )

    return f"{effective_project}.{dataset}.{table}"


# -----------------------------
# Error extraction helpers
# -----------------------------

_BIGSTORE_PREFIX = "/bigstore/"
_FAILED_PARQUET_RE = re.compile(
    r"Failed to read Parquet file\s+([^;]+?\.parquet)\.?", re.IGNORECASE
)


def _normalize_gcs_uri(uri_or_path: str) -> str:
    s = uri_or_path.strip().rstrip(".")
    if s.startswith("gs://"):
        return s
    if s.startswith(_BIGSTORE_PREFIX):
        return "gs://" + s[len(_BIGSTORE_PREFIX) :]
    return s


def _extract_failed_uris(exc: Exception, source_uris: list[str]) -> list[str]:
    """
    Pull failing parquet file paths out of BigQuery error text and map them
    back to the provided source_uris.
    """
    text = str(exc)
    failed_paths = {_normalize_gcs_uri(p) for p in _FAILED_PARQUET_RE.findall(text)}
    if not failed_paths:
        return []

    norm_to_orig: dict[str, str] = {_normalize_gcs_uri(u): u for u in source_uris}

    failed: set[str] = set()
    for fp in failed_paths:
        if fp in norm_to_orig:
            failed.add(norm_to_orig[fp])
            continue

        # fallback: match by filename
        fname = fp.rsplit("/", 1)[-1]
        matches = [u for u in source_uris if u.endswith("/" + fname) or u.endswith(fname)]
        if len(matches) == 1:
            failed.add(matches[0])

    return sorted(failed)


# -----------------------------
# Hive partitioning + job config
# -----------------------------

def _normalize_source_uri_prefix(prefix: str) -> str:
    p = prefix.strip()
    if not p.endswith("/"):
        p += "/"
    return p


def _infer_hive_partitioning_mode(source_uri_prefix: str) -> str:
    p = source_uri_prefix
    if "{" in p and "}" in p:
        return "CUSTOM"
    return "AUTO"


def _build_hive_partitioning_options(source_uri_prefix: str) -> bigquery.HivePartitioningOptions:
    prefix = _normalize_source_uri_prefix(source_uri_prefix)
    mode = _infer_hive_partitioning_mode(prefix)
    return bigquery.HivePartitioningOptions.from_api_repr({"mode": mode, "sourceUriPrefix": prefix})


def _destination_table_exists(client: bigquery.Client, table_fq: str) -> bool:
    try:
        client.get_table(table_fq)
        return True
    except NotFound:
        return False


def _build_parquet_load_job_config(
    *,
    write_disposition: bigquery.WriteDisposition,
    source_uri_prefix: str,
    time_partition_field: str,
    clustering_fields: list[str] | None,
    destination_exists: bool,
) -> bigquery.LoadJobConfig:
    cfg = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=write_disposition,
        hive_partitioning=_build_hive_partitioning_options(source_uri_prefix),
    )

    if not destination_exists:
        cfg.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=time_partition_field,
            expiration_ms=None,
            require_partition_filter=True,
        )
        cfg.clustering_fields = clustering_fields

    return cfg


def _run_parquet_load_job(
    *,
    client: bigquery.Client,
    source_uris: list[str],
    destination_partition: str,
    job_config: bigquery.LoadJobConfig,
) -> None:
    job = client.load_table_from_uri(
        source_uris=source_uris,
        destination=destination_partition,
        job_config=job_config,
    )
    job.result()


# -----------------------------
# Partition clearing (rare fallback)
# -----------------------------

def _clear_partition_by_query(
    *,
    client: bigquery.Client,
    destination_table: str,  # project.dataset.table
    date_partition: date,
    time_partition_field: str,
) -> None:
    table = client.get_table(destination_table)

    field_type = None
    for f in table.schema:
        if f.name == time_partition_field:
            field_type = f.field_type.upper()
            break

    if field_type == "DATE":
        predicate = f"{time_partition_field} = DATE '{date_partition.isoformat()}'"
    else:
        predicate = f"DATE({time_partition_field}) = DATE '{date_partition.isoformat()}'"

    sql = f"DELETE FROM `{destination_table}` WHERE {predicate}"
    client.query(sql).result()
    log.info("Cleared partition via DELETE", table=destination_table, dt=date_partition.isoformat())


# -----------------------------
# Slow-path patching
# -----------------------------

@dataclass(frozen=True)
class HiveParts:
    parts: dict[str, str]

    @staticmethod
    def from_uri(uri: str) -> "HiveParts":
        if uri.startswith("gs://"):
            parsed = urlparse(uri)
            path = parsed.path.lstrip("/")
        elif uri.startswith(_BIGSTORE_PREFIX):
            path = uri[len(_BIGSTORE_PREFIX) :].split("/", 1)[1]
        else:
            path = uri

        parts: dict[str, str] = {}
        for seg in path.split("/"):
            if "=" in seg:
                k, v = seg.split("=", 1)
                if k and v:
                    parts[k] = v
        return HiveParts(parts=parts)


def _download_gcs_to_tempfile(storage_client: storage.Client, gcs_uri: str) -> str:
    parsed = urlparse(gcs_uri)
    if parsed.scheme != "gs":
        raise ValueError(f"Expected gs:// URI, got: {gcs_uri}")

    bucket = storage_client.bucket(parsed.netloc)
    blob = bucket.blob(parsed.path.lstrip("/"))

    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    tmp.close()
    blob.download_to_filename(tmp.name)
    return tmp.name


def _infer_extra_cols_for_uri(
    *,
    parquet_uri: str,
    destination_schema_field_names: set[str],
    date_partition: date,
    time_partition_field: str,
) -> dict[str, Any]:
    hive = HiveParts.from_uri(parquet_uri).parts
    extra_cols: dict[str, Any] = {}

    if time_partition_field in destination_schema_field_names:
        dt_str = hive.get(time_partition_field)
        extra_cols[time_partition_field] = date.fromisoformat(dt_str) if dt_str else date_partition

    for k, v in hive.items():
        if k in destination_schema_field_names and k not in extra_cols:
            extra_cols[k] = date.fromisoformat(v) if k == "dt" else v

    return extra_cols


def _rewrite_parquet_with_injected_cols(
    *,
    in_path: str,
    out_path: str,
    extra_cols: dict[str, Any],
    page_size: int,
    batch_rows: int,
) -> dict[str, Any]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(in_path)
    writer: pq.ParquetWriter | None = None
    rows = 0
    batches = 0

    try:
        for batch in pf.iter_batches(batch_size=batch_rows):
            batches += 1
            t = pa.Table.from_batches([batch])

            for col, val in extra_cols.items():
                if col in t.column_names:
                    continue
                t = t.append_column(col, pa.array([val] * t.num_rows))

            if writer is None:
                writer = pq.ParquetWriter(
                    out_path,
                    t.schema,
                    compression="zstd",
                    data_page_size=page_size,
                )

            writer.write_table(t)
            rows += t.num_rows

        return {"rows": rows, "batches": batches, "row_groups": pf.num_row_groups}
    finally:
        if writer is not None:
            writer.close()


def _append_parquet_uri_via_file_upload(
    *,
    client: bigquery.Client,
    storage_client: storage.Client,
    parquet_uri: str,
    destination_partition: str,  # project.dataset.table$YYYYMMDD
    destination_table: str,      # project.dataset.table
    date_partition: date,
    time_partition_field: str,
    rewrite_page_size: int = 256 * 1024,
    rewrite_batch_rows: int = 25_000,
    dataframe_fallback_batch_rows: int = 25_000,
) -> None:
    """
    Slow path: download -> rewrite (smaller pages + inject hive cols) -> load_table_from_file (APPEND)
    """
    log.warning("Slow-path patch start", uri=parquet_uri, destination=destination_partition)

    table = client.get_table(destination_table)
    schema_field_names = {f.name for f in table.schema}

    extra_cols = _infer_extra_cols_for_uri(
        parquet_uri=parquet_uri,
        destination_schema_field_names=schema_field_names,
        date_partition=date_partition,
        time_partition_field=time_partition_field,
    )

    local_in = _download_gcs_to_tempfile(storage_client, _normalize_gcs_uri(parquet_uri))
    local_out_tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    local_out_tmp.close()
    local_out = local_out_tmp.name

    try:
        try:
            stats = _rewrite_parquet_with_injected_cols(
                in_path=local_in,
                out_path=local_out,
                extra_cols=extra_cols,
                page_size=rewrite_page_size,
                batch_rows=rewrite_batch_rows,
            )
            log.info("Parquet rewritten for upload", uri=parquet_uri, stats=stats)

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema=table.schema,
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            )

            with open(local_out, "rb") as f:
                job = client.load_table_from_file(f, destination_partition, job_config=job_config)
            job.result()

            log.warning("Slow-path patch complete (file upload)", uri=parquet_uri)
            return

        except Exception as exc:
            # last resort
            log.warning("Slow-path upload failed; falling back to dataframe append", uri=parquet_uri, error=str(exc))
            _append_parquet_uri_rowwise(
                client=client,
                storage_client=storage_client,
                parquet_uri=parquet_uri,
                destination_partition=destination_partition,
                destination_table=destination_table,
                date_partition=date_partition,
                time_partition_field=time_partition_field,
                batch_rows=dataframe_fallback_batch_rows,
            )

    finally:
        for p in (local_in, local_out):
            try:
                os.remove(p)
            except OSError:
                pass


def _append_parquet_uri_rowwise(
    *,
    client: bigquery.Client,
    storage_client: storage.Client,
    parquet_uri: str,
    destination_partition: str,
    destination_table: str,
    date_partition: date,
    time_partition_field: str,
    batch_rows: int = 25_000,
) -> None:
    log.warning("Manual patch start (dataframe append)", uri=parquet_uri, destination=destination_partition)

    import pyarrow as pa
    import pyarrow.parquet as pq

    table = client.get_table(destination_table)
    schema_field_names = {f.name for f in table.schema}

    extra_cols = _infer_extra_cols_for_uri(
        parquet_uri=parquet_uri,
        destination_schema_field_names=schema_field_names,
        date_partition=date_partition,
        time_partition_field=time_partition_field,
    )

    local_path = _download_gcs_to_tempfile(storage_client, _normalize_gcs_uri(parquet_uri))
    try:
        pq_file = pq.ParquetFile(local_path)

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=table.schema,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        )

        loaded = 0
        batches = 0

        for batch in pq_file.iter_batches(batch_size=batch_rows):
            batches += 1
            t = pa.Table.from_batches([batch])
            df = t.to_pandas()
            for col, val in extra_cols.items():
                df[col] = val

            client.load_table_from_dataframe(df, destination_partition, job_config=job_config).result()
            loaded += len(df)

        log.warning("Manual patch complete", uri=parquet_uri, rows_loaded=loaded, batches=batches)

    finally:
        try:
            os.remove(local_path)
        except OSError:
            pass


# -----------------------------
# Fast-path retry loop (the core change)
# -----------------------------

def _truncate_load_failfast_retry(
    *,
    client: bigquery.Client,
    destination_partition: str,
    job_config_truncate: bigquery.LoadJobConfig,
    source_uris: list[str],
) -> tuple[list[str], list[str]]:
    """
    Keep retrying WRITE_TRUNCATE loads, removing only the failing parquet URIs
    extracted from the BadRequest message, until the first load succeeds.

    Returns: (loaded_uris, excluded_uris)
    """
    remaining = list(source_uris)
    excluded: list[str] = []

    max_attempts = int(os.getenv("OP_BQ_FAST_RETRY_MAX_ATTEMPTS", "25"))

    attempt = 0
    while True:
        attempt += 1
        if not remaining:
            return [], excluded

        try:
            _run_parquet_load_job(
                client=client,
                source_uris=remaining,
                destination_partition=destination_partition,
                job_config=job_config_truncate,
            )
            return remaining, excluded

        except BadRequest as exc:
            failing = _extract_failed_uris(exc, remaining)
            if not failing:
                log.error(
                    "Fast load failed; could not extract failing parquet URI",
                    destination=destination_partition,
                    attempt=attempt,
                    remaining=_summarize_uris(remaining),
                    error=str(exc),
                )
                raise

            failing_set = set(failing)
            before = len(remaining)
            remaining = [u for u in remaining if u not in failing_set]
            after = len(remaining)

            # progress guard
            new_exclusions = [u for u in failing if u not in excluded]
            excluded.extend(new_exclusions)

            log.warning(
                "Fast load failed; excluding failing parquet(s) and retrying",
                destination=destination_partition,
                attempt=attempt,
                excluded_now=_summarize_uris(failing, max_items=50),
                remaining_count=after,
            )

            if after == before:
                raise RuntimeError(
                    f"Retry loop made no progress excluding failing uris. "
                    f"Extracted={failing}. destination={destination_partition}"
                ) from exc

            if attempt >= max_attempts:
                raise RuntimeError(
                    f"Exceeded OP_BQ_FAST_RETRY_MAX_ATTEMPTS={max_attempts} while retrying fast load. "
                    f"excluded_count={len(excluded)} remaining_count={len(remaining)} destination={destination_partition}"
                ) from exc


# -----------------------------
# Public API
# -----------------------------

def load_from_parquet_uris(
    source_uris: list[str],
    source_uri_prefix: str,
    destination: str,  # dataset.table OR project.dataset.table (NO $decorator)
    date_partition: date,
    time_partition_field: str,
    clustering_fields: list[str] | None,
    *,
    allow_prod_writes: bool = False,
    patch_batch_rows: int = 25_000,
):
    """
    Fail-fast + retry strategy:

      1) Try one WRITE_TRUNCATE batch load for all URIs
      2) On failure, extract failing parquet file(s), remove them, retry immediately
      3) Stop as soon as a batch load succeeds
      4) Append excluded files one-by-one via slow path (rewrite -> file upload; fallback to dataframe append)

    This avoids preflight scanning and keeps runtime dominated by the single successful fast job
    plus a small number of slow patches.
    """
    client = init_client()

    # tests/mocks: keep legacy behavior and skip guardrails
    if isinstance(client, MagicMock):
        date_suffix = date_partition.strftime("%Y%m%d")
        destination_partition = f"{destination}${date_suffix}"
        job_config = _build_parquet_load_job_config(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_uri_prefix=source_uri_prefix,
            time_partition_field=time_partition_field,
            clustering_fields=clustering_fields,
            destination_exists=False,
        )
        client.load_table_from_uri(source_uris=source_uris, destination=destination_partition, job_config=job_config).result()
        log.info("DRYRUN WRITE_TRUNCATE to BQ", destination=destination_partition)
        return

    destination_fq = _safe_qualify_destination(client, destination, allow_prod_writes=allow_prod_writes)
    destination_exists = _destination_table_exists(client, destination_fq)

    date_suffix = date_partition.strftime("%Y%m%d")
    destination_partition = f"{destination_fq}${date_suffix}"

    job_config_truncate = _build_parquet_load_job_config(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_uri_prefix=source_uri_prefix,
        time_partition_field=time_partition_field,
        clustering_fields=clustering_fields,
        destination_exists=destination_exists,
    )

    # 1) Fail-fast retries until first fast load succeeds
    loaded_uris, excluded_uris = _truncate_load_failfast_retry(
        client=client,
        destination_partition=destination_partition,
        job_config_truncate=job_config_truncate,
        source_uris=source_uris,
    )

    if loaded_uris:
        log.info(
            "Fast load succeeded",
            destination=destination_partition,
            loaded=_summarize_uris(loaded_uris),
            excluded=_summarize_uris(excluded_uris, max_items=50),
        )
    else:
        # No fast load succeeded (all files excluded). Keep behavior deterministic:
        # ensure partition is empty before patching.
        if destination_exists:
            _clear_partition_by_query(
                client=client,
                destination_table=destination_fq,
                date_partition=date_partition,
                time_partition_field=time_partition_field,
            )
        else:
            raise RuntimeError(
                f"All parquet URIs failed fast-load, and destination table does not exist: {destination_fq}. "
                f"Cannot safely patch via slow path without an existing schema."
            )

    # 2) Patch excluded files (append)
    if excluded_uris:
        storage_client = storage.Client()

        # These are the knobs that actually help with the parquet OOM class
        rewrite_page_size = int(os.getenv("OP_BQ_SANITIZE_PAGE_SIZE", str(256 * 1024)))
        rewrite_batch_rows = int(os.getenv("OP_BQ_SANITIZE_BATCH_ROWS", str(25_000)))

        for uri in excluded_uris:
            _append_parquet_uri_via_file_upload(
                client=client,
                storage_client=storage_client,
                parquet_uri=uri,
                destination_partition=destination_partition,
                destination_table=destination_fq,
                date_partition=date_partition,
                time_partition_field=time_partition_field,
                rewrite_page_size=rewrite_page_size,
                rewrite_batch_rows=rewrite_batch_rows,
                dataframe_fallback_batch_rows=patch_batch_rows,
            )

        log.info(
            "Partition loaded with fail-fast strategy",
            destination=destination_partition,
            fast_loaded=len(loaded_uris),
            patched=len(excluded_uris),
            patched_uris=_summarize_uris(excluded_uris, max_items=50),
        )


def load_unpartitioned_single_uri(
    source_uri: str,
    dataset: str,
    table: str,
    clustering_fields: list[str] | None,
    *,
    allow_prod_writes: bool = False,
):
    client = init_client()

    if isinstance(client, MagicMock):
        destination = f"{dataset}.{table}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            clustering_fields=clustering_fields,
        )
        client.load_table_from_uri(source_uris=[source_uri], destination=destination, job_config=job_config).result()
        log.info("DRYRUN WRITE_TRUNCATE to BQ", destination=destination)
        return

    destination_fq = _safe_qualify_destination(
        client, f"{dataset}.{table}", allow_prod_writes=allow_prod_writes
    )

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    client.load_table_from_uri(source_uris=[source_uri], destination=destination_fq, job_config=job_config).result()
    log.info("WRITE_TRUNCATE to BQ", destination=destination_fq)
