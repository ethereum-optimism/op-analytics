import os
from dataclasses import dataclass
from datetime import date, datetime

import polars as pl

# ---- Core types & helpers ----




# ---- Minimal IO that writes Parquet + emits FileMarkers ----
import fsspec

from op.core.pipeline.build import build_pipeline
from op.core.types.dataset import Dataset
from op.core.types.file_marker import FileMarker
from op.core.types.location import FileLocation, MultiLocation
from op.core.types.partition import Partition
from op.core.types.product_ref import ProductRef
from op.platform.binders.duckdb import bind_duckdb_model
from op.platform.configs.bindings import PipelineConfig
from op.platform.configs.duckdb import DuckDBModelConfig
from op.platform.markers.legacy_clickhouse import LegacyClickHouseMarkerAdapter, LegacyProductBinding
from op.platform.markers.store import ClickHouseMarkerStore
from ...platform.markers.union_store import UnionMarkerStore
from ...core.types.catalog import Catalog
from ...core.types.env import EnvProfile


@dataclass
class FileProductIOWithMarkers:
    env: EnvProfile
    catalog: Catalog
    marker_store: UnionMarkerStore

    def _write_parquet_rows(self, rows: list[dict], path: str) -> int:
        df = pl.DataFrame(rows)
        fs, _, paths = fsspec.get_fs_token_paths(path)
        with fs.open(paths[0], "wb") as f:
            df.write_parquet(f)
        return len(df)

    def write(self, product: ProductRef, data: Dataset, part: Partition) -> str:
        # Resolve primary write location & concrete path (no wildcard)
        loc = self.catalog.resolve_write_location(self.env, product)
        if not isinstance(loc, FileLocation):
            raise TypeError(f"{product.id} is not a file-backed product for this example")
        base = format_uri(self.env, product, part, loc, wildcard=False)
        if not base.endswith("/"):
            base += "/"
        # timestamped object name to avoid clobber during concurrent runs
        out_path = base + f"out-{int(datetime.utcnow().timestamp())}.parquet"

        # Write parquet
        row_count = self._write_parquet_rows(
            [r if isinstance(r, dict) else r.__dict__ for r in data.rows],
            out_path,
        )

        # Record marker (v2)
        fm = FileMarker(
            env=self.env.name,
            product_id=product.id,
            root_path=f"{product.domain}/{product.name}_{product.version}",
            data_path=out_path,
            partition=part.values,
            row_count=row_count,
            process_name="FileProductIOWithMarkers",
            writer_name=os.uname().nodename,
            run_id="",  # filled by pipeline if you track per-step run_ids
            fingerprint="",  # could hash schema/version/row_count, optional here
            updated_at=datetime.utcnow(),
            upstream=tuple(),  # optionally pass upstream product_ids
        )
        self.marker_store.write_file_markers([fm])
        return out_path

    def read(self, product: ProductRef, part: Partition) -> Dataset:
        # Read from primary + legacy (union) and concat
        dfs: list[pl.DataFrame] = []
        for loc in self.catalog.resolve_read_locations(self.env, product):
            if not isinstance(loc, FileLocation):
                continue
            glob = format_uri(self.env, product, part, loc, wildcard=True)
            try:
                dfs.append(pl.scan_parquet(glob).collect())
            except Exception:
                # tolerate missing legacy paths
                continue
        if not dfs:
            return Dataset(rows=[])
        df = pl.concat(dfs, how="vertical_relaxed")
        return Dataset(rows=df.to_dicts())


def main():
    # ---------- 1) Products ----------
    FEES_RAW = ProductRef(domain=("raw","dune"), name="gas_fees_daily", version="v1")
    GAS_RAW  = ProductRef(domain=("raw","dune"), name="gas_used",       version="v1")
    FEES_CUR = ProductRef(domain=("staged","fees"), name="fees_enriched", version="v1")

    # ---------- 2) Environments ----------
    DEV = EnvProfile(
        name="dev",
        vars={
            "gcs_bucket": os.getenv("DEV_GCS_BUCKET", "my-dev-bucket"),
            "legacy_bucket": os.getenv("DEV_LEGACY_BUCKET", "my-legacy-bucket"),
            "root_prefix": "analytics",
        },
    )

    # ---------- 3) Catalog with MultiLocation (primary + legacy) ----------
    CATALOG = Catalog(entries={
        FEES_RAW: MultiLocation(
            primary=FileLocation("gs://{gcs_bucket}/{root_prefix}/raw/{domain}/{name}/{version}/dt={dt}/", "parquet"),
            legacy=(FileLocation("gs://{legacy_bucket}/dailydata/{name}_{version}/dt={dt}/", "parquet"),),
        ),
        GAS_RAW: MultiLocation(
            primary=FileLocation("gs://{gcs_bucket}/{root_prefix}/raw/{domain}/{name}/{version}/dt={dt}/", "parquet"),
            legacy=(FileLocation("gs://{legacy_bucket}/dailydata/{name}_{version}/dt={dt}/", "parquet"),),
        ),
        FEES_CUR: MultiLocation(
            primary=FileLocation("gs://{gcs_bucket}/{root_prefix}/staged/{domain}/{name}/{version}/dt={dt}/", "parquet"),
            legacy=(),  # new curated path only
        ),
    })

    # ---------- 4) Marker store: v2 (CH) + legacy adapter -> Union ----------
    # v2 ClickHouse marker store
    v2_store = ClickHouseMarkerStore(
        host=os.getenv("CH_HOST","localhost"),
        username=os.getenv("CH_USER","default"),
        password=os.getenv("CH_PASS",""),
        database=os.getenv("CH_DB","analytics"),
    )

    # Legacy adapter: map legacy table columns to our v2 FileMarker fields
    import clickhouse_connect
    ch_client = clickhouse_connect.get_client(
        host=os.getenv("CH_HOST","localhost"),
        username=os.getenv("CH_USER","default"),
        password=os.getenv("CH_PASS",""),
        database=os.getenv("CH_DB","analytics"),
    )
    legacy = LegacyClickHouseMarkerAdapter(
        client=ch_client,
        env=DEV.name,
        products={
            FEES_RAW.id: LegacyProductBinding(
                product_id=FEES_RAW.id,
                table="ops.blockbatch_markers",
                partition_cols={"dt": "dt"},       # add {"chain":"chain"} if legacy has it
                data_path_col="data_path",
                row_count_col="row_count",
                span_min_col="min_block",
                span_max_col="max_block",
                span_kind_literal="block",
                updated_at_col="updated_at",
                process_name_col="process_name",
                writer_name_col="writer_name",
                root_path_literal="dune/gas_fees_daily_v1",
            ),
            GAS_RAW.id: LegacyProductBinding(
                product_id=GAS_RAW.id,
                table="ops.blockbatch_markers",
                partition_cols={"dt": "dt"},
                data_path_col="data_path",
                row_count_col="row_count",
                updated_at_col="updated_at",
                root_path_literal="dune/gas_used_v1",
            ),
        },
    )

    MARKERS = UnionMarkerStore(primary=v2_store, legacy=legacy)

    # ---------- 5) IO with markers ----------
    IO = FileProductIOWithMarkers(env=DEV, catalog=CATALOG, marker_store=MARKERS)

    # ---------- 6) Sources (Dune SQL recipes) ----------
    # Use mock data for a local run; flip `use_mock_data=False` to hit Dune with a real key.
    fees_src = bind_sql_source(SqlSourceConfig(
        product=FEES_RAW,
        recipe_pkg="op_analytics.platform.sources.dune.gas_fees_daily",
        runner=DuneClient(),         # ignored when use_mock_data=True
        use_mock_data=True,
        defaults={"trailing_days": 2, "ending_days": 0, "single_chain": "none"},
    ))
    gas_src = bind_sql_source(SqlSourceConfig(
        product=GAS_RAW,
        recipe_pkg="op_analytics.platform.sources.dune.gas",
        runner=DuneClient(),
        use_mock_data=True,
        defaults={"trailing_days": 2, "ending_days": 0, "single_chain": "none"},
    ))

    # ---------- 7) DuckDB transform (file-native; reads directly from Catalog) ----------
    # Set up a session with httpfs + GCS HMAC (for real GCS reading)
    sess = DuckDBSessionConfig(
        enable_httpfs=True,
        gcs=GcsHmacAuth(
            key_id=os.getenv("GCS_HMAC_KEY_ID","test-key"),
            secret=os.getenv("GCS_HMAC_SECRET","test-secret"),
        ),
        udf_modules=("op_analytics.platform.udf.common",),  # optional shared UDFs
    )

    # Inline SQL; you can also point to a .sql.j2 via sql_template_path.
    sql_text = """
    WITH fees AS (SELECT * FROM {{ read_parquet('fees') }}),
         gas  AS (SELECT * FROM {{ read_parquet('gas')  }})
    SELECT
      f.blockchain,
      f.dt,
      f.tx_fee_usd,
      g.sum_evm_gas_used,
      CASE WHEN is_positive(g.sum_evm_gas_used)
           THEN f.tx_fee_usd / g.sum_evm_gas_used ELSE NULL END AS fee_per_gas
    FROM fees f JOIN gas g USING (blockchain, dt)
    """

    duck_cfg = DuckDBModelConfig(
        output=FEES_CUR,
        inputs={"fees": FEES_RAW, "gas": GAS_RAW},
        session=sess,
        sql_text=sql_text,
        format="parquet",
        overwrite=True,
        output_filename="part-000.parquet",
    )
    duck_node = bind_duckdb_model(duck_cfg)

    # ---------- 8) Build pipeline ----------
    pipe = build_pipeline(PipelineConfig(
        sources=[fees_src, gas_src],
        nodes=[duck_node],
        default_io=IO,               # used for source writes; DuckDB node writes itself, but markers still capture
    ))

    # ---------- 9) Run one day ----------
    part = Partition({"dt": date.today().isoformat()})
    # Ingest both raw products (sources) and then the curated transform
    pipe.run(part)

    # ---------- 10) Ask markers about coverage & backlog ----------
    cov = MARKERS.coverage(FEES_RAW.id, dims={}, window=(date.today(), date.today()))
    print("Coverage (fees/raw today):", cov)

    missing = MARKERS.missing_partitions(
        producer=FEES_RAW.id,
        consumer=FEES_CUR.id,
        dims={},            # add {"chain":"op"} if you partition by chain too
        lookback_days=7,
    )
    print("Backlog (raw present, curated missing):", missing)


if __name__ == "__main__":
    main()
