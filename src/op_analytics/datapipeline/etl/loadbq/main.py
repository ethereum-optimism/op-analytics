from op_analytics.datapipeline.etl.ingestion.reader.rootpaths import RootPath
from .load import load_blockbatch_to_bq


def load_superchain_raw_to_bq(
    range_spec: str,
    dryrun: bool,
    force_complete: bool,
    force_not_ready: bool,
):
    return load_blockbatch_to_bq(
        range_spec=range_spec,
        root_paths_to_read=[
            RootPath.of("ingestion/blocks_v1"),
            RootPath.of("ingestion/logs_v1"),
            RootPath.of("ingestion/traces_v1"),
            RootPath.of("ingestion/transactions_v1"),
        ],
        bq_dataset_name="superchain_raw",
        table_name_map={
            "blocks_v1": "blocks",
            "logs_v1": "logs",
            "traces_v1": "traces",
            "transactions_v1": "transactions",
        },
        markers_table="superchain_raw_bigquery_markers",
        dryrun=dryrun,
        force_complete=force_complete,
        force_not_ready=force_not_ready,
        excluded_chains=["kroma"],
    )


def load_superchain_4337_to_bq(
    range_spec: str,
    dryrun: bool,
    force_complete: bool,
    force_not_ready: bool,
):
    return load_blockbatch_to_bq(
        range_spec=range_spec,
        root_paths_to_read=[
            RootPath.of("blockbatch/account_abstraction/enriched_entrypoint_traces_v2"),
            RootPath.of("blockbatch/account_abstraction/useroperationevent_logs_v2"),
        ],
        bq_dataset_name="superchain_4337",
        table_name_map={},
        markers_table="superchain_4337_bigquery_markers",
        dryrun=dryrun,
        force_complete=force_complete,
        force_not_ready=force_not_ready,
        excluded_chains=["kroma", "xterio"],
    )
