from op_datasets.logic.audits.basic import (
    distinct_block_numbers,
    monotonically_increasing,
    valid_hashes,
    txs_join_to_blocks,
)

registered_audit_funcs = [
    distinct_block_numbers,
    monotonically_increasing,
    valid_hashes,
    txs_join_to_blocks,
]

registered_audits = {func.__name__: func for func in registered_audit_funcs}

__all__ = ["registered_audits"]
