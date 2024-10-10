from op_datasets.logic.audits.blocks import (
    distinct_block_numbers,
    monotonically_increasing,
)

registered_audit_funcs = [
    distinct_block_numbers,
    monotonically_increasing,
]

registered_audits = {func.__name__: func for func in registered_audit_funcs}

__all__ = ["registered_audits"]
