from datetime import datetime
from unittest.mock import patch

from op_datasets.etl.ingestion.batches import BlockBatch
from op_datasets.etl.ingestion.status import is_safe


@patch("op_datasets.etl.ingestion.status.now_seconds", lambda: datetime(2024, 10, 24, 19, 44))
def test_is_safe():
    batch = BlockBatch(chain="fraxtal", min=11320000, max=11340000)

    chain_max_block = 11493429
    chain_max_ts = 1729797569

    ans = is_safe(
        max_requested_timestamp=None,
        block_batch=batch,
        chain_max_block=chain_max_block,
        chain_max_ts=chain_max_ts,
    )
    assert ans


@patch("op_datasets.etl.ingestion.status.now_seconds", lambda: datetime(2024, 10, 24, 19, 44))
def test_is_not_safe_for_number():
    batch = BlockBatch(chain="fraxtal", min=0, max=1100)

    chain_max_block = 2000
    chain_max_ts = 1729797569

    ans = is_safe(
        max_requested_timestamp=None,
        block_batch=batch,
        chain_max_block=chain_max_block,
        chain_max_ts=chain_max_ts,
    )
    assert not ans


@patch("op_datasets.etl.ingestion.status.now_seconds", lambda: datetime(2024, 10, 24, 23, 44))
def test_is_not_safe_for_provider():
    batch = BlockBatch(chain="fraxtal", min=11320000, max=11340000)

    chain_max_block = 11493429
    chain_max_ts = 1729797569

    ans = is_safe(
        max_requested_timestamp=None,
        block_batch=batch,
        chain_max_block=chain_max_block,
        chain_max_ts=chain_max_ts,
    )
    assert not ans
