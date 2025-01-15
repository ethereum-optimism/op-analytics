from datetime import datetime
from unittest.mock import patch

from op_analytics.coreutils.rangeutils.blockrange import ChainMaxBlock
from op_analytics.datapipeline.etl.ingestion.batches import BlockBatch
from op_analytics.datapipeline.etl.ingestion.status import is_safe


@patch(
    "op_analytics.datapipeline.etl.ingestion.status.now_trunc",
    lambda: datetime(2024, 10, 24, 19, 44),
)
def test_is_safe():
    batch = BlockBatch(chain="fraxtal", min=11320000, max=11340000)

    chain_max_block = ChainMaxBlock(
        chain="fraxtal",
        ts=1729797569,
        number=11493429,
    )

    ans = is_safe(
        requested_max_timestamp=None,
        requested_max_block=batch.max,
        chain_max_block=chain_max_block,
    )
    assert ans


@patch(
    "op_analytics.datapipeline.etl.ingestion.status.now_trunc",
    lambda: datetime(2024, 10, 24, 19, 44),
)
def test_is_not_safe_for_number():
    batch = BlockBatch(chain="fraxtal", min=0, max=1100)

    chain_max_block = ChainMaxBlock(
        chain="fraxtal",
        ts=1729797569,
        number=2000,
    )

    ans = is_safe(
        requested_max_timestamp=None,
        requested_max_block=batch.max,
        chain_max_block=chain_max_block,
    )
    assert not ans


@patch(
    "op_analytics.datapipeline.etl.ingestion.status.now_trunc",
    lambda: datetime(2024, 10, 24, 23, 44),
)
def test_is_not_safe_for_provider():
    batch = BlockBatch(chain="fraxtal", min=11320000, max=11340000)

    chain_max_block = ChainMaxBlock(
        chain="fraxtal",
        ts=1729797569,
        number=11493429,
    )

    ans = is_safe(
        requested_max_timestamp=None,
        requested_max_block=batch.max,
        chain_max_block=chain_max_block,
    )
    assert not ans
