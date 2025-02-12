import os
from unittest.mock import patch

from op_analytics.coreutils.partitioned import dailydatawrite
from op_analytics.coreutils.partitioned.location import DataLocation


def mock_location():
    return DataLocation.GCS


os.environ["ALLOW_WRITE"] = "true"


def backfill_lend_borrow_pools():
    with patch.object(dailydatawrite, "determine_location", mock_location):
        # NOTE: Before running these the LAST_N_DAYS values were manually modified.

        # Protocols (modified to 120 days)
        from op_analytics.datasources.defillama import lend_borrow_pools

        result = lend_borrow_pools.execute_pull()


if __name__ == "__main__":
    backfill_lend_borrow_pools()
