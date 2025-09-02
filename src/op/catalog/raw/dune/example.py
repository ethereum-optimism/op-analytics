from op.core.types.partition import Partition
from op.platform.clients.dune.client import DuneClient
from op.platform.binders.sql import bind_sql_source
from op.catalog.raw.dune.products import FEES_RAW

if __name__ == "__main__":
    FEES_SRC_BINDING = bind_sql_source(
        "op.catalog.raw.dune.fees",
        product=FEES_RAW,  # required now
        runner=DuneClient(),  # or None with use_mock_data=True
        use_mock_data=True,  # flip False to run against Dune
        defaults={"trailing_days": 1, "single_chain": "none"},
    )
    for r in FEES_SRC_BINDING.source.resource.fetch(Partition(values={"dt": "2023-01-01"})):
        print(r)
