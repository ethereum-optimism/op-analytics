from ....core.types.product_ref import ProductRef
from ....platform.binders.sql import bind_sql_source
from ....platform.configs.bindings import SqlSourceConfig
from ....platform.clients.dune.client import DuneClient

# Common domain for RAW Dune products
DOMAIN = ("raw", "dune")

# Defaults passed into the recipe's request template (anchor_day comes from Partition["dt"] by default)
DEFAULTS = {
    "trailing_days": 1,
    "ending_days": 0,
    "single_chain": "none",
}

# If you're running with mock data, runner can be None; keep a client handy for switching to live.
DUNE_CLIENT = DuneClient()

# -----------------------------
# fees
# -----------------------------
FEES_RAW = ProductRef(DOMAIN, name="fees", version="v1")
FEES_SRC_BINDING = bind_sql_source(
    SqlSourceConfig(
        product=FEES_RAW,
        recipe_pkg="op.catalog.raw.dune.fees",  # expects templates/query.sql.j2, schema.py, (optional) mock_data.py
        runner=DUNE_CLIENT,                      # ignored if use_mock_data=True
        use_mock_data=True,                      # flip to False to hit Dune live
        defaults=DEFAULTS,
        # main_template="query.sql.j2",          # (default)
        # request_spec="config:Config",          # (default)
        # row_type_spec="schema:Record",         # (default)
        # mock_data_spec="mock_data:generate",   # (default)
        # anchor_from_partition_key="dt",        # (default)
    )
)

# -----------------------------
# gas
# -----------------------------
GAS_RAW = ProductRef(DOMAIN, name="gas", version="v1")
GAS_SRC_BINDING = bind_sql_source(
    SqlSourceConfig(
        product=GAS_RAW,
        recipe_pkg="op.catalog.raw.dune.gas",
        runner=DUNE_CLIENT,
        use_mock_data=True,
        defaults=DEFAULTS,
    )
)

# -----------------------------
# gas_fees
# -----------------------------
GAS_FEES_RAW = ProductRef(DOMAIN, name="gas_fees", version="v1")
GAS_FEES_SRC_BINDING = bind_sql_source(
    SqlSourceConfig(
        product=GAS_FEES_RAW,
        recipe_pkg="op.catalog.raw.dune.gasfees",
        runner=DUNE_CLIENT,
        use_mock_data=True,
        defaults=DEFAULTS,
    )
)

# Convenience export for bulk registration
BINDINGS = [FEES_SRC_BINDING, GAS_SRC_BINDING, GAS_FEES_SRC_BINDING]
