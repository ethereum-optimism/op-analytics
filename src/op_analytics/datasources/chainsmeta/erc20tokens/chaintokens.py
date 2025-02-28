import itertools
from dataclasses import dataclass
from datetime import date

from op_analytics.coreutils.clickhouse.client import init_client
from op_analytics.coreutils.logger import bound_contextvars, structlog
from op_analytics.coreutils.request import new_session

from .tokens import Token

log = structlog.get_logger()

# ClickHouse columns for insert
COLUMNS = [
    "process_dt",
    "chain",
    "chain_id",
    "contract_address",
    "decimals",
    "symbol",
    "name",
    "total_supply",
    "block_number",
    "block_timestamp",
]

COLUMN_TYPE_NAMES = [
    "Date",
    "String",
    "Int32",
    "FixedString(42)",
    "UInt8",
    "String",
    "String",
    "UInt256",
    "UInt64",
    "DateTime",
]


# Chain-dependent delay between RPC requests
SPEED_BUMP = {
    "op": 1.0,
    "worldchain": 1.0,
}

DEFAULT_SPEED_BUMP = 0.4


@dataclass
class ChainTokens:
    """A batch of RPC requests for a single blockchain."""

    rpc_endpoint: str
    chain: str
    chain_id: int
    tokens: list[Token]

    def fetch(self, process_dt: date):
        """Call RPC and insert.

        Loop over all the tokens for this chain and store their metadata.

        The list of tokens is batched into chunks. We call the RPC for each
        chunk and then write the results to the database.
        """
        session = new_session()
        client = init_client("OPLABS")

        total_written_rows = 0
        with bound_contextvars(chain=self.chain):
            for batch in itertools.batched(self.tokens, n=30):
                data = []

                for token in batch:
                    token_metadata = token.call_rpc(
                        rpc_endpoint=self.rpc_endpoint,
                        session=session,
                        speed_bump=SPEED_BUMP.get(
                            token.chain, DEFAULT_SPEED_BUMP
                        ),  # avoid hitting the RPC rate limit
                    )

                    if token_metadata is None:
                        # an error was encountered
                        log.warning(f"error encountered for: {token}")
                        continue

                    row = token_metadata.to_dict()
                    row["process_dt"] = process_dt
                    data.append([row[_] for _ in COLUMNS])

                log.info(f"fetched token metadata from rpc for {len(batch)} tokens")

                result = client.insert(
                    table="chainsmeta.fact_erc20_token_metadata_v1",
                    data=data,
                    column_names=COLUMNS,
                    column_type_names=COLUMN_TYPE_NAMES,
                )
                log.info(
                    f"inserted token medatata {len(batch)} tokens, {result.written_rows} written rows"
                )
                total_written_rows += result.written_rows

        return total_written_rows
