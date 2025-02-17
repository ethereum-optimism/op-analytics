import os
from collections import defaultdict
from datetime import date

import polars as pl

from op_analytics.coreutils.clickhouse.oplabs import run_query_oplabs
from op_analytics.coreutils.logger import structlog

from .chaintokens import ChainTokens
from .endpoints import get_rpc_for_chain
from .tokens import Token

log = structlog.get_logger()

# Percentile cutoff for tokens that will be updated every day.
PERCENTILE = 0.90


def find_tokens(process_dt: date) -> list[ChainTokens]:
    """Find the tokens that need to be updated on a given date.

    The results are broken down by chain and a ChainsTokens object is
    returned for each chain.
    """

    with open(os.path.join(os.path.dirname(__file__), "sql/tokens_to_update.sql")) as fsql:
        df = run_query_oplabs(
            query=fsql.read(),
            parameters={
                "percentile": PERCENTILE,
                "process_dt": process_dt,
            },
        )

        pending_df = df.filter(pl.col("already_fetched") == 0)
        log.info(f"found {len(pending_df)}/{len(df)} pending tokens to update")
        tokens_to_update = pending_df.to_dicts()

    # Group the tokens by blockchain.
    tokens_by_chain = defaultdict(list)
    for row in tokens_to_update:
        token = Token.from_database_row(row)
        tokens_by_chain[(token.chain, token.chain_id)].append(token)

    # Split the list for each blockchain into chunks of 10 tokens.
    # We will issue a single JSON-RPC batch for each chunk.
    result = []
    for (chain, chain_id), tokens in tokens_by_chain.items():
        result.append(
            ChainTokens(
                rpc_endpoint=get_rpc_for_chain(chain_id=chain_id),
                chain=chain,
                chain_id=chain_id,
                tokens=tokens,
            )
        )

    return result
