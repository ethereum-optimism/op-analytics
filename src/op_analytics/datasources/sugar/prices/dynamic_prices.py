from typing import List, Any

from op_analytics.coreutils.logger import structlog

log = structlog.get_logger()


async def fetch_prices_with_retry(
    chain: Any, tokens: List[Any], initial_batch_size: int = 40
) -> List[Any]:
    """
    Fetch prices for a list of tokens using dynamic batch sizing.

    Retries with a reduced batch size if known errors occur.
    """
    prices = []
    index = 0
    current_batch_size = initial_batch_size

    while index < len(tokens):
        token_chunk = tokens[index : index + current_batch_size]
        try:
            batch_prices = await chain.get_prices(token_chunk)
            prices.extend(batch_prices)
            log.info(
                "Fetched token prices",
                start=index,
                end=index + current_batch_size,
                total=len(tokens),
            )
            index += current_batch_size
        except Exception as exc:
            error_str = str(exc)
            if "out of gas" in error_str or "0x3445e17c" in error_str:
                if current_batch_size > 1:
                    log.warning(
                        "Reducing batch size due to error",
                        error=error_str,
                        old_size=current_batch_size,
                        new_size=current_batch_size // 2,
                    )
                    current_batch_size = max(1, current_batch_size // 2)
                else:
                    log.error("Skipping token due to persistent error", error=error_str)
                    index += 1
            else:
                log.error("Unexpected error fetching prices", error=error_str)
                raise
    return [p for p in prices if p is not None]
