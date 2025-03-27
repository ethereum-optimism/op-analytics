from op_analytics.coreutils.logger import structlog
from op_analytics.datapipeline.chains import goldsky_chains

log = structlog.get_logger()


def normalize_chains(chains: str) -> list[str]:
    """Parse a comma-separated list of chains into an actual list.

    Supports some special ALL_CAPS values that automaticlly bring in
    some chains.
    """

    # If for some reason we need to force exclude a chain, add it here.
    not_included = set()

    result = set()
    for chain in chains.split(","):
        if chain == "ALL":
            result.update(goldsky_chains.goldsky_mainnet_chains())
            result.update(goldsky_chains.goldsky_testnet_chains())
        elif chain == "MAINNETS":
            result.update(goldsky_chains.goldsky_mainnet_chains())
        elif chain == "TESTNETS":
            result.update(goldsky_chains.goldsky_testnet_chains())
        elif chain.startswith("-"):
            not_included.add(chain.removeprefix("-").strip())
        else:
            result.add(chain.strip())

    excluded = result.intersection(not_included)
    for chain in excluded:
        log.warning(f"Excluding chain: {chain!r}")

    return list(result - not_included)


def normalize_blockbatch_models(models: str) -> list[str]:
    """Parse a comma-separated list of models into an actual list."""

    not_included = set()

    result = set()
    for model in models.split(","):
        if model == "MODELS":
            result.add("contract_creation")
            result.add("refined_traces")
            result.add("token_transfers")
            result.add("account_abstraction_prefilter")
            result.add("account_abstraction")
        elif model == "GROUPA":
            result.add("contract_creation")
            result.add("token_transfers")
            result.add("account_abstraction_prefilter")
            result.add("account_abstraction")
        elif model == "GROUPB":
            result.add("refined_traces")
        elif model.startswith("-"):
            not_included.add(model.removeprefix("-").strip())
        else:
            result.add(model.strip())

    excluded = result.intersection(not_included)
    for model in excluded:
        log.warning(f"Excluding model: {model!r}")

    return list(result - not_included)
