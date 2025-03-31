from sugar.chains import AsyncOPChain, AsyncBaseChain


def chain_cls_to_str(chain_cls: type) -> str:
    """
    Convert a chain class like OPChain → 'op' or BaseChain → 'base'.
    Extend this if you support more chains in future.
    """
    name = chain_cls.__name__.lower()
    if "opchain" in name:
        return "op"
    elif "basechain" in name:
        return "base"
    # Fallback or raise an error for unhandled cases
    raise ValueError(f"Unrecognized chain class: {chain_cls.__name__}")


# Map our chain names to the sugar sdk Chain class.
SUGAR_CHAINS = {
    "op": AsyncOPChain(),
    "base": AsyncBaseChain(),
}
