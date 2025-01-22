from eth_typing_lite import TypeStr


def abi_inputs_to_typestr(abi_entry: dict) -> list[TypeStr]:
    """Create TypeStr for each of of the input parameters in an Ethereum ABI entry.

    Args:
        abi_entry (dict): A dictionary containing the ABI specification for a function,
                         including input parameter types.

    Returns:
        A list of TypeStr strings. One for each input parameter.
    """
    return [process_type(param) for param in abi_entry["inputs"]]


def process_type(param: dict):
    """Convert a single parameter definition to its canonical type string representation."""

    # Handle structs (tuples in ABI)
    if param["type"] == "tuple":
        component_types = [process_type(comp) for comp in param.get("components", [])]
        return f"({','.join(component_types)})"

    # Handle arrays of primitive types or structs
    elif param["type"].endswith("[]"):
        base_type = param["type"][:-2]
        if base_type == "tuple":
            # Recursively process tuple arrays
            component_types = [process_type(comp) for comp in param.get("components", [])]
            return f"({','.join(component_types)})[]"
        return param["type"]

    # Handle primitive types
    else:
        return param["type"]
