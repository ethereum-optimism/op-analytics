from eth_typing_lite import TypeStr


def abi_entry_to_typestr(abi_entry: dict, include_indexed: bool = False) -> list[TypeStr]:
    """Convert each parameter value in the ABI entry to its typestr and return as a list."""

    is_log = abi_entry.get("type") == "event"

    result = []
    for param in abi_entry["inputs"]:
        if is_log and param["indexed"] and not include_indexed:
            continue
        result.append(process_type(param))
    return result


def process_type(param: dict):
    """Convert a single parameter to its typestr representation."""

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
