from dataclasses import dataclass

from eth_typing_lite import TypeStr


@dataclass
class NamedParam:
    name: str
    typestr: TypeStr


def abi_inputs_to_params(abi_entry: dict, include_indexed: bool = False) -> list[NamedParam]:
    """Create TypeStr for each of of the input parameters in an Ethereum ABI entry.

    Args:
        abi_entry (dict): A dictionary containing the ABI specification for a function,
                         including input parameter types.

    Returns:
        A list of TypeStr strings. One for each input parameter.
    """

    is_log = abi_entry.get("type") == "event"

    result = []
    for param in abi_entry["inputs"]:
        if is_log and param["indexed"] and not include_indexed:
            continue
        result.append(
            NamedParam(
                name=param["name"],
                typestr=process_type(param),
            )
        )
    return result


def abi_inputs_to_typestr(abi_entry: dict, include_indexed: bool = False) -> list[TypeStr]:
    return [
        _.typestr
        for _ in abi_inputs_to_params(
            abi_entry=abi_entry,
            include_indexed=include_indexed,
        )
    ]


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
