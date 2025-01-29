from op_analytics.coreutils.misc import camel_to_snake

from .conversion import safe_uint256


def make_struct(abi_entry: dict | str, decoded_result: tuple) -> dict:
    if abi_entry["type"].endswith("[]"):
        return [
            make_struct(
                abi_entry=dict(abi_entry, type=abi_entry["type"][:-2]),
                decoded_result=item,
            )
            for item in decoded_result
        ]

    if abi_entry["type"] == "function":
        result = {}
        for i, field in enumerate(abi_entry["inputs"]):
            result[camel_to_snake(field["name"])] = make_struct(field, decoded_result[i])
        return result

    if abi_entry["type"] == "event":
        result = {}
        for i, field in enumerate(abi_entry["inputs"]):
            if field["indexed"]:
                continue
            result[camel_to_snake(field["name"])] = make_struct(field, decoded_result[i])
        return result

    if abi_entry["type"] == "tuple":
        result = {}
        for i, field in enumerate(abi_entry["components"]):
            result[camel_to_snake(field["name"])] = make_struct(field, decoded_result[i])
        return result

    if abi_entry["type"] == "address":
        return decoded_result

    if abi_entry["type"].startswith("uint"):
        return {
            "value": safe_uint256(decoded_result),
            "lossless": str(decoded_result),
        }

    if abi_entry["type"].startswith("bytes"):
        return "0x" + decoded_result.hex()

    return decoded_result


def make_duckdb_type(abi_entry: dict) -> str:
    if abi_entry["type"].endswith("[]"):
        return make_duckdb_type(dict(abi_entry, type=abi_entry["type"][:-2])) + "[]"

    if abi_entry["type"] == "function":
        fields = [
            camel_to_snake(field["name"]) + " " + make_duckdb_type(field)
            for field in abi_entry["inputs"]
        ]
        return "STRUCT(" + ", ".join(fields) + ")"

    if abi_entry["type"] == "event":
        fields = []
        for field in abi_entry["inputs"]:
            if field["indexed"]:
                continue
            fields.append(camel_to_snake(field["name"]) + " " + make_duckdb_type(field))
        return "STRUCT(" + ", ".join(fields) + ")"

    if abi_entry["type"] == "tuple":
        fields = [
            camel_to_snake(field["name"]) + " " + make_duckdb_type(field)
            for field in abi_entry["components"]
        ]
        return "STRUCT(" + ", ".join(fields) + ")"

    if abi_entry["type"] == "address":
        return "VARCHAR"

    if abi_entry["type"].startswith("uint"):
        return "STRUCT(value BIGINT, lossless VARCHAR)"

    if abi_entry["type"].startswith("bytes"):
        return "VARCHAR"

    if abi_entry["type"] == "bool":
        return "BOOL"

    raise NotImplementedError(f"Unknown type: {abi_entry['type']}")
