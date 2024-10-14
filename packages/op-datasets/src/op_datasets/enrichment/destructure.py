from collections import namedtuple

import polars as pl


def desctructure_event_args(dataframes) -> pl.Series:
    breakpoint()
    return dataframes["logs"]["data"]


class DestructureError(Exception):
    pass


Ref = namedtuple("Ref", ["pos", "points_to"])


def destructure_non_indexed_args(data: str):
    if not data.startswith("0x"):
        raise DestructureError("invalid data: does not start with 0x")

    try:
        view = memoryview(bytes.fromhex(data[2:]))
    except ValueError as ex:
        raise DestructureError("invalid data: not a valid hex string") from ex

    if len(view) % 32 != 0:
        # Data is not aligned to 32-bytes. This is a non-standard case and will be ignored.
        return None

    print()
    possible_references = []
    pos = 0
    for pos in range(0, len(view), 32):
        as_number = int.from_bytes(bytes(view[pos : pos + 32]), "big")

        is_aligned = as_number % 32 == 0

        # a value is not a reference if it points to the start of data (i.e. if it is equal to 0)
        # it must at least point to the second aligned position.
        not_start = as_number // 32 > 0

        # a value is not a reference if it points to beyond the end of the data.
        not_end = as_number < len(view)

        if is_aligned and not_start and not_end:
            possible_references.append(Ref(pos, as_number))

        if as_number < len(view):
            print(f"{pos//32:02d} ->  {view[pos : pos + 32].hex()}  {as_number}  {as_number / 32}")
        else:
            print(f"{pos//32:02d} ->  {view[pos : pos + 32].hex()}")

    if not possible_references:
        # This means we are only dealing with non-dynamic types.
        return ["0x" + view[pos : pos + 32].hex() for pos in range(0, len(view), 32)]

    # Attempt to follow references to get only the valid ones.
    min_valid = 0
    valid = [possible_references[min_valid]]
    head_end = possible_references[min_valid].points_to

    # next_idx = 1
    # next_ref = possible_references[next_idx]

    # if head_end < next_ref.points_to and next_ref.points_to > valid[-1].points_to:
    #     valid.append(possible_references[next_idx])
    # else:
    #     pass

    return ["0x" + view[pos : pos + 32].hex() for pos in range(0, head_end, 32)]
