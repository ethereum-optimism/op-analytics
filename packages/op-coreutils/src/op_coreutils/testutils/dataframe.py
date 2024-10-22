from polars.testing import assert_frame_equal


def compare_schemas(msg, left, right):
    """Checks if schema on the right agrees with the schema on the left.

    The schema on the right can have more columns not defined on the left.
    """
    missing = []
    different = []
    for col, schema in left.schema.items():
        if col not in right.schema:
            missing.append(col)
        elif schema != right.schema[col]:
            different.append((col, schema, right.schema[col]))

    if not missing and not different:
        return

    raise AssertionError(f"{msg} Differences:\nmissing={missing}\ndifferent={different}")


def compare_dataframes(left, right):
    """Compare two dataframes.

    Raises and AssertionError with details when the dataframes are not equal.
    """
    # Compare the schemas from left to right
    compare_schemas("L->R", left, right)

    # Compare the schemas from left to right
    compare_schemas("R->L", right, left)

    # Compare the full dataframes
    assert_frame_equal(left, right)
