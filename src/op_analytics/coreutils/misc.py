import json
import re

import polars as pl


CAMEL_CASE_STARTS = re.compile(
    # The regex below can be understood as follows:
    # preceded by lowercase
    # followed by uppercase
    #   OR
    # preceded by lowercase
    # followed by uppercase, then lowercase
    r"""
        (?<=[a-z])
        (?=[A-Z])
        |
        (?<=[A-Z])
        (?=[A-Z][a-z])
    """,
    # This flag allows you to write regular expressions that look nicer and are
    # more readable by allowing you to visually separate logical sections of
    # the pattern and add comments. Whitespace within the pattern is ignored.
    re.X,
)


def camel_to_snake(s: str) -> str:
    """
    Convert a camelCase or PascalCase string into snake_case,
    while handling certain acronyms as single blocks.

    Example:
      "someColumnName" -> "some_column_name"
      "logoURI"        -> "logo_uri"
    """

    return CAMEL_CASE_STARTS.sub("_", s).lower()


def raise_for_schema_mismatch(actual_schema: pl.Schema, expected_schema: pl.Schema):
    if actual_schema != expected_schema:
        actual = json.dumps([f"{_}  {str(__)}" for _, __ in actual_schema.items()])
        expected = json.dumps([f"{_}  {str(__)}" for _, __ in expected_schema.items()])

        raise Exception(f"""
        Mismatching schema:
        
        {actual}
        
        {expected}
        """)
