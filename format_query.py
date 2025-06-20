#!/usr/bin/env python3
import sys
import duckdb
from datetime import datetime, date


def format_as_assert(db_path, query):
    """Format query result as Python assert statement"""
    conn = duckdb.connect(db_path)
    result = conn.execute(query).fetchall()
    columns = [desc[0] for desc in conn.description]

    if not result:
        print("No results found")
        return

    row = result[0]
    row_dict = dict(zip(columns, row))

    print("assert output == [")
    print("    {")

    for key, value in row_dict.items():
        if value is None:
            print(f'        "{key}": None,')
        elif isinstance(value, str):
            print(f'        "{key}": "{value}",')
        elif isinstance(value, (int, float)):
            print(f'        "{key}": {value},')
        elif isinstance(value, date):
            print(f'        "{key}": datetime.date({value.year}, {value.month}, {value.day}),')
        elif isinstance(value, datetime):
            print(
                f'        "{key}": datetime.datetime({value.year}, {value.month}, {value.day}, {value.hour}, {value.minute}, {value.second}),'
            )
        else:
            print(f'        "{key}": {repr(value)},')

    print("    }")
    print("]")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python format_query.py <db_path> <query>")
        sys.exit(1)

    db_path = sys.argv[1]
    query = sys.argv[2]
    format_as_assert(db_path, query)
