from op_analytics.coreutils.partitioned.writerduckdb import OutputDuckDBRelation
from op_analytics.coreutils.duckdb_inmem import init_client
from op_analytics.coreutils.partitioned.partition import Partition


def test_output_relation():
    ctx = init_client()

    ctx.client.sql("""
    CREATE TABLE city_airport 
    (
        dt VARCHAR,
        entity VARCHAR,
        city_name VARCHAR, 
        iata VARCHAR, 
        code INTEGER
    );""")

    ctx.client.sql("""
    INSERT INTO city_airport VALUES
    ('2025-03-14', 'airport', 'Amsterdam', 'AMS', 1),
    ('2025-03-14', 'airport', 'Rotterdam', 'RTM', 2),
    ('2025-03-14', 'airport', 'Eindhoven', 'EIN', 3),
    ('2025-03-14', 'airport', 'Groningen', 'GRQ', 4);    
    """)

    output = OutputDuckDBRelation(
        relation=ctx.as_relation("city_airport"),
        root_path="my/root",
        partition=Partition.from_tuples([("dt", "2025-03-14"), ("entity", "airport")]),
    )

    actual = output.without_partition_cols().pl().to_dicts()
    assert actual == [
        {"city_name": "Amsterdam", "iata": "AMS", "code": 1},
        {"city_name": "Rotterdam", "iata": "RTM", "code": 2},
        {"city_name": "Eindhoven", "iata": "EIN", "code": 3},
        {"city_name": "Groningen", "iata": "GRQ", "code": 4},
    ]
