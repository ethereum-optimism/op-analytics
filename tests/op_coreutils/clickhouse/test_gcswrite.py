from op_analytics.coreutils.clickhouse.gcswrite import construct_partition_by


def test_construct_partition_by():
    actual = construct_partition_by(["dt", "chain"])
    assert actual == "CONCAT('/dt=', toString(dt), '/chain=', toString(chain))"

    actual = construct_partition_by(["dt"])
    assert actual == "CONCAT('/dt=', toString(dt))"
