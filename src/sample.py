def test_providers_column_conversion_alternative():
    import polars as pl

    # Create a test dataframe with a list column
    df = pl.DataFrame({
        "id": [1, 2, 3],
        "providers": [
            ["provider1", "provider2"],
            ["provider3"],
            ["provider4", "provider5", "provider6"]
        ]
    })

    # Use map_elements for direct conversion
    result = df.with_columns(
        pl.col("providers")
        .map_elements(lambda x: ",".join(x))
        .alias("provider")
    )

    expected_providers = [
        "provider1,provider2",
        "provider3",
        "provider4,provider5,provider6"
    ]

    actual_providers = result.get_column("provider").to_list()
    assert actual_providers == expected_providers


if __name__ == "__main__":
    test_providers_column_conversion_alternative()