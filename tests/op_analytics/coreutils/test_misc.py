from op_analytics.coreutils.misc import camel_to_snake


def test_camel_to_snake():
    EXAMPLES = [
        ("someColumnName", "some_column_name"),
        ("logoURI", "logo_uri"),
        ("tokenID", "token_id"),
        ("baseURL", "base_url"),
        ("simpleText", "simple_text"),
        ("APIEndpoint", "api_endpoint"),
        ("JSONResponse", "json_response"),
        ("camelCase", "camel_case"),
        ("PascalCase", "pascal_case"),
        ("HTTPStatusCode", "http_status_code"),
    ]

    for camel, expected_snake in EXAMPLES:
        assert camel_to_snake(camel) == expected_snake
