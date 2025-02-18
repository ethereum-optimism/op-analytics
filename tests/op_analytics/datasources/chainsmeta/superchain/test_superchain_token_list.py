from op_analytics.datasources.chainsmeta.dataaccess import ChainsMeta


def test_properties():
    assert ChainsMeta.SUPERCHAIN_TOKEN_LIST.db == "chainsmeta"
    assert ChainsMeta.SUPERCHAIN_TOKEN_LIST.table == "superchain_token_list_v1"
    assert ChainsMeta.SUPERCHAIN_TOKEN_LIST.root_path == "chainsmeta/superchain_token_list_v1"
