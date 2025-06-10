from op_analytics.datasources.chainsmeta.dataaccess import ChainsMeta


def test_properties():
    assert ChainsMeta.SUPERCHAIN_CHAIN_LIST.db == "chainsmeta"
    assert ChainsMeta.SUPERCHAIN_CHAIN_LIST.table == "superchain_chain_list_v1"
    assert ChainsMeta.SUPERCHAIN_CHAIN_LIST.root_path == "chainsmeta/superchain_chain_list_v1"
