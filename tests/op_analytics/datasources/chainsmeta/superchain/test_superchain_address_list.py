from op_analytics.datasources.chainsmeta.dataaccess import ChainsMeta


def test_properties():
    assert ChainsMeta.SUPERCHAIN_ADDRESS_LIST.db == "chainsmeta"
    assert ChainsMeta.SUPERCHAIN_ADDRESS_LIST.table == "superchain_address_list_v1"
    assert ChainsMeta.SUPERCHAIN_ADDRESS_LIST.root_path == "chainsmeta/superchain_address_list_v1"
