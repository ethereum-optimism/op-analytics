from op_analytics.datasources.ethereum_optimism.dataaccess import EthereumOptimism


def test_properties():
    assert EthereumOptimism.SUPERCHAIN_TOKEN_LIST.db == "ethereumoptimism"
    assert EthereumOptimism.SUPERCHAIN_TOKEN_LIST.table == "superchain_token_list_v1"
    assert (
        EthereumOptimism.SUPERCHAIN_TOKEN_LIST.root_path
        == "ethereumoptimism/superchain_token_list_v1"
    )
