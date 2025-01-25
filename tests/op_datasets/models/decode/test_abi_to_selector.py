from op_analytics.datapipeline.models.decode.abi_to_selector import (
    get_function_selector,
    get_log_selector,
)

from op_analytics.datapipeline.models.code.account_abstraction.abis import ABI_V0_6_0


def test_abi_to_selector():
    assert get_function_selector(ABI_V0_6_0, "handleOps") == "0x1fad948c"


def test_abi_to_log_selector():
    assert (
        get_log_selector(ABI_V0_6_0, "UserOperationEvent")
        == "0x49628fd1471006c1482da88028e9ce4dbb080b815c9b0344d39e5a8e6ec1419f"
    )
