from op_analytics.datapipeline.models.decode.abi_to_selector import (
    get_all_log_selectors,
    get_function_selector,
    get_log_selector,
)

from op_analytics.datapipeline.models.code.account_abstraction.abis import ABI_V0_6_0, ABI_V0_7_0


def test_abi_to_selector():
    assert get_function_selector(ABI_V0_6_0, "handleOps") == "0x1fad948c"


def test_abi_to_log_selector():
    assert (
        get_log_selector(ABI_V0_6_0, "UserOperationEvent")
        == "0x49628fd1471006c1482da88028e9ce4dbb080b815c9b0344d39e5a8e6ec1419f"
    )


def test_abi_log_detectors():
    actual_v6 = get_all_log_selectors(ABI_V0_6_0)
    assert actual_v6 == {
        "AccountDeployed": "0xd51a9c61267aa6196961883ecf5ff2da6619c37dac0fa92122513fb32c032d2d",
        "BeforeExecution": "0xbb47ee3e183a558b1a2ff0874b079f3fc5478b7454eacf2bfc5af2ff5878f972",
        "Deposited": "0x2da466a7b24304f47e87fa2e1e5a81b9831ce54fec19055ce277ca2f39ba42c4",
        "SignatureAggregatorChanged": "0x575ff3acadd5ab348fe1855e217e0f3678f8d767d7494c9f9fefbee2e17cca4d",
        "StakeLocked": "0xa5ae833d0bb1dcd632d98a8b70973e8516812898e19bf27b70071ebc8dc52c01",
        "StakeUnlocked": "0xfa9b3c14cc825c412c9ed81b3ba365a5b459439403f18829e572ed53a4180f0a",
        "StakeWithdrawn": "0xb7c918e0e249f999e965cafeb6c664271b3f4317d296461500e71da39f0cbda3",
        "UserOperationEvent": "0x49628fd1471006c1482da88028e9ce4dbb080b815c9b0344d39e5a8e6ec1419f",
        "UserOperationRevertReason": "0x1c4fada7374c0a9ee8841fc38afe82932dc0f8e69012e927f061a8bae611a201",
        "Withdrawn": "0xd1c19fbcd4551a5edfb66d43d2e337c04837afda3482b42bdf569a8fccdae5fb",
    }

    actual_v7 = get_all_log_selectors(ABI_V0_7_0)
    assert actual_v7 == {
        "AccountDeployed": "0xd51a9c61267aa6196961883ecf5ff2da6619c37dac0fa92122513fb32c032d2d",
        "BeforeExecution": "0xbb47ee3e183a558b1a2ff0874b079f3fc5478b7454eacf2bfc5af2ff5878f972",
        "Deposited": "0x2da466a7b24304f47e87fa2e1e5a81b9831ce54fec19055ce277ca2f39ba42c4",
        "PostOpRevertReason": "0xf62676f440ff169a3a9afdbf812e89e7f95975ee8e5c31214ffdef631c5f4792",
        "SignatureAggregatorChanged": "0x575ff3acadd5ab348fe1855e217e0f3678f8d767d7494c9f9fefbee2e17cca4d",
        "StakeLocked": "0xa5ae833d0bb1dcd632d98a8b70973e8516812898e19bf27b70071ebc8dc52c01",
        "StakeUnlocked": "0xfa9b3c14cc825c412c9ed81b3ba365a5b459439403f18829e572ed53a4180f0a",
        "StakeWithdrawn": "0xb7c918e0e249f999e965cafeb6c664271b3f4317d296461500e71da39f0cbda3",
        "UserOperationEvent": "0x49628fd1471006c1482da88028e9ce4dbb080b815c9b0344d39e5a8e6ec1419f",
        "UserOperationPrefundTooLow": "0x67b4fa9642f42120bf031f3051d1824b0fe25627945b27b8a6a65d5761d5482e",
        "UserOperationRevertReason": "0x1c4fada7374c0a9ee8841fc38afe82932dc0f8e69012e927f061a8bae611a201",
        "Withdrawn": "0xd1c19fbcd4551a5edfb66d43d2e337c04837afda3482b42bdf569a8fccdae5fb",
    }

    pairs_v6 = set((k, v) for k, v in actual_v6.items())
    pairs_v7 = set((k, v) for k, v in actual_v7.items())

    only_v6 = sorted(pairs_v6 - pairs_v7)
    assert only_v6 == []

    only_v7 = sorted(pairs_v7 - pairs_v6)
    assert only_v7 == [
        (
            "PostOpRevertReason",
            "0xf62676f440ff169a3a9afdbf812e89e7f95975ee8e5c31214ffdef631c5f4792",
        ),
        (
            "UserOperationPrefundTooLow",
            "0x67b4fa9642f42120bf031f3051d1824b0fe25627945b27b8a6a65d5761d5482e",
        ),
    ]
