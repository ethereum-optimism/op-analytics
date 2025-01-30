from op_analytics.datapipeline.models.code.account_abstraction.event_decoders import (
    user_op_event_decoder,
)

# https://basescan.org/tx/0xa6afb687ed95e708b6086b8fd864cd56bd46746c9850e943a035c4863f88fbed
log_data_01 = "0x000000000000000000000000000000000000000000000000000000000000003c000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000167da8e59350000000000000000000000000000000000000000000000000000000000052539"

# Manually constructed:
log_data_02 = "0x000000000000000000000000000000000000000000000000000000000000003c000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000167da8e5935f000000000000000000000000000000000000000000000000000000000052539"


def test_inner_handle_op():
    decoder = user_op_event_decoder()

    # The same decoder can handle both v6 and v7.
    actual = [
        decoder.decode(log_data_01),
        decoder.decode(log_data_02),
    ]
    assert actual == [
        {
            "nonce": 60,
            "nonce_lossless": "60",
            "success": True,
            "actual_gas_cost": 1545560021301,
            "actual_gas_cost_lossless": "1545560021301",
            "actual_gas_used": 337209,
            "actual_gas_used_lossless": "337209",
        },
        {
            "nonce": 60,
            "nonce_lossless": "60",
            "success": True,
            "actual_gas_cost": 1545560021301,
            "actual_gas_cost_lossless": "1545560021301",
            "actual_gas_used": None,
            "actual_gas_used_lossless": "108555083659983933209597798445644913612440610624038028786991485007418559374649",
        },
    ]
