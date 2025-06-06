import json

from op_analytics.datapipeline.models.code.account_abstraction.decoders import user_op_event_decoder

# https://basescan.org/tx/0xa6afb687ed95e708b6086b8fd864cd56bd46746c9850e943a035c4863f88fbed
log_data_01 = "0x000000000000000000000000000000000000000000000000000000000000003c000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000167da8e59350000000000000000000000000000000000000000000000000000000000052539"

# Manually constructed:
log_data_02 = "0x000000000000000000000000000000000000000000000000000000000000003c000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000167da8e5935f000000000000000000000000000000000000000000000000000000000052539"


def test_inner_handle_op():
    decoder = user_op_event_decoder()
    decoder.as_json = True

    # The same decoder can handle both v6 and v7.
    actual = [
        decoder.decode(log_data_01),
        decoder.decode(log_data_02),
    ]

    actual = [json.loads(_) for _ in actual]

    assert actual == [
        {
            "nonce": "60",
            "success": True,
            "actualGasCost": "1545560021301",
            "actualGasUsed": "337209",
        },
        {
            "nonce": "60",
            "success": True,
            "actualGasCost": "1545560021301",
            "actualGasUsed": "108555083659983933209597798445644913612440610624038028786991485007418559374649",
        },
    ]


def test_inner_handle_op_as_dict():
    decoder = user_op_event_decoder()

    # The same decoder can handle both v6 and v7.
    actual = [
        decoder.decode(log_data_01),
        decoder.decode(log_data_02),
    ]

    assert actual == [
        {
            "nonce": "60",
            "success": True,
            "actualGasCost": "1545560021301",
            "actualGasUsed": "337209",
        },
        {
            "nonce": "60",
            "success": True,
            "actualGasCost": "1545560021301",
            "actualGasUsed": "108555083659983933209597798445644913612440610624038028786991485007418559374649",
        },
    ]
