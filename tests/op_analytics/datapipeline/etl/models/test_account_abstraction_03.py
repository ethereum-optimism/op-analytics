from op_analytics.coreutils.testutils.inputdata import InputTestData
from op_analytics.datapipeline.models.compute.testutils import ModelTestBase


class TestAccountAbstraction0003(ModelTestBase):
    """Edge Case: Worldchain encountered on 2025/04/03

    In this edgecase there is a subtrace that has a method_id that collides with the innerHandleOp method id.

    The typical "input" value for an innnerHandleOp looks like this:

    0x0042dc530000000000000000000000000000000000000000000000000000000000000200000000000000000000000000b
    9c4e7ec03c3dc1acff5a226e5dbc4d15c28dec800d1e523863dfab351eb200717e658790f76608800000000000000000000
    000c000000000000000000000000000000000000000000000000000000000000e1ef0000000000000000000000000000000
    000000000000000000000000000063c08000000000000000000000000000000000000000000000000000000000000767f00
    000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
    00000000000000000000007dbb7000000000000000000000000ef725aa22d43ea69fb22be2ebe6eca205a6bcf5b00000000
    0000000000000000000000000000000000000000000000000001e0aa0000000000000000000000000000000000000000000
    00000000000000001d8a878fcbcf8c896dfc7672921a0fc106547593180a397c7e2c23b3285cd6067958300000000000000
    00000000000000000000000000000000000000001cfc94dde20000000000000000000000000000000000000000000000000
    000000000003580000000000000000000000000000000000000000000000000000000000008847e00000000000000000000
    000000000000000000000000000000000000000004e00000000000000000000000000000000000000000000000000000000
    0000002a47bb3742800000000000000000000000038869bf66a61cf6bdb996a6ae40d5853fd43b526000000000000000000
    000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
    000000000800000000000000000000000000000000000000000000000000000000000000001000000000000000000000000
    00000000000000000000000000000000000001e48d80ff0a000000000000000000000000000000000000000000000000000
    0000000000020000000000000000000000000000000000000000000000000000000000000019900cd1e32b86953d79a6ac5
    8e813d2ea7a1790cab630000000000000000000000000000000000000000000000000000000000000000000000000000000
    0000000000000000000000000000000000000000000000144c22673871e15f1daf895cdbac76a314d31ca2d4fccbdba4a1e
    0935f57b3ca0c6b97e24bb263d19239098cefcb0d8d10829d2733ee75ee47ea33f689a866e512281b4e5be2b3b005bbce61
    b4f23ae35e6b3405b341a6f4f2676d71476bc9097e511ac328b2c3c0ad379fa3d5bfd62509e7c719f1b03641ad4d8ea75fc
    7047bb3fba0bcf1d01cca4d0f208266d6542632e281c2a8fc40d82a5a09323420d96b630896089b3216546a64bd9c761fd6
    bed250e075afe63e6cdbe2b479d81819058aed441ac79056abc8e11f012e01b793bfa4c055adf5379bd81c1e7d85fde11ed
    f119b17c420de9452c43e6dea7ec653a240043f8c6dc75a3ee50e33b25965635b7406bb2281889689f1e849652b1185d642
    15acc055274c4490ad7b0f7d7d6ebedcf5238d20fc9af540f8812d743bd1fbeab77dd2e9e118f59e6b05bb72756249c367c
    1ff900000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
    00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000

    The first parameter is of type "bytes" (the callData) and so we expected to see a uint256 value
    at the very start of the input right after the method id signature.

    The edge case data has a trace where "input" looks like this:

    0x0042dc5317a5211c3d583c7ee57a5c89e85b416cae31dda7b52b010c052e8aa713c698726ddf0497303828b00b0baa17c
    e9e56daddc64112bacb109047be45f40e3752c35539624b760e41cb776a8cc59c7cf58aa20d03bed4be43209815a0d42ae5
    75be1d7ed0e58064ecf2e58307c065d5def1da4f7a942c46eb9050457647

    This is CLEARLY WRONG. And it fails decoding. We try to decode this with the innerHandleOp ABI and
    the decoder gets thrown out of bounds because the length of the "bytes" first argument is really
    long (there are no zeros at first so as a uint256 it is a gigantic value).

    """

    model = "account_abstraction"
    inputdata = InputTestData.at(__file__)
    chains = ["worldchain"]
    target_range = 12158401
    block_filters = [
        "{block_number} IN (12158642) OR block_number % 100 < 10",
    ]

    _enable_fetching = True

    def test_overall_totals(self):
        assert self._duckdb_context is not None

        num_logs = (
            self._duckdb_context.client.sql(
                "SELECT COUNT(*) as num_logs FROM useroperationevent_logs_v2"
            )
            .pl()
            .to_dicts()[0]["num_logs"]
        )

        num_traces = (
            self._duckdb_context.client.sql(
                "SELECT COUNT(*) as num_traces FROM enriched_entrypoint_traces_v2"
            )
            .pl()
            .to_dicts()[0]["num_traces"]
        )

        assert num_logs == 921
        assert num_traces == 13587

    def test_log_counts(self):
        assert self._duckdb_context is not None

        output = (
            self._duckdb_context.client.sql("""
        SELECT transaction_hash, count(*) as num_logs FROM useroperationevent_logs_v2
        GROUP BY 1
        ORDER BY 1
        LIMIT 10
        """)
            .pl()
            .to_dicts()
        )

        assert output == [
            {
                "transaction_hash": "0x00e0bdf36731b2d969c571d0781094ca79f8a6a79401fdb3eca3389c66b5bd8e",
                "num_logs": 14,
            },
            {
                "transaction_hash": "0x026ffb6f87377443b19265172192fcd4e552bb2a34f00c4600dea3cc70f8337c",
                "num_logs": 15,
            },
            {
                "transaction_hash": "0x02da4a1e3faf341dee8628f48688ff7477d702d94846848193d63e9e6fd60e55",
                "num_logs": 7,
            },
            {
                "transaction_hash": "0x040ebaa164965d9ad4f8330848d8ad93a805aab31b6ba4923d27eb12dd8eda77",
                "num_logs": 15,
            },
            {
                "transaction_hash": "0x047139b55b614d46a58f07cbe9a9a58267a0cf322eb05444a4259592487393c3",
                "num_logs": 9,
            },
            {
                "transaction_hash": "0x067c8d2c631708e9e769a36c473f30aa60ae12b9dc468e28964a6097abb3c8d9",
                "num_logs": 10,
            },
            {
                "transaction_hash": "0x0b471987186337d175a999660e8d0403e35c99fa33b42874dd0ade16fc905b27",
                "num_logs": 6,
            },
            {
                "transaction_hash": "0x0d6edfab9ccbef4cbe93656a1532103cc0aa540058a36b05dbfa4e64aecb9992",
                "num_logs": 4,
            },
            {
                "transaction_hash": "0x1059b963b44b6147edd2ce8ffb1d10e733f3ca61e1a249da39bea6d59a6afef7",
                "num_logs": 4,
            },
            {
                "transaction_hash": "0x111e1209387bb3a173d05875c71988918d43b13168333ab18aeb89f5524a2f13",
                "num_logs": 8,
            },
        ]
