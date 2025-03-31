"""Block Batching Utilities

We split ingestion into batches to meet the following goals:

  - Incremental ingestion of raw onchain data by block number.
  - Integration with BigQuery using a hive partitioned BigQuery external table.

Given a range of block numbers we want to have a deterministic way of locating where the
data for those blocks is stored in GCS.
"""

from dataclasses import dataclass

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.output import ExpectedOutput
from op_analytics.coreutils.rangeutils.blockrange import BlockRange
from op_analytics.datapipeline.chains.goldsky_chains import ChainNetwork, determine_network

log = structlog.get_logger()


# For each chain we configure the block batch size that should be used. The batch size
# can change over time for a chain, so the configuration stores the block numbers at
# which the batch size changes. We refer to these block numbers as "delimiters".
#
# At the start of the chain or for some smaller chains we can use very large batches.
# For chains that grow in usage we have to use progressively smaller batches.


@dataclass
class Delimiter:
    block_number: int
    batch_size: int

    @property
    def next_block(self):
        return self.block_number + self.batch_size


# Helful chart to see what trace file sizes by chain:
# https://optimistic.grafana.net/explore?schemaVersion=1&panes=%7B%22dar%22:%7B%22datasource%22:%22grafanacloud-logs%22,%22queries%22:%5B%7B%22refId%22:%22A%22,%22expr%22:%22max_over_time%28%7Bcluster%3D%5C%22oplabs-tools-data-primary%5C%22,%20namespace%3D%5C%22op-analytics%5C%22,%20container%3D%5C%22python-runner-ingestion%5C%22%7D%20%7C%3D%20%60done%20writing%60%20or%20%60traces%60%20%7C%3D%20%60size%60%20%7C%20json%20chain,%20size%20%7C%20unwrap%20size%20%5B1h%5D%29%20by%20%28chain%29%22,%22queryType%22:%22range%22,%22datasource%22:%7B%22type%22:%22loki%22,%22uid%22:%22grafanacloud-logs%22%7D,%22editorMode%22:%22builder%22,%22direction%22:%22backward%22%7D%5D,%22range%22:%7B%22from%22:%22now-2d%22,%22to%22:%22now%22%7D,%22panelsState%22:%7B%22logs%22:%7B%22visualisationType%22:%22logs%22%7D%7D%7D%7D&orgId=1
MICROBATCH_SIZE_CONFIGURATION = {
    "automata": [
        # https://explorer.ata.network/block/11008000
        Delimiter(0, 20000),
        Delimiter(block_number=5440000, batch_size=8000),
        Delimiter(block_number=11008000, batch_size=4000),
    ],
    "bob": [
        # https://explorer.gobob.xyz/block/15168000
        Delimiter(0, 20000),
        Delimiter(block_number=9600000, batch_size=8000),
        Delimiter(block_number=15168000, batch_size=4000),
    ],
    "base": [
        # https://basescan.org/
        Delimiter(block_number=0, batch_size=10000),
        Delimiter(block_number=10000000, batch_size=5000),
        Delimiter(block_number=15000000, batch_size=2000),
        # Starting here base is ~2.5M traces / 1k blocks.
        # Traces file can be ~70MB.
        Delimiter(block_number=20900000, batch_size=1000),
        # (2024/10/29) Decided to go to 400 blocks per batch to give us more room.
        # Traces file size is ~25MB.
        Delimiter(block_number=21200000, batch_size=400),
        # (2025/03/17) Go to 200 blocks per batch. Trace file size is at ~55MB.
        Delimiter(block_number=27730000, batch_size=200),
    ],
    "cyber": [
        # https://cyberscan.co/block/1488000
        Delimiter(0, 20000),
        Delimiter(block_number=9280000, batch_size=8000),
        Delimiter(block_number=1488000, batch_size=4000),
    ],
    "fraxtal": [
        # https://fraxscan.com/block/countdown/18200000
        Delimiter(0, 20000),
        Delimiter(block_number=12600000, batch_size=8000),
        Delimiter(block_number=18200000, batch_size=4000),
    ],
    "ham": [
        # https://explorer.ham.fun/block/26590870
        Delimiter(block_number=0, batch_size=20000),
        # Reducing due to large memory usage fetching traces.
        Delimiter(block_number=15160000, batch_size=8000),
    ],
    "ink": [
        # https://explorer.inkonchain.com/block/9704000
        Delimiter(block_number=0, batch_size=8000),
        Delimiter(block_number=9696000, batch_size=4000),
    ],
    "kroma": [
        # https://blockscout.kroma.network/block/24664000
        Delimiter(0, 20000),
        Delimiter(block_number=19080000, batch_size=8000),
        Delimiter(block_number=24664000, batch_size=4000),
    ],
    "lisk": [
        Delimiter(0, 20000),
        Delimiter(block_number=8640000, batch_size=8000),
        # Decreased on 2025/02/04
        Delimiter(block_number=12032000, batch_size=4000),
    ],
    "lyra": [
        Delimiter(0, 20000),
        Delimiter(block_number=16000000, batch_size=8000),
        # Decreased on 2025/01/10.
        Delimiter(block_number=18232000, batch_size=4000),
        # Decreased on 2025/01/21.
        Delimiter(block_number=18720000, batch_size=3000),
        # Decreased on 2025/03/17.
        Delimiter(block_number=21114000, batch_size=1000),
    ],
    "metal": [
        # https://explorer.metall2.com/block/15824000
        Delimiter(0, 20000),
        Delimiter(block_number=10240000, batch_size=8000),
        Delimiter(block_number=15824000, batch_size=4000),
    ],
    "mint": [
        # https://explorer.mintchain.io/block/13800000
        Delimiter(0, 20000),
        Delimiter(block_number=8200000, batch_size=8000),
        Delimiter(block_number=13800000, batch_size=4000),
    ],
    "mode": [
        Delimiter(0, 20000),
        Delimiter(block_number=15920000, batch_size=8000),
        # Decreased on 2024/12/10.
        Delimiter(block_number=16832000, batch_size=2000),
    ],
    "op": [
        Delimiter(block_number=0, batch_size=10000),
        Delimiter(block_number=62000000, batch_size=5000),
        Delimiter(block_number=94000000, batch_size=2000),
        # Decreased on 2025/01/21.
        Delimiter(block_number=130928000, batch_size=1000),
        # Decreased on 2025/03/17.
        Delimiter(block_number=133324000, batch_size=400),
    ],
    "orderly": [
        # At various times during 2024 orderly had a sustained peak
        # of ~130k transactions per second.
        Delimiter(block_number=0, batch_size=5000),
    ],
    "polynomial": [
        # https://polynomialscan.io/block/12592000
        Delimiter(0, 20000),
        Delimiter(block_number=7000000, batch_size=8000),
        Delimiter(block_number=12592000, batch_size=4000),
    ],
    "race": [
        # https://racescan.io/block/11400000
        Delimiter(0, 20000),
        Delimiter(block_number=5800000, batch_size=8000),
        Delimiter(block_number=11400000, batch_size=4000),
    ],
    "redstone": [
        Delimiter(block_number=0, batch_size=20000),
        # Reduce memory use for Redstone batches.
        Delimiter(block_number=9820000, batch_size=10000),
        # More memory reduction. Saw one batch with many logs got stuck due to memory issues.
        Delimiter(block_number=9920000, batch_size=4000),
    ],
    "shape": [
        # https://shapescan.xyz/block/countdown/10736000
        Delimiter(0, 20000),
        Delimiter(block_number=5160000, batch_size=8000),
        Delimiter(block_number=10736000, batch_size=4000),
    ],
    "soneium": [
        Delimiter(0, 8000),
        # Decreased on 2025/02/22
        Delimiter(block_number=3584000, batch_size=4000),
        # Decreased on 2025/
        Delimiter(block_number=4560000, batch_size=1000),
    ],
    "swan": [
        Delimiter(block_number=0, batch_size=20000),
        # Reduced on 2024/11/16 to decrease pipeline latency. Swan has less blocks per second so
        # 20k blocks means more hours.
        Delimiter(block_number=2640000, batch_size=8000),
        # Reduced on 2025/03/17 to further decrease pipeline latency.
        Delimiter(block_number=4728000, batch_size=2000),
    ],
    "swell": [
        # https://explorer.swellnetwork.io/block/5256000
        Delimiter(0, 8000),
        Delimiter(block_number=5256000, batch_size=4000),
    ],
    "unichain": [
        Delimiter(0, 8000),
        # Decreased on 2025/02/22
        Delimiter(block_number=13440000, batch_size=4000),
    ],
    "worldchain": [
        Delimiter(block_number=0, batch_size=20000),
        # On 2024/10/13 (after launching) worldchain started having
        # sustained transaction count of ~600k/day.
        Delimiter(block_number=4700000, batch_size=4000),
        # Decreased on 2024/12/03.
        Delimiter(block_number=6948000, batch_size=2000),
        # Decreased on 2024/12/09.
        Delimiter(block_number=7216000, batch_size=800),
        # Decreased on 2025/03/17.
        Delimiter(block_number=11458400, batch_size=400),
    ],
    "xterio": [
        # https://eth.xterscan.io/block/13336000
        Delimiter(0, 20000),
        Delimiter(block_number=7760000, batch_size=8000),
        Delimiter(block_number=13336000, batch_size=4000),
    ],
    "zora": [
        # https://explorer.zora.energy/block/28256000
        Delimiter(0, 20000),
        Delimiter(block_number=22680000, batch_size=8000),
        Delimiter(block_number=28256000, batch_size=4000),
    ],
    # Testnets
    "ink_sepolia": [Delimiter(0, 5000)],
    "op_sepolia": [
        Delimiter(0, 5000),
        # Decreased on 2024/12/03.
        Delimiter(block_number=20710000, batch_size=2000),
        # Decreased on 2025/03/17.
        Delimiter(block_number=25222000, batch_size=1000),
    ],
    "unichain_sepolia": [Delimiter(0, 5000)],
}


class InvalidMicrobatchConfig(Exception):
    pass


def validate_microbatch_configuration(boundaries: list[Delimiter]):
    for i, boundary in enumerate(boundaries):
        if i == 0:
            if boundary.block_number != 0:
                raise InvalidMicrobatchConfig(
                    f"the first delimiter should always start at block 0: {boundary}"
                )

        else:
            prev_boundary = boundaries[i - 1]
            distance = boundary.block_number - prev_boundary.block_number
            if distance % prev_boundary.batch_size != 0:
                raise InvalidMicrobatchConfig(
                    f"delimiter block number should align: {prev_boundary} -> {boundary}"
                )


for config in MICROBATCH_SIZE_CONFIGURATION.values():
    validate_microbatch_configuration(config)


def find_batch_delimiter(boundaries: list[Delimiter], block_number: int) -> Delimiter:
    padded_start_boundaries = [boundaries[0]] + boundaries
    padded_end_boundaries = boundaries + [boundaries[-1]]

    for prev_delimiter, delimiter in zip(padded_start_boundaries, padded_end_boundaries):
        assert delimiter.block_number % delimiter.batch_size == 0

        if delimiter.block_number <= block_number:
            continue
        else:
            break

    return Delimiter(
        block_number=block_number - (block_number % prev_delimiter.batch_size),
        batch_size=prev_delimiter.batch_size,
    )


@dataclass
class BlockBatch:
    """Represents a single processing batch.

    BlockBatch is structurally the same as a BlockRange, but the BlockBatch is
    constructed to match the block bach boundaries configured for each chain."""

    chain: str
    min: int  # inclusive
    max: int  # exclusive

    @property
    def contextvars(self):
        return dict(
            chain=self.chain,
            blocks=f"#{self.min}-{self.max}",
        )

    def __len__(self):
        return self.max - self.min

    def num_blocks(self):
        return self.max - self.min

    def filter(self, number_column: str = "number"):
        return f" {number_column} >= {self.min} and {number_column} < {self.max}"

    def construct_expected_output(self, root_path: str):
        padded_min_block = f"{self.min:012d}"
        marker = f"{root_path}/{self.chain}/{padded_min_block}"
        parquet_name = f"{padded_min_block}.parquet"

        return ExpectedOutput(
            root_path=root_path,
            file_name=parquet_name,
            marker_path=marker,
        )

    @property
    def network(self):
        return determine_network(self.chain)

    @property
    def is_testnet(self):
        return self.network == ChainNetwork.TESTNET

    def storage_directory(self):
        network = self.network

        if network == ChainNetwork.MAINNET:
            return "ingestion"
        if network == ChainNetwork.TESTNET:
            return "ingestion_testnets"
        raise NotImplementedError(f"invalid network: {network}")

    def dataset_directory(self, dataset_name: str) -> str:
        prefix = self.storage_directory()

        if dataset_name == "blocks":
            return f"{prefix}/blocks_v1"
        elif dataset_name == "logs":
            return f"{prefix}/logs_v1"
        elif dataset_name == "transactions":
            return f"{prefix}/transactions_v1"
        elif dataset_name == "traces":
            return f"{prefix}/traces_v1"
        else:
            raise NotImplementedError(f"invalid dataset: {dataset_name}")


def split_block_range(chain: str, block_range: BlockRange) -> list[BlockBatch]:
    boundaries = MICROBATCH_SIZE_CONFIGURATION[chain]
    return split_block_range_from_boundaries(chain, boundaries, block_range)


def split_block_range_from_boundaries(
    chain: str,
    boundaries: list[Delimiter],
    block_range: BlockRange,
) -> list[BlockBatch]:
    validate_microbatch_configuration(boundaries)

    delimiter = find_batch_delimiter(boundaries, block_range.min)

    batches: list[BlockBatch] = []
    while delimiter.block_number < block_range.max:
        new_delimiter = find_batch_delimiter(boundaries, delimiter.next_block)
        if delimiter.next_block != new_delimiter.block_number:
            raise ValueError(f"inconsisent batch: {delimiter}  ..  {new_delimiter}")
        batches.append(BlockBatch(chain, delimiter.block_number, delimiter.next_block))
        delimiter = new_delimiter
    return batches
