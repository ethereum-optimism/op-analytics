### Analaysis
**Purpose:** To recommend Fee Scalar updates for the Fjord hard fork. In Ecotone, compressed transaction size was estimated by dividing the l1GasUsed for a transaction by 16. In Fjord, the [RLP-encoded](https://ethereum.org/en/developers/docs/data-structures-and-encoding/rlp/) transactions are sent through the [FastLZ](https://github.com/ariya/FastLZ) compression algorithm to generate an `estimatedSize` for the transaction, which becomes how users are charged for L1/Blob gas.

**Todos:**
1. Understand the average estimatedSize per transaction for the higher volume chains
  - Pull transactions from Goldsky/Clickhouse by chain -> RLP-Econde transaction input data (count bytes) -> run through FastLZ (count bytes)
2. Approximate accuracy by comparing total estimatedSize on L2 vs blobgas used on L1 (and gas if calldata for chains that may use calldata)
  - This should be a bit of an underestimate, since we'll have brotli compression in Fjord. We ~could handle for this by assuming some extra compression? But v1 likely should not mess with this.

- (1) and (2) are inputs to the Fee Scalar Recommendation logic :squidward_dab:

### L1 Fee Cost (from Fjord Specs)
```
l1FeeScaled = baseFeeScalar*l1BaseFee*16 + blobFeeScalar*l1BlobBaseFee
estimatedSize = max(minTransactionSize, intercept + fastlzCoef*fastlzSize)
l1Cost = estimatedSize * l1FeeScaled / 1e12
```

### References
- [Ecotone Scalar Recommendation Sheet](https://docs.optimism.io/builders/chain-operators/management/blobs#determine-scalar-values-for-using-blobs)

- [Fjord Specs](https://github.com/ethereum-optimism/specs/blob/main/specs/fjord/exec-engine.md#fjord-l1-cost-fee-changes-fastlz-estimator)

- [New Fjord Data Fee Cost Func](https://github.com/ethereum-optimism/op-geth/blob/966c43537e49f7936bb57a426079fb0da9baf03b/core/types/rollup_cost.go#L356)
    - [FastLz Fjord Implementation](https://github.com/ethereum-optimism/op-geth/blob/966c43537e49f7936bb57a426079fb0da9baf03b/core/types/rollup_cost.go#L399)

- [Legacy Ecotone Data Fee Cost Func](https://github.com/ethereum-optimism/op-geth/blob/966c43537e49f7936bb57a426079fb0da9baf03b/core/types/rollup_cost.go#L214)

- [FastLZ Repo](https://github.com/ariya/FastLZ)

- [RLP-encoding python package](https://pypi.org/project/rlp/)