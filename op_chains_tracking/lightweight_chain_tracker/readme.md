# Lightweight OP Stack Aggregate Data

See [Cryo Python Example](https://docs.chainstack.com/docs/cryo-with-chainstack-and-python#basic-usage-of-cryo_python)
- [Polars EVM](https://github.com/sslivkoff/polars_evm)

## Problem
There are (and likely will be) a countless amount of OP Stack Chains, and traditional methods of data analysis; like storing all transactions, traces, logs, etc in a database; may not be feasible. So, we're exploring "non-database" methods of querying blockchain data, starting with basic aggregate stats to answer the question "how much activity is this chain producing?" A subset of the highest volume chains may then be worth spinning up comprehensive databases for.

Our goal is to generate metrics comparable to what appears on [this OP Chains dashboard](https://dune.com/oplabspbc/op-stack-chains-l1-activity)

## Fields

#### General
- **User Transactions**: Count of transactions where `gas_price`` is > 0 (filter out system transactions and deposits)
- **Successful User Transactions**: Count of `User Transactions` with a successful status.
- **System Transactions**: Count of transactions where `gas_price`` = 0, and the `to_address` = `0x4200000000000000000000000000000000000015`
- **Deposit Transactions**: `User Transactions per Day` - `System Transactions per Day` (for simplicity)
- **Blacks**: Count of blocks, used to understand how much of the day was active (i.e. day 1 and day n may be partial days).
- **Avg Block Time**: Average time between blocks in seconds (standard config for OP Stack is 2s).

### L2 Execution Gas (in wei units)
- **L2 Gas Used by User Transactions**: `gas_used` in `User Transactions` (also per block)
- **L2 Gas Used by System Transactions**: `gas_used` in `System Transactions`
- **L2 Gas Used by Deposit Transactions**: `gas_used` in `Deposit Transactions`

- **L2 Base Fees Paid by User Transactions**: `gas_used`*`base_fee_per_gas` in `User Transactions`
- **L2 Priority Fees Paid by User Transactions**: `gas_used`*(`gas_price`-`base_fee_per_gas`) in `User Transactions`

### L1 Data Gas (in wei units)
- **L1 Gas Used on L2**: `receipt_l1_gas_used`
- **Calldata Gas Used on L2**: calldata gas of `input`
- **L1 Gas Paid on L2**: `receipt_l1_gas_used`*`receipt_l1_fee_scalar`


### Gas Prices (in gwei units)
- **L1 Gas Price:** Average `receipt_l1_gas_price` weighted by `L1 Gas Used`
- **L2 Gas Price:** Average `gas_price` weighted by `L2 Gas Used`
- **L2 Base Fee:** Average `base_fee_per_gas` weighted by `L2 Gas Used`
- **L2 Base Fee:** Average (`gas_price`-`base_fee_per_gas`) weighted by `L2 Gas Used`

### Fee Revenue (in ETH units)
- **L2 Fees - L1 Data Fee:** `l1_fee`
- **L2 Fees - L2 Base Fee:** `gas_used` * `base_fee_per_gas`
- **L2 Fees - L2 Priority Fee:** `gas_used` * (`gas_price`-`base_fee_per_gas`)
- **L2 Fees - Total:** `l1_fee` + (`gas_used`*`gas_price`)

### Later Versions
- Aggregate Contract Usage Table: # Transactions, Traces, Logs, Gas Used by contract x day
- Actively Deploying Developers: Unique Deployer Addresses (given spam/noise filters)
- All Contract Creation Traces + Script to rebuild a version of our [Contract Mapping model in Dune Spellbook](https://github.com/duneanalytics/spellbook/tree/main/models/_sector/contracts/optimism)