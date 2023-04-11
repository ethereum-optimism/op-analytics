# OP Analytics
Onchain Data, Utilities, References, and other Analytics on Optimism. Join the conversation with other numba nerds in the #analytics channel in the Optimism Discord.

## Table of Contents
* [I'm looking for Data About](#im-looking-for-data-about)
* [Select Optimism Data Abstractions](#select-optimism-data-abstractions)
* [Contributors](#contributors)

---

## I'm Looking for Data About:
A select list of Optimism data dashboards:

### Network Usage
- [Optimism Protocol Metrics (i.e. transactions, fees, onchain value)](https://dune.com/optimismfnd/Optimism)
- [Popular Apps on Optimism and Project Usage Trends](https://dune.com/optimismfnd/Optimism-Project-Usage-Trends)

### Token House & Citizen's House Governance
- [OP Token House Delegates](https://dune.com/optimismfnd/optimism-op-token-house)
- [Governance & Voting (by Flipside Crypto)](fscrypto.co/op-governance)
- [AttestationStation Usage](https://app.flipsidecrypto.com/dashboard/optimism-attestation-station-data-station-WAT27_)
- [AttestationStation Key and Creator Distributions](https://dune.com/oplabspbc/optimism-attestationstation)

### OP Token Distributions & Growth Programs Tracking
- [Incentive Program Onchain Usage Summary Dashboard](https://dune.com/oplabspbc/optimism-incentive-program-usage-summary)
- [Time-Series of TVL Flows by Program](https://static.optimism.io/op-analytics/op_rewards_tracking/img_outputs/overall/cumul_ndf_last_price.html) (Sourced from DefiLlama and TheGraph API)
 - [Program-Specific TVL Flows Charts](https://github.com/ethereum-optimism/op-analytics/tree/main/op_rewards_tracking/img_outputs/app/last_price/svg)
*See Distributions mapping resources in [Select Optimism Data Abstractions](#select-optimism-data-abstractions)*

### DeFi
- [Perpetuals Market (by rplust)](https://dune.com/rplust/Perpetuals-Trading-on-Optimism)
- [Total Value Locked in DeFi (by Defillama)](https://defillama.com/chain/Optimism)
- [App Fees & Revenue (by Defillama)](https://defillama.com/fees/chains/optimism)
- [DEX Trade Volume (by Defillama)](https://defillama.com/dexs/chains/optimism)
- TVL Flows Between Apps and Chains: [Last 7 Days](https://static.optimism.io/op-analytics/value_locked_flows/img_outputs/html/net_app_flows_7d.html), [30 Days](https://static.optimism.io/op-analytics/value_locked_flows/img_outputs/html/net_app_flows_30d.html), [90 Days](https://static.optimism.io/op-analytics/value_locked_flows/img_outputs/html/net_app_flows_90d.html), [180 Days](https://static.optimism.io/op-analytics/value_locked_flows/img_outputs/html/net_app_flows_180d.html), [365 Days](https://static.optimism.io/op-analytics/value_locked_flows/img_outputs/html/net_app_flows_365d.html) (From Defillama API)

### NFTs
- [NFT Marketplaces and Collection Volume](https://dune.com/oplabspbc/optimism-nft-secondary-marketplaces)
- [L1<>L2 NFT Bridge](https://dune.com/chuxin/optimism-nft-bridge?L1+NFT+Contract+Address_t4e85b=0x5180db8f5c931aae63c74266b211f580155ecac8)

### Protocol Economics
- [L2 Transaction Fees, L1 Security Costs, Sequencer Fees](https://dune.com/optimismfnd/optimism-l1-batch-submission-fees-security-costs)
- [Protocol Revenue (by TokenTerminal)](https://tokenterminal.com/terminal/projects/optimism/revenue-share)

### Transaction Costs
- [Transaction Fee Savings Calculator](https://dune.com/optimismfnd/How-Much-Could-You-Save-on-Optimism-Fee-Savings-Calculator)

### User Onboarding
- [CEX & On/Off-Ramp Usage](https://dune.com/oplabspbc/optimism-onoff-ramp-usage)
- [App Growth on Optimism After Quests](https://dune.com/oplabspbc/optimism-quests-project-usage-growth)

## Select Optimism Data Abstractions
**[Dune Spellbook](https://github.com/duneanalytics/spellbook/tree/main/models)**: *Tables can be used in [Dune Analytics](https://dune.com/browse/dashboards)*
- [`contracts_optimism.contract_mapping`](https://github.com/duneanalytics/spellbook/tree/main/models/contracts/optimism): Near exhaustive mappings of contracts to project names on Optimism - uses decoded contracts in Dune (`optimism.contracts`) and known deployer addresses to map contracts.
- [`op_token_distributions_optimism.transfer_mapping`](https://github.com/duneanalytics/spellbook/tree/main/models/op/token_distributions/optimism): Mappings of token distributions from the OP Foundation & by Grant/Growth Experiment recipients. You can use this table to count h0w much OP has been deployed, by who, and to where. *Note: These are "best guess" mappings* (contirbute address mappings in the [Dune Spellbook repo](https://github.com/duneanalytics/spellbook/tree/main/models/op/token_distributions/optimism)).
- [`dex.trades`](https://github.com/duneanalytics/spellbook/tree/main/models/dex): Aggregation of swaps across many decentralized exchanges
- [`nft.trades`](https://github.com/duneanalytics/spellbook/tree/main/models/nft): Aggregation of swaps across many NFT marketplaces. Also see [`nft.wash_trades`](https://github.com/duneanalytics/spellbook/blob/main/models/nft/optimism/nft_optimism_wash_trades.sql) by hildobby for filtering out likely wash trades.
- [`perpetual.trades`](https://github.com/duneanalytics/spellbook/tree/main/models/perpetual): Aggregation of swaps across many perpetuals exchanges (by rplust)

**[Flipside Crypto - Optimism Models > Gold-Level Tables](https://github.com/FlipsideCrypto/optimism-models/tree/main/models/gold)**: *Tables can be used in [Flipside](https://flipsidecrypto.xyz/)*
- [`optimism.core.ez_dex_swaps`](https://github.com/FlipsideCrypto/optimism-models/tree/main/models/gold/dex): Aggregation of swaps across many decentralized exchanges
- [`optimism.core.ez_nft_sales`](https://github.com/FlipsideCrypto/optimism-models/blob/main/models/gold/core__ez_nft_sales.sql): Aggregation of swaps across many NFT marketplaces
- [`optimism.core.fact_delegations`](https://github.com/FlipsideCrypto/optimism-models/blob/main/models/gold/core__fact_delegations.sql): Aggregation of OP governance delegation events.

## Contributors
### Configs
For scripts which use APIs from providers with API keys, add the lines like below in a .env file (Replace with your API key - remember to add to gitignore):
```
DUNE_API_KEY = 'Your API Key'
FLIPSIDE_SHROOMDK_KEY = 'Your API Key'
```

### Installation
```
python -m pip install pipenv
pipenv install
```
See `Pipfile` for all the requirements.

### Common Requirements
Common packages used for python scripts include
- [pandas](https://github.com/pandas-dev/pandas)
- [requests](https://github.com/psf/requests)
- [aiohttp-retry](https://github.com/inyutin/aiohttp_retry)
- [dune-client](https://github.com/cowprotocol/dune-client)
- [subgrounds](https://github.com/0xPlaygrounds/subgrounds)
- [web3.py](https://github.com/ethereum/web3.py)
- [ethereum-etl](https://github.com/blockchain-etl/ethereum-etl)

In this repository, we use `pre-commit` to ensure consistency of formatting. To install for Mac, run
```
brew install pre-commit
```
Once installed, in the command line of the repository, run
```
pre-commit install
```
This will install `pre-commit` to the Git hook, so that `pre-commit` will run and fix files covered in its config before committing.
