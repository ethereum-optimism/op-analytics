# OP Analytics
On-Chain Data, Utilities, References, and other Analytics on Optimism. Join the conversation with other numba nerds in the #analytics channel in the Optimism Discord.

## I'm Looking for Data About:
A select list of Optimism data dashboards:

### Network Usage
- [Optimism Protocol Metrics (i.e. transactions, fees, on-chain value)](https://dune.com/optimismfnd/Optimism)
- [Popular Apps on Optimism and Project Usage Trends](https://dune.com/optimismfnd/Optimism-Project-Usage-Trends)

### Token House & Citizen's House Governance
- [OP Token House Delegates](https://dune.com/optimismfnd/optimism-op-token-house)
- [Governance & Voting (by Flipside Crypto)](fscrypto.co/op-governance)
- [AttestationStation Usage](https://app.flipsidecrypto.com/dashboard/optimism-attestation-station-data-station-WAT27_)
- [AttestationStation Key and Creator Distributions](https://dune.com/oplabspbc/optimism-attestationstation)

### DeFi
- [Perpetuals Market (by rplust)](https://dune.com/rplust/Perpetuals-Trading-on-Optimism)
- [Total Value Locked in DeFi (by Defillama)](https://defillama.com/chain/Optimism)
- [App Fees & Revenue (by Defillama)](https://defillama.com/fees/chains/optimism)
- [DEX Trade Volume (by Defillama)](https://defillama.com/dexs/chains/optimism)

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


## Configs
For scripts which use APIs from providers with API keys, add the lines like below in a .env file (Replace with your API key - remember to add to gitignore):
```
DUNE_API_KEY = 'Your API Key'
FLIPSIDE_SHROOMDK_KEY = 'Your API Key'
```

## Common Requirements
Common python packages used include
- [pandas](https://github.com/pandas-dev/pandas)
- [requests](https://github.com/psf/requests)
- [aiohttp-retry](https://github.com/inyutin/aiohttp_retry)
- [dune-client](https://github.com/cowprotocol/dune-client)
- [subgrounds](https://github.com/0xPlaygrounds/subgrounds)
- [web3.py](https://github.com/ethereum/web3.py)
- [ethereum-etl](https://github.com/blockchain-etl/ethereum-etl)

## Installation
```
python -m pip install pipenv
pipenv install
```
See `Pipfile` for all the requirements

## Select Optimism Data Abstractions
**[Dune Spellbook](https://github.com/duneanalytics/spellbook/tree/main/models)**: *Tables can be used in [Dune Analytics](https://dune.com/browse/dashboards)*
- [`contracts_optimism.contract_mapping`](https://github.com/duneanalytics/spellbook/tree/main/models/contracts/optimism): Near exhaustive mappings of contracts to project names on Optimism - uses decoded contracts in Dune (`optimism.contracts`) and known deployer addresses to map contracts.
- [`dex.trades`](https://github.com/duneanalytics/spellbook/tree/main/models/dex): Aggregation of swaps across many decentralized exchanges
- [`nft.trades`](https://github.com/duneanalytics/spellbook/tree/main/models/nft): Aggregation of swaps across many NFT marketplaces
- [`perpetual.trades`](https://github.com/duneanalytics/spellbook/tree/main/models/perpetual): Aggregation of swaps across many perpetuals exchanges (by rplust)

**[Flipside Crypto - Optimism Models > Gold-Level Tables](https://github.com/FlipsideCrypto/optimism-models/tree/main/models/gold)**: *Tables can be used in [Flipside](https://flipsidecrypto.xyz/)*
- [`optimism.core.ez_dex_swaps`](https://github.com/FlipsideCrypto/optimism-models/tree/main/models/gold/dex): Aggregation of swaps across many decentralized exchanges
- [`optimism.core.ez_nft_sales`](https://github.com/FlipsideCrypto/optimism-models/blob/main/models/gold/core__ez_nft_sales.sql): Aggregation of swaps across many NFT marketplaces
- [`optimism.core.fact_delegations`](https://github.com/FlipsideCrypto/optimism-models/blob/main/models/gold/core__fact_delegations.sql): Aggregation of OP governance delegation events.
