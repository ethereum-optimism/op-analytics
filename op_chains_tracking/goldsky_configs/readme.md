## Setting up a pipeline for a new chain
tl;dr: We set up a unique pipeline for each chain / network combination (i.e. fraxtal mainnet, base sepolia). We do this through custom yml files rather than the Goldsky UI so that we can configure multiple tables per pipeline (i.e. transactions, logs, traces, blocks) 

*Future TBD iteration: Do we also add pipelines for common subsets (i.e. ERC20/NFT/ETH transfers, creation traces), or do we do this at the modeling stage?*

1. Install and authenticate in to Goldsky ([Walkthrough](https://docs.goldsky.com/mirror/guides/get-started))

2. Make a copy of `template_default.yml` and name it for your pipeline (i.e. `fraxtal_mainnet.yml`) 

3. Find and replace in the template yml file:
- `chain-db-name`: Name of Goldsky database (i.e. `frax`)
- `chain-name`: Name we want to use for the chain, ideally 1:1 with the superchain registry (i.e. `fraxtal`)
- `network-name`: Network type, ideally 1:1 with the superchain registry (i.e. `mainnet`)
*We don’t include `chain_id` here, but could in future versions. The reason to not include it now, is to protect against any changes, and if we ingest before the chain is listed in [ethereum-lists](https://github.com/ethereum-lists/chains). We will have a reference table to join `chain_name & network` to `chain_id` and other fields.*

4. Execute the pipeline creation:
```
goldsky pipeline create <pipeline-name> --definition-path <path_to_yml/file_name>.yml
```

5. If successful, go to the goldksy app website returned and monitor the pipeline

6. Have fun

