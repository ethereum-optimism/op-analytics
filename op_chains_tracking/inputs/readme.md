## *Note: This is NOT an "offical" registry.*

#### Chain Metadata
This file aims to capture all OP Chains & OP Stack Forks. See [Key Definitions for Chain Segments](https://community.optimism.io/docs/contribute/important-terms/#the-superchain)


Please PR to fix any inaccuracies! *Add ideas to this [GitHub Issue](https://github.com/ethereum-optimism/op-analytics/issues/249).*

- chain_name: superchain-registry file name (if listed), or an otherwise arbitrary identifier.

#### OP Collective Revenue Share Configuration
- Identify chains using their chain name in the Chain Metadata file.
  - We could use chain id as the primary, but this is harder to read. All chains we include here will be in Chain Metadata, so we can join on chain name.
  - `ethereum` and other da layers can also be mentioned if custom query logic for them exists.
- `op_collective` is the recipient address, all other entities are sender addresses.
- if `transfer_on_chain_name` is null, then this address applies to all chains int he superchain (and ethereum).

---

### How can I make Updates?

Fork the repo, make your changes, then submit a PR and tag @msilb7. Adding missing chains, filling in "unknowns", and adding mainnet RPCs for chains is most valuable right now.

### Info Sources:
- [OP Superchain Registry](https://github.com/ethereum-optimism/superchain-registry)
- [L2Beat List](https://l2beat.com/scaling/summary)
- [Conduit Optimism Repo](https://github.com/conduitxyz/optimism/tree/develop)
- [Caldera Blog](https://blog.caldera.xyz/)
- [superchain.eco](https://www.superchain.eco/ecosystem/chains)
- [Monorepo - Deploy Configs](https://github.com/ethereum-optimism/optimism/tree/develop/packages/contracts-bedrock/deploy-config)


### Updates Spec:
- Re-configure as a yml, or some format that's easier to edit and handle for "flexibility" (i.e. unique attributes & config changes)
- Core unit is ecosystem, versus chain (i.e. Base Ecosystem, Zora Ecosystem)
- Allow for multiple chains within one ecosystem (i.e. multiple mainnets, multiple testnets)

### Current Use:
- [l2_revenue_tracking.ipynb](https://github.com/ethereum-optimism/op-analytics/blob/main/op_chains_tracking/l2_revenue_tracking.ipynb) - hourly pulls of fee vaults pushed to `dune.oplabspbc.dataset_op_stack_chains_cumulative_revenue_snapshots` in Dune.
