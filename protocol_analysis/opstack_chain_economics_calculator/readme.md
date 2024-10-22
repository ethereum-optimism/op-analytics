# OP Stack Chain Economics Calculator
*Note: Initializing this in the op-analytics repo, we may eventually pop this out in to its own repo for easier management (i.e. PRs, issues, link sharing, other contributions)*

**Current State:** Working on skeletal structure, figuring out the best way to handle for simulated transaction flow on L2 (i.e. burst for 2 hours, empty for 2 hours - may save this for a V2 though)

## Goals:
1. A potential chain partner and operator can input facts / projections about their chain & output an estimation of total L2 Fees, total DA costs, and net onchain margin.
2. Protocol teams can "simulate" various upgrades & cost-saving techniques by modifying the calculator and/or inputs (i.e. single batches to span batches, Calldata DA to Blob DA).
3. Have a "simple version" for chain simulation (i.e. I have a sense of what user activity looks like, but idk about compression & data ratios)
4. Have an "advanced version" for fine tuning (i.e. I have a running chain, and I know what every minute input means).

### References:
- [Superchain Usage & Economics Dashboard](https://dune.com/oplabspbc/op-stack-chains-l1-activity): Data sourced from Dune (for the subset of chains listed) and Goldsky (uploaded) for the unsupported OP Chains.
- [by OP Chain - Economics Deep-Dive Dashboard](https://dune.com/oplabspbc/optimism-l2-l1-economics): Data only available (so far) for chains Listed in Dune, so far.
- Labs Internal Google Sheet Calculator

### Other Ideas:
*Open for anyone to jump on!*
1. Dedicated website for Superchain economics, chain simulations/projections, and user-facing transaction cost expectations.