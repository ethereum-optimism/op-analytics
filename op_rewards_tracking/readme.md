## Data Sources
- [OP Incentive Program Info](https://www.notion.so/oplabs/26d856d5ad7c4fda919c62e839cf6051?v=4e38174b2e994129b51dcfa127965aa4) (requires manual update)
- [OP Incentive Program Info - Dune](https://dune.com/queries/1861732) (requires manual update)
- [Incentive Program Usage Stats](https://dune.com/queries/2195796)
- [OP Deployed](https://dune.com/queries/1886707)
- [OP Summer TVL Latest Stats](https://github.com/MSilb7/msilb7-crypto-queries/blob/main/L2%20TVL/csv_outputs/op_summer_latest_stats.csv)
- [OP Incentivized DEX Pool Summary](https://dune.com/queries/1904611) (requires manual update)
- [OP Incentivized DEX Pool Performance](https://dune.com/queries/2175452/3563944)

## Instructions
![](/op_rewards_tracking/simplified_op_reward_tracking_workflow.jpg?raw=true "Simplified OP Reward Tracking Workflow")

The illustration above is a simplifed version of how OP reward tracking workflow looks like, updated as of March 2023.

There are 3 main work streams:

- Tracking program allocation, start/end announcements and other program details in [OP Incentive Program Info](https://www.notion.so/oplabs/26d856d5ad7c4fda919c62e839cf6051?v=4e38174b2e994129b51dcfa127965aa4), which then gets fed into [OP Incentive Program Info - Dune](https://dune.com/queries/1861732) as well as downstream summary statistics like [Incentive Program Usage Stats](https://dune.com/queries/2195796)
- Tracking OP token distribution, including claims, deploys and transfers internally and between programs [OP Deployed](https://dune.com/queries/1886707). More details can be found in these [spells](https://github.com/duneanalytics/spellbook/tree/main/models/op/token_distributions/optimism).
- In terms of DeFi related projects, tracking net TVL flows via APIs from [DeFiLlama](https://defillama.com/) and [The Graph](https://thegraph.com/en/), and volumes via `dex.trades` on Dune.

All the sources are then joined together in [op_incentive_program_summary](/op_rewards_tracking/op_incentive_program_summary.ipynb) for aggregated fund season level and app level analysis, and [op_incentives_program_attribution](/op_rewards_tracking/op_incentives_program_attribution.ipynb) for TVL and token distribution attribution of specific programs per app.

## Methodology
In the latest version, we meausre the pefromance of an incentive program based on its impact on general usage (i.e. incremental transactions, addresses and gas fees), as well as the goals it is designed to achieve (i.e. liquidity mining, trading volume, etc.).

The incremental impact is measured against the average performance of an app 30 days before an incentive program launch. Once the incentive program ends, we will also measure its impact on usage and other goals in the 30 days after program ends.

This method is by no means the best as incentive programs do overlap and there are external forces that may change the usage pattern significantly that is not a direct result of token incentives. We will continue to refine our measurement to understand the effectiveness of token incentives.
