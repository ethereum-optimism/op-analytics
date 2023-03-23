## Data Sources
- [OP Incentive Program Info](https://oplabs.notion.site/26d856d5ad7c4fda919c62e839cf6051?v=4e38174b2e994129b51dcfa127965aa4) (requires manual update)
- [OP Incentive Program Info - Dune](https://dune.com/queries/1861732) (requires manual update)
- [Incentive Program Usage Stats](https://dune.com/queries/2195796)
- [OP Deployed](https://dune.com/queries/1886707)
- [OP Summer TVL Latest Stats](https://github.com/MSilb7/msilb7-crypto-queries/blob/main/L2%20TVL/csv_outputs/op_summer_latest_stats.csv)
- [OP Incentivized DEX Pool Summary](https://dune.com/queries/1904611) (requires manual update)
- [OP Incentivized DEX Pool Performance](https://dune.com/queries/2175452/3563944)

## Instructions
![](/op_rewards_tracking/simplified_op_reward_tracking_workflow.jpg?raw=true "Simplified OP Reward Tracking Workflow")

The diagram above represents a simplified version of the OP reward tracking workflow, as of March 2023.

There are three main workstreams involved:

- Tracking program allocation, start/end announcements and other program details in [OP Incentive Program Info](https://www.notion.so/oplabs/26d856d5ad7c4fda919c62e839cf6051?v=4e38174b2e994129b51dcfa127965aa4). This information is fed into [OP Incentive Program Info - Dune](https://dune.com/queries/1861732) and downstream summary statistics, such as [Incentive Program Usage Stats](https://dune.com/queries/2195796).
- Monitoring OP token distribution, which includes claims, deploys, and transfers internally and between programs in [OP Deployed](https://dune.com/queries/1886707). Further details can be found in these [spells](https://github.com/duneanalytics/spellbook/tree/main/models/op/token_distributions/optimism).
- For DeFi-related projects, tracking net TVL flows via APIs from [DeFiLlama](https://defillama.com/) and [The Graph](https://thegraph.com/en/), as well as volumes through `dex.trades` on Dune.

All sources are combined in [op_incentive_program_summary](/op_rewards_tracking/op_incentive_program_summary.ipynb) for aggregated fund season level and app-level analysis, and [op_incentives_program_attribution](/op_rewards_tracking/op_incentives_program_attribution.ipynb) for TVL and token distribution attribution of specific programs per app.

## Methodology
In the latest iteration, we evaluate the performance of an incentive program based on its impact on general usage (i.e., incremental transactions, addresses, and gas fees) and the goals it aims to achieve (e.g., liquidity mining, trading volume).

The incremental impact is measured against the average performance of an app 30 days before an incentive program launch. Once the incentive program ends, we also assess its impact on usage and other goals in the 30 days following the program's conclusion.

This methodology is not perfect, as incentive programs can overlap and external forces may significantly alter usage patterns in ways that are not directly tied to token incentives. We will continue working with our community to refine our measurements to better understand the effectiveness of token incentives.
