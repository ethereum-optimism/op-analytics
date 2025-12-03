# Address Lists
Folder storing various address lists for reference.
- These address lists may also be available in data providers such as [Dune](https://dune.com/) and [Flipside](https://flipsidecrypto.xyz/). Please feel free to port these lists in to other tools and use these lists for analysis. Get in touch, and share your work with us in the #analytics channel in the Optimism discord.

## Airdrop 1

#### op_airdrop_1_addresses_detailed_list.csv
Airdrop #1 (May 2022) address lists, category eligibility, and total OP eligible to claim.
- `address`: User address
- `total_op_eligible_to_claim`: Total OP the address was eligible to claim. *Note: These numbers need to be divided by 10^18 to get to the # of OP (they are in raw form, not decimal adjusted)*


## Background

The Optimism Foundation’s Airdrop #1 rewards those who have been instrumental as early adopters and active users of projects in the Optimism ecosystem. 
To celebrate our Ethereum roots, The Optimism Foundation also wants to welcome active L1 participants who can help scale Ethereum’s innovations, culture, and values to Layer 2. 
In total, **248,699** addresses are eligible to claim OP in this *initial* airdrop.

Our airdrop has six sets of criteria. 
Two for OP Mainnet users that target early adopters and active project users, and four for L1 Ethereum, which target active contribution, positive-sum behaviors, and active participation. 
Each set is distinct, meaning that an address can be eligible for multiple and allocated a sum of tokens accordingly. 
A snapshot of addresses was taken on 03-25-2022 0:00 UTC.
The allocations and criteria break down as follows:

## Airdrop #1 Allocations

| Name | # of Addresses Eligible | OP Allocated per Address |
| -------- | ----------------------: | -----------------------: |
| OP Mainnet Users | 92,157 | 776.86
| Repeat OP Mainnet Users* | 19,174 | 1,692.49
| DAO Voters | 84,015 | 271.83
| Multisig Signers | 19,542 | 1,190.26
| Gitcoin Donors (on L1) | 23,925 | 555.92
| Users Priced Out of Ethereum | 74,272 | 409.42

\* *Note that Repeat OP Mainnet Users Receive 776.86 + 1,692.49 = 2,469.35 OP*


| Overlap Bonuses | # of Addresses Eligible | OP Allocated per Address
| - | -: | -:
| 4 Categories & OP Mainnet User | 2,707 |  4,180.54
| 5 Categories & OP Mainnet User |   627 | 13,330.16
| 6 Categories & OP Mainnet User |    45 | 27,534.98


The amount of OP tokens that an address receives is cumulative, meaning that the sum of OP tokens allocated for each matching criteria set is the amount that an address is eligible to claim (overlap bonuses are not cumulative).

### OP Mainnet Early Adopters

The Foundation sought to identify OP Mainnet users who had actively used applications on OP Mainnet as a part of their crypto experience. 

### OP Mainnet Users

This group selects for addresses that have used OP Mainnet, including both early adopters and newer users, but narrows down to a group that has used OP Mainnet multiple times. 

*Criterion: Address bridged to OP Mainnet from L1 during the early phases of mainnet (prior to Jun 23, 2021), or used OP Mainnet for more than 1 day (at least 24 hours between their first and last transaction) and made a transaction using an app (after to Jun 23, 2021).*
- *These rules are only based on usage on OP Mainnet. It does not matter which bridge, exchange, fiat on-ramp, etc you used in order to come to OP Mainnet.*

### Repeat OP Mainnet Users

This tier selects for the most active OP Mainnet users, who repeatedly come back to use applications in the Optimism ecosystem.

*Criterion: Address is an ‘OP Mainnet User’ and made at least 1 transaction with an OP Mainnet application across four or more distinct weeks. This selects for the top 20% of ‘OP Mainnet Users’.*
- *"Distinct weeks" are counted based on the time of an addresses' first transaction (i.e. days 1 - 7 are considered Week 1, not the calendar week). This makes sure that there is no skew by which day of the week an address joined OP Mainnet.*

### Active Ethereum Participants

These rules aim to target behaviors that match Ethereum and Optimism’s values of active contribution, positive-sum behavior, and scaling decentralized applications to the world.

### DAO Voters

The Foundation believes that active and engaged governance is crucial to scaling decentralized systems. 
Addresses who match this criterion have actively chosen to actively contribute by participating in governance.

*Criterion: Address has either voted on or authored at least one proposal onchain, or at least two on Snapshot (offchain).*
- *We filtered to “active DAOs” who have had at least 5 proposals with at least 5 votes.*
- *Onchain governance contracts included: Governor Alpha and Bravo, Aave, Curve, Maker, Aragon, DAOHaus, DAOStack, and forks.*
- *Since Snapshot votes were offchain, the Foundation recognized that these were more susceptible to spam/farming behavior, often driven by voters with nominal voting power. 
  To mitigate this, we filtered Snapshot votes to voters who made up the top 99.9% of total voting power in each DAO (i.e. >= 0.1% of voting power was made up by the sum of all smaller voters).*

### Multi-Sig Signers

Multi-sig signers are entrusted with larger pools of capital or control over key protocol functions. 
They are often the present (and future) DAO leaders and builders.

*Criterion: Address is a current signer on a Multi-Sig which has executed at least 10 transactions all-time (this cohort includes 95% of all multisig transactions).*
- *Multisig Wallets Include: Gnosis Safe v0.1.0-1.3.0, MultiSigWithDailyLimit, MultiSigWalletWithTimeLock, and addresses in Etherscan’s ‘Multisig’ label which had a function to get owner addresses.*

### Gitcoin Donors (on L1)

Gitcoin donors have chosen to behave in positive-sum ways by funding public goods. 
These addresses may also align with Optimism’s goal to build sustainable funding source for public goods through [retroactive funding (RetroPGF](https://medium.com/ethereum-optimism/retropgf-experiment-1-1-million-dollars-for-public-goods-f7e455cbdca)).

*Criterion: Address has made an onchain donation through Gitcoin on L1. This includes any donation, regardless of if it was during a matching round.*
- *For the time period between rounds 1 - 5, we included addresses who interacted with Gitcoin contracts, sent legacy `ExecuteSubscription` calls, or appeared in Gitcoin’s donor API.*
- *For the period between rounds 6 - 13 (present), we included donor addresses in the Gitcoin ‘BulkCheckout’ contract transaction logs.*

### Users Priced Out of Ethereum

Active users of dapps on Ethereum are critical to ecosystem growth. Many of these addresses have started bridging to other chains due to high fees, and we want to help retain them in the Ethereum ecosystem while rewarding their curiosity and exploration. Optimism’s airdrop is also calibrated to reward loyalty to Ethereum, so users who have abandoned Ethereum entirely would not receive an airdrop.

*Criterion: Address bridged to another chain, but still made an app transaction on Ethereum in each month after they bridged, and transacted at an average rate of at least 2 per week since then (top 60% of matching addresses).*

- *Bridges included top L1s by TVL: Terra, BSC, Fantom, Avalanche, Solana, Polygon; and general-purpose L2s: Arbitrum, OP Mainnet, Metis, Boba.*
- *To ensure that we gave a long enough period to sample activity, addresses had to have bridged away from Ethereum at least 90 days before the snapshot.*

### Overlap Bonus

Early OP Mainnet users who also match multiple Ethereum criteria may be most likely to become important participants in the OP Mainnet ecosystem, so these addresses were rewarded with an extra overlap bonus.

*Criterion: Address matches an ‘OP Mainnet Early Adopter’ criterion, and matches at least 4 criteria sets in total (including OP Mainnet criteria).*

- *The overlap bonus increases as more criteria are matched (i.e. the bonus for 5 criteria is greater than the bonus for 4 criteria)*

### Global Filtering Criteria

We believe that it’s best for the community for our airdrop list narrows as well as possible to real users and honest actors. 
So we applied some basic filters to various criteria:

- **Address Activity:** Addresses needed to have used Ethereum for more than 1 day (24 hours between their first and last transaction) in order to qualify for ‘Active Ethereum Participants’ criteria. 
  This applied to all Ethereum criteria except for multisig signers, since it’s common for signers to have “signing-only” addresses.
- **Sybil Farmers:** We identified a few patterns of likely sybil attackers, who often created tens, hundreds, or more duplicate addresses. 
  We required stronger activity criteria for these addresses, which helped filter out these attackers, but also preserved many real users. 
  It’s impossible to catch everything, but removing as many sybils as we can help get more OP in the hands of true positive-sum participants.
- **Snapshot Bots and Spam:** We used [ENS’ bot-catching proposal](https://snapshot.org/#/ens.eth/proposal/QmfLwPbo5UwnyvkXo7emsSMDMFCr8UtJ76n87v9Rf7gEuH) to filter out addresses that spam Snapshot votes. Snapshot spaces that appeared to be either compromised or falsified were also excluded.
- **Exchanges and On-Ramps:** We filtered our known centralized exchange and fiat on-ramp addresses.
- **Exploiters:** Known exploit addresses were filtered out.
- **Recency Filter:** Addresses needed to have made a transaction after Jan 1, 2019. 
  For multisig signers, only the multisig needed to have executed a transaction after Jan 1, 2019. 
  We chose this cutoff date to narrow in on active addresses, while also providing a window before [L1 gas prices](https://etherscan.io/chart/gasprice) consistently rose and potentially priced some users out of Ethereum.

### Additional Sybil Filtering

Optimism is for the people, not the sybils.
After the announcement of Airdrop #1, we received limited reports of sybil activity that was not excluded by our initial filtering.
We have since applied additional filtering to Airdrop #1 that excludes 17k addresses and recovers 14m OP.
Recovered OP will be redistributed proportionally to remaining addresses already included in Airdrop #1.
The [Airdrop #1 Allocations](#airdrop-1-allocations) table above has been updated accordingly to reflect this.
For a list of excluded sybil addresses, [see this spreadsheet](https://docs.google.com/spreadsheets/d/1Yt2YEs9A2cWRmBwFYd3M-K4SYZQo3CBm-PnfELXb1Us/edit?gid=822397105#gid=822397105).

In the interest of maintaining the integrity of future OP Airdrops, we will not be publishing the additional filters used to remove these addresses.

## What’s Next?

To check your eligibility for Airdrop #1 and claim your tokens visit [the airdrop app](https://app.optimism.io/airdrop/check).

We made our best effort to make Airdrop #1 as fair as possible.
However, there’s always a chance some great community members may have slipped through the cracks.
Don’t worry!
This is Airdrop #1, and 14% of the total initial token supply is reserved for future airdrops.
The best way to earn OP is to get involved in the Optimism Collective:

- If you want to build on Optimism, visit our [developer documentation](https://docs.optimism.io/) or jump in to [Discord](https://discord-gateway.optimism.io).
- If your talents are non-technical, jump in to [Discord](https://discord-gateway.optimism.io) to find other builders, apply to be a Support NERD, or just make some memes 🍉
- [Bridge your assets to OP Mainnet here](https://app.optimism.io/bridge) and explore projects on OP

As always: stay safe and stay Optimistic.

## References

- **Application Transaction:** Any transaction, excluding token transfer and approval transactions. 
- **Airdrop #1 Dashboard:** [https://dune.com/optimismfnd/optimism-airdrop-1](https://dune.com/optimismfnd/optimism-airdrop-1)
- **Detailed Protocol Metrics:** [https://dune.com/optimismfnd/Optimism](https://dune.com/optimismfnd/Optimism)
- **OP Token Address:** [0x4200000000000000000000000000000000000042](https://explorer.optimism.io/address/0x4200000000000000000000000000000000000042)
- **Optimism on Twitter:** [https://twitter.com/optimismFND](https://twitter.com/optimismFND)
- **Join the Optimism Discord:** [https://discord-gateway.optimism.io/](https://discord-gateway.optimism.io/)
- **CSV with the list of airdropped addresses:** [op_airdrop1_addresses_detailed_list.csv](https://github.com/ethereum-optimism/op-analytics/blob/main/reference_data/address_lists/op_airdrop1_addresses_detailed_list.csv)

#### op_unclaimed_airdrop_1_distribution_simple_list.csv
Simplified address list for unclaimed Airdrop #1 distribution (Sep 2023). This was distributed directly to wallets, and distribution is completed. See [OP Governance notification](https://twitter.com/OptimismGov/status/1702748223847170261).
- `address`: User address
- `total_op`: Total OP the address received in human readable form (i.e. already decimal adjusted)

## Airdrop 2 

#### op_airdrop2_simple_list.csv
Simplified Airdrop #2 (Feb 2023) address list with total OP eligible to claim (this was distributed directly to wallets, and distribution has completed). 
- `address`: User address
- `total_op`: Total OP the address received in human readable form (i.e. already decimal adjusted)
- **Dashboards**
  - [@theericstone on Flipside / Optimism Airdrop Round 2](https://flipsidecrypto.xyz/theericstone-pine/optimism-airdrop-round-2-qu3UsW)
  - [@springzhang on Dune / OP Airdrop #2 Summary](https://dune.com/springzhang/op-airdrop-2-summary)
  - [@oplabspbc on Dune / OP Airdrop #2 Distributions](https://dune.com/oplabspbc/op-airdrop-2-distributions)

#### op_airdrop_2_addresses_detailed_list.csv
Airdrop #2 (Feb 2023) address lists, category eligibility, and total OP received (this was distributed directly to wallets, and distribution has completed).
- `address`: User address
- `total_op_eligible_to_claim`: Total OP the address received. *Note: These numbers need to be divided by 10^18 to get to the # of OP (they are in raw form, not decimal adjusted)*

## Background

Optimism’s Airdrop #1 distributed over 200m OP tokens to 250,000 early adopters and engaged users in May 2022.
With the introduction of OP, the Collective set out to establish a bicameral governance system that provides the foundation for our growing digital city.

Since then, the Token House has matured as the first piece of Optimism governance.
As of today (February 9th, 2023) 88k addresses have voted on over 90 proposals to distribute more than 55m OP tokens across the ecosystem, and Optimism’s first protocol upgrade is headed to vote in February. A total of 293k addresses are **delegating their voting power** — a positive-sum activity that helps strengthen the fabric of Optimism’s governance system.
At the same time, OP Mainnet is developing into a booming economy. 
Over 1.5 million addresses have sent 64 million transactions since our first Airdrop, spending nearly $15 million in fees on the network. 
Participation in governance and in the Optimism economy is what helps our Collective grow and thrive.

Optimism’s Airdrop #2 distributes 11,742,277.10 OP to 307,965 unique addresses to reward **positive-sum governance participation** and **power users of OP Mainnet**. 
A snapshot of addresses was taken on 01-20-2023 0:00 UTC.

Read on for more detail about eligibility criteria and distribution.

::: tip Airdrop #2 does not need to be claimed

Airdrop #2 is distributed directly to eligible wallets. 
There is no need to claim tokens by interacting with any website.
Do not trust any website pretending to help you claim your tokens for Airdrop #2. 
If an address is eligible, it will have tokens sent directly to it on February 9th, 2023 by 8pm ET.

:::

Airdrop #2 rewards users for their involvement in Optimism governance and covers a portion of their network usage costs:

- Governance delegation rewards based on the amount of OP delegated and the length of time it was delegated.
- Partial gas rebates for active Optimism users who have spent over a certain amount on gas fees.
- Multiplier bonuses determined by additional attributes related to governance and usage.


## Airdrop #2 allocations

| Reward Type                      | Criteria | Number of qualifying addresses | Reward allocation |
| -------------------------------- | -------- | -----------------------------: | ----------------- |
| **Governance Delegation Reward** | Has had ≥ 2,000 total `OP Delegated x Days`<sup>(1)</sup> | 57,204 | <sup>0.42</sup> &#8260; <sub>365</sub> OP per `OP Delegated x Day`, max 5,000 OP per address<sup>(2)</sup> |
| **Gas Usage Reward** | Spent ≥ the average cost of one L1 transaction ($6.10)<sup>(3)</sup> on Optimism | 280,057 | 80% of gas fees rebated in OP, up to $500 of gas fees rebated per address<sup>(4)</sup> |


(1) `OP Delegated x Days` = Cumulative Sum of OP Delegated per Day (i.e. 20 OP delegated for 100 days: 20 * 100 = 2,000 OP Delegated x Days).

(2) Addresses which received OP from Token House Governance are not eligible for the reward.

(3) The average cost of one L1 transaction on Ethereum was measured during the period between the airdrop #1 snapshot and airdrop #2 snapshot.

(4) Gas Fees USD conversion was made at the time of the transaction. 
OP rebated conversion was made using the trailing 7-day average market price at the time of the snapshot.

## Bonus Attributes

There are 4 additional attributes that addresses can earn multiplier bonuses for:

| Attributes | Qualifying addresses | Multiplier (bonus) |
| --- | --: | --- |
| 0 | 206,033 | 1.00x (No Bonus) |
| 1 | 53,529 | 1.05x (5% Bonus)  |
| 2 | 39,531 | 1.10x (10% Bonus) |
| 3 | 6,320 | 1.50x (50% Bonus)  |
| 4 | 2,552 | 2.00x (100% Bonus) |


### Attribute Description

| Attribute criteria | Description | Qualifying addresses |
| --- | --- | ---: |
| Governance: Had an Active Delegation ≥ 20 OP at Snapshot | Addresses that remained delegating to Optimism Governance | 30,762 |
| Governance: Had 54,367 total ‘OP Delegated x Days’<sup>(5)</sup> | Addresses that had a larger Governance commitment | 23,089 |
| Usage: Had made App transactions<sup>(6)</sup> on Optimism across 6 distinct months | Addresses that remained active on Optimism for a longer period of time | 52,438 |
| Usage: Spent ≥ $20 on Gas Fees | Addresses that have done a larger sum of activity on Optimism | 55,470 |

(5) 54,367 `OP Delegated x Days` is the equivalent of delegating the smallest reward from Airdrop #1 (271.83 OP) for 200 days (Since July 4, 2022).

(6) App transactions were counted as any transaction excluding token approvals, token transfers, and ETH wrapping/unwrappping transactions. "Distinct months" measured as trailing 30 day periods from the snapshot date.



## Analytics

The address list is available [on Github](https://github.com/ethereum-optimism/op-analytics/tree/main/reference_data/address_lists) and is shared with Dune and Flipside.
Please use the #analytics channel in [the Optimism Discord](https://discord-gateway.optimism.io/) for any additional sharing requests or schema questions 

## Airdrop 3

#### op_airdrop_3_simple_list.csv
Simplified address list for Airdrop #3 distribution (Sep 2023). This was distributed directly to wallets, and distribution has completed. 
- `address`: User address
- `total_op`: Total OP the address received in human readable form (i.e. already decimal adjusted)

#### op_airdrop_3_addresses_detailed_list.csv
Airdrop #3 address lists, category eligibility, and total OP received. This was distributed directly to wallets, and distribution is *completed*. 
- `address`: User address
- `op_amount_decimal_adjusted`: Total OP the address received in human readable form (i.e. already decimal adjusted)
- `op_amount_raw`: Total OP the address received. *Note: These numbers need to be divided by 10^18 to get to the # of OP (they are in raw form, not decimal adjusted)*

Airdrop 3 took place on Monday, September 18. The airdrop allocated 19,411,313 OP to 31,870 unique addresses. 

Similar to Airdrop 2, this drop rewards positive sum governance participation. Users who have delegated tokens between 2023-01-20 at 0:00 UTC and 2023-07-20 0:00 UTC received tokens in this airdrop. A bonus is given to addresses who delegated to a delegate who voted in Optimism Governance. 

Read on for more detail about eligibility criteria and allocations.

> Airdrop #3 is disbursed directly to eligible wallets. There is no need to claim tokens by interacting with any website. Do not trust any website pretending to help you claim your tokens for Airdrop #3. If an address is eligible, it will have tokens sent directly to it on or shortly after Sep 15, 2023.

## Airdrop #3 Allocations

| Reward Type | Criteria | Number of qualifying addresses | Reward allocation
| - | -: | -: | -:
| Governance Delegation Reward | Delegated OP above the minimum threshold | 31,529 | 0.67 ⁄ 365 OP per `OP Delegated x Day`(1), max 10,000 OP per address
| Voting Delegate Bonus | Delegated to an address that voted onchain at least once (2) | 25,561 | (0.67 ⁄ 365)*2 OP per `OP Delegated x Day to a voting delegate`, max 10,000 OP per address

### Details

(1) `OP Delegated x Days` = Cumulative Sum of OP Delegated per Day (i.e. 20 OP delegated for 100 days: 20 * 100 = 2,000 OP Delegated x Days).

(2) Delegate must have voted onchain in OP Governance during the snapshot period (01-20-2023 at 0:00 UTC and 07-20-2023 0:00 UTC )

- See the list of addresses and allocation amounts [here](https://github.com/ethereum-optimism/op-analytics/blob/main/reference_data/address_lists/op_airdrop_3_simple_list.csv)

### Cutoff Criteria

- Addresses with fewer than 18,000 `OP Delegated x Days` (or 9,000 if delegated to a voting delegate) were not eligible for this airdrop.
- Addresses who were delegated for < 7 days were not eligible for this airdrop
- Known delegation program wallets were not eligible for this airdrop.
- Each reward type had a maximum reward of 10,000 OP per address.

*Note: A small set of delegation activity was not included in Airdrop #2. The eligible amounts were added to this allocation, resulting in 341 additional addresses.*

### How it works:

- To work out your `Cummultive Delegated OP`, multiply your OP delegated by the time delegated: `OP delegated x Days` = `Cummultive Delegated OP`. To qualify for the airdrop your `Cummultive Delegated OP` needs to be larger than 18,000. If you delegated to a “Voting Delegate” the `Cummultive Delegated OP` only needs to be above 9,000. For example, if you delegated 80 OP tokens for 180 days that would make a `Cummultive Delegated OP` of 14,400 which would NOT qualify for the airdrop. However, if the delegate you delegated to was active, then your 14,400 is above the 9,000 required for a voting delegate and you WOULD receive the airdrop.
    - Addresses who delegated for less than 7 days are not eligible.
    - Known delegation program wallets were not eligible for this airdrop.
    - Each reward type had a maximum reward of 10,000 OP per address.
- For the Voting Delegate Bonus the address you have delegated to MUST have voted onchain in OP Governance during the snapshot period (2023-01-20 at 0:00 UTC and 2023-07-20 0:00 UTC)

## Airdrop 4

#### op_airdrop_4_simple_list.csv
Simplified address list for Airdrop #4 distribution (Feb 2024). Claims were available at [optimism.io](https://app.optimism.io/airdrops/4). 
- `address`: User address
- `total_op`: Total OP the address received in human readable form (i.e. already decimal adjusted)

#### op_airdrop_4_addresses_detailed_list.csv
Airdrop #4 (Feb 2024) address lists, category eligibility, and total OP received. Claims were available at [optimism.io](https://app.optimism.io/airdrops/4). 
- `address`: User address
- `is_eligible_...` & `a_`,`b_`,`c_`,`..._`: True/False if the address was eligible for the category.
- `multiplier`: The address' multiplier bonus, if qualified
- `op_amount_raw`: Total OP the address received. *Note: These numbers need to be divided by 10^18 to get to the # of OP (they are in raw form, not decimal adjusted)*

Airdrop #4 celebrates the vibrant creative energy that artists introduce to the Superchain, and highlights the essential role creators fulfill within the Optimism Collective and the broader Ethereum ecosystem. Creators with addresses that deployed NFT contracts on Ethereum L1, Base, OP Mainnet and Zora before 2024-01-10 00:00:00 UTC were considered in this airdrop.

Read on for more detail about eligibility criteria and allocations.

> Airdrop #4 needs to be claimed 
> Go to [https://app.optimism.io/airdrops](https://app.optimism.io/airdrops) to claim.

## Airdrop 4 Allocations

| Reward Type | Criteria | Number of qualifying addresses | Reward allocation
| - | :-: | -: | :-:
| You created engaging NFTs on the Superchain | Total gas on OP Chains (OP Mainnet, Base, Zora) in transactions involving transfers<sup>2</sup> of NFTs created by your address<sup>1</sup>. Measured during the trailing 365 days before the airdrop cutoff (Jan 10, 2023 - Jan 10, 2024) | 9,294 | 5,000 OP per 1 ETH of gas on the Superchain (i.e. 0.002 ETH of gas fees = 10 OP)
| You created engaging NFTs on Ethereum Mainnet | Total gas on Ethereum L1 in transactions involving transfers<sup>2</sup> of NFTs created by your address<sup>1</sup>. Measured during the trailing 365 days before the airdrop cutoff (Jan 10, 2023 - Jan 10, 2024) | 15,073 | 50 OP per 1 ETH of gas on Ethereum L1 (i.e. 0.2 ETH of gas fees = 10 OP)


1. “Creator addresses” were identified as the transaction sender (`from` address) in the transaction where the NFT (ERC721 & ERC1155) contract was created.
1. “Transactions with NFT transfers” excluded transfers to the creators' address (i.e. mint to themselves) and transfers to a burn address. While transfers were measured over the 365 days before cutoff, the NFT contract could’ve been created at any time.

See the list of addresses and allocation amounts [here](https://github.com/ethereum-optimism/op-analytics/blob/main/reference_data/address_lists/op_airdrop_4_simple_list.csv)

### Bonus Attributes
There were 5 attributes that addresses could earn multiplier bonuses for:

| Amount of Attributes | Qualifying addresses | Multiplier (bonus)
| - | -: | -: |
| 0 | 2,969 | (No Bonus)
| 1 | 3,628 | 1.05x (5% Bonus)
| 2 | 4,566 | 1.10x (10% Bonus)
| 3 | 8,305 | 1.25x (25% Bonus)
| 4+ | 3,530 | 1.50x (50% Bonus)

## Attribute Description 

| Criteria name | Attribute Criteria | Qualifying addresses
| - | -: | -: |
| Early creator | You created your first NFT contract before Jan 1, 2023 | 7,443
| Rising creator | An NFT you created was transferred after Nov 11, 2023 | 14,927
| Active onchain | You’ve been active on the Superchain or L1 since Nov 11, 2023 | 12,316
| Popular creator | Total gas fees spent in transactions involving transfers of NFTs you created was >= 0.005 ETH in the 180 days prior to cutoff | 16,941
| We ❤️ the Art creators that qualified for the airdrop | In addition to qualifying for the drop, you participated in WLTA | 338

## Rewards Detail
* Addresses with final rewards lower than 20 OP were not eligible.
* The final reward was capped at 6,000 OP per address.

## Airdrop 5

#### op_airdrop_5_simple_list.csv
Simplified address list for Airdrop #5 distribution (Oct 2024). Claims were available at [optimism.io](https://app.optimism.io/airdrops/5). 

- `address`: User address
- `op_total`: Total OP the address received in human readable form (i.e. already decimal adjusted)

#### op_airdrop_5_addresses_detailed_list.csv
Airdrop #5 address lists, category eligibility, and total OP received. Claims were available at [optimism.io](https://app.optimism.io/airdrops/5). 

- `address`: User address
- `is_[category]`: True/False indicates if the address was eligible for the category
- `multiplier`: The address' multiplier bonus, if qualified
- `op_total`: Total OP the address received. *Note: These numbers need to be divided by 10^18 to get to the # of OP (they are in raw form, not decimal adjusted)*

Airdrop #5 rewards power users who have contributed to the growth of the Superchain Ecosystem. The best apps across social, DeFi, gaming and more live on the Superchain, and it’s crucial to support their growth and success. If you're a Superchain power user actively engaging with apps across the ecosystem, you're not just participating—you're helping to scale Ethereum. Your contributions are vital and prove that when we build together, we benefit together.

EOA addresses that made `App transactions` on OP Mainnet, Base, Zora, Mode, Metal, Fraxtal, Cyber, Mint, Swan, Redstone, Lisk, Derive, BOB, Xterio, Polynomial, Race, and Orderly between 2024-03-15 00:00:00 UTC and 2024-09-15 00:00:00 UTC were considered for this airdrop. The airdrop allocated 10,368,678 OP to 54,723 unique addresses.

Read on for more detail about eligibility criteria and allocations.

<Callout type="info">
Airdrop #5 needs to be claimed.
Go to [https://app.optimism.io/airdrops](https://app.optimism.io/airdrops/5) to claim.
</Callout>

<Callout>
`App transactions` are defined as transactions that emit an event log, excluding operational “non-app” events (ERC20/721/1155 token approvals & WETH wrapping/unwrapping).
</Callout>

## Allocations

### Base Rewards

| Reward Name | Criteria | Number of Qualifying Addresses | Reward Allocation |
| --- | --- | --- | --- |
| Superchain Power User | Interacted with ≥ 20 unique contracts¹ on the Superchain² and had a contracts-to-transactions ratio³ of ≥ 10% during the eligibility period from Mar 15, 2024, to Sep 15, 2024. | 54,723 | Rewards scale in proportion to network usage. |

1. `Unique contracts` are calculated as the number of distinct EOA-initiated contract calls on the transaction level (`to` addresses).
2. Activity on OP Mainnet, Base, Zora, Mode, Metal, Fraxtal, Cyber, Mint, Swan, Redstone, Lisk, Derive, BOB, Xterio, Polynomial, Race, and Orderly was considered for this airdrop.
3. `Contracts-to-transactions ratio` is defined as the ratio of unique contracts an EOA address interacted with relative to its total number of transactions (e.g. 25 unique contracts and 100 app transactions = contracts-to-transactions ratio of 25%).

### Bonus Rewards

| Reward Name | Criteria | Number of Qualifying Addresses |
| --- | --- | --- |
| Active Delegator | Had ≥ 9,000 total OP delegated x days delegated¹ during the eligibility period from Mar 15, 2024, to Sep 15, 2024. | 4,155 |
| Frequent User | Made at least 10 app transactions per week in ≥ 20 distinct weeks during the eligibility period from Mar 15, 2024, to Sep 15, 2024. | 9,841 |
| Superchain Explorer | Made at least 1 app transaction on ≥ 7 chains in the Superchain² during the eligibility period from Mar 15, 2024, to Sep 15, 2024. | 10,336 |
| Early Superchain Adopter | Made at least 1 app transaction on ≥ 3 chains in the Superchain in the first week after each chain’s public mainnet launch³. | 6,989 |
| Quester | Completed ≥ 1 Optimism quest between Sep 20, 2022, and Jan 17, 2023. | 14,113 |
| SuperFest Participant | Participated in ≥ 5 [SuperFest](https://superfest.optimism.io/) missions during the campaign period from Jul 9, 2024, to Sep 3, 2024. | 6,732 |
| SUNNYs Fan | Minted NFTs from ≥ 3 unique contracts that registered for [the SUNNYs](https://www.thesunnyawards.fun/)⁴ during the eligibility period from Mar 15, 2024, to Sep 15, 2024. | 12,905 |

1. [`OP delegated x days delegated`](airdrop-3) is calculated as the cumulative sum of OP delegated per day (e.g. 50 OP delegated for 100 days = 5,000 OP delegated x days delegated).
2. Activity on OP Mainnet, Base, Zora, Mode, Metal, Fraxtal, Cyber, Mint, Swan, Redstone, Lisk, Derive, BOB, Xterio, Polynomial, Race, and Orderly was considered for this airdrop.
3. [`Public mainnet launch`](https://github.com/ethereum-optimism/op-analytics/blob/main/op_chains_tracking/inputs/chain_metadata_raw.csv) is defined as the date the public mainnet was officially announced.
4. Includes contracts that were registered on the official SUNNYs signup page between Aug 21, 2024, and Sep 15, 2024.
    
### Bonus Attributes

| **Number of Attributes** | **Number of Qualifying Addresses** | **Multiplier** |
| --- | --- | --- |
| 0 | 22,772 | 1.0x |
| 1 | 13,449 | 1.5x |
| 2 | 9,760 | 2.5x |
| 3 | 4,988 | 4.0x |
| 4 | 2,252 | 6.75x |
| 5 | 1,001 | 10.75x |
| 6 | 382 | 17.5x |
| 7 | 119 | 27.5x |

## Reward Details

- Addresses that interacted with less then 20 unique contracts were not eligible for this airdrop.
- Addresses with a contracts-to-transactions ratio of less than 10% were not eligible for this airdrop.
- Addresses with total rewards below the minimum threshold of 50 OP were not eligible for this airdrop.
- The base reward size was capped at 150 OP.
- The total reward size was capped at 3,500 OP.
- See the [list of addresses and allocation amounts](https://github.com/ethereum-optimism/op-analytics/blob/main/reference_data/address_lists/op_airdrop_5_simple_list.csv).

## SuperStacks

#### op_superstacks_addresses_detailed_list.csv
SuperStacks address lists and total OP received. Claims were available at [app.optimism.io/superstacks](https:/app.optimism.io/superstacks). 

- `address`: User address
- `op_total`: Total OP the address received. *Note: These numbers need to be divided by 10^18 to get to the # of OP (they are in raw form, not decimal adjusted)*

## About

With many chains building as one, a new network structure is emerging to solve fragmentation in Ethereum. This network is modular, interoperable, and composable by default. We call it the Superchain: and it changes everything.

However, infrastructure alone is not enough. We need new ways to reward those who participate in this shared network. Systems that recognize contribution not to a single app or chain, but to the whole.

At Optimism, we believe in building for the long haul, and experimenting, iterating, and learning along the way. That’s why we’re introducing *SuperStacks:* a pilot points program testing a more proactive and intentional approach to ecosystem rewards that moves beyond traditionally siloed incentives. 

SuperStacks aims to reward behavior around interoperable assets and applications that work seamlessly across multiple chains. The program is specifically designed to prepare the ecosystem for the upcoming interoperability launch by accelerating development of apps and use cases that will thrive in an interoperable Superchain. It will generate insights that help evolve this program into a long-term engine for ecosystem growth.

Get more detailed info:
* [SuperStacks Blog post](https://optimism.mirror.xyz/VcgOzzYe4iQ_Ouh5cs9AWPuStZn8xQEH2PvUw8JkFYk)
* [SuperStacks Leaderboard](https://app.optimism.io/superstacks)
* Learn more about [Superchain Interop](https://docs.optimism.io/interop/explainer)

# FAQs

## Basic Questions

### When does SuperStacks begin and when does it end?

The program starts on the 16th of April 2025 @ 16:00 UTC, and ends on the 30th of June 2025 @ 11:59pm UTC. 

### When can I redeem my points?

After the program is completed the amount of OP tokens you can redeem for each XP will be announced, and the claims page will be published. More information about when claims will go live will be made available towards the end of the program. 
OP tokens left unclaimed after one year will be returned to the treasury and can no longer be claimed. The claims page will be linked to on the [SuperStacks Leaderboard](https://app.optimism.io/superstacks). 

### Which chains, protocols & pools are included?

The starting list of pools can be found on the [SuperStacks Leaderboard](https://app.optimism.io/superstacks). More chains, protocols and tokens may be added during the program, so keep an eye out for new pool announcements! 

### How is the XP calculated?

10 XP = $1 in liquidity for 24hrs

You will start to receive XP once your liquidity has been in a qualifying pool for 24hrs. For every $1 that remains in the qualifying pool for 24 consecutive hrs, you earn 10 XP, per pool.

Please ensure that the protocol you are using is the correct version (for example, Uniswap v3 vs Uniswap v4). If you are unsure if you have the correct pool you can follow the link to the pool directly from the [SuperStacks Leaderboard](https://app.optimism.io/superstacks). 
Note that the XP points are updated daily. There are also additional bonuses and multipliers that may apply to the pools you are in, resulting in a higher XP amount than the base rate above. These multipliers may change through the course of the program. 

### Why is there no weekly XP points emission rate? 

The emission rate cannot be fixed due to the way XP is earned ($1 to 10 XP per 24 hours). As such, the amount of XP allocated over the week depends on the activity and volume in the various pools. 

You can view the cumulative number of points on the [SuperStacks Leaderboard](https://app.optimism.io/superstacks), enabling you to manually track weekly emissions over time.

## XP Questions

### Where can I check how many points I have?

To check how many points you have see the leaderboard [SuperStacks Leaderboard](https://app.optimism.io/superstacks), or check with our [data provider](https://www.obl.dev/).

### How long does it take for me to get my points?

Your points should be updated after 24hrs. If your points have not been updated within 48 hours, please follow the points self check steps. 

## Meta Questions

### Why is Optimism doing a points program?

Optimism is testing the hypothesis that proactive (do something, get rewarded) points programs may have a stronger impact than retroactive (already done something, get rewarded) airdrop campaigns. 

SuperStacks is the result of extensive, industry-wide research, and this pilot program will give Optimism deep, valuable insights into what’s most effective. This points program is our first experiment with this kind of program, trying to get deeper liquidity on [Superchain Interop](https://docs.optimism.io/interop/explainer) ready assets. Learn more about [Superchain Interop](https://docs.optimism.io/interop/explainer). 

### Why are these incentives focused on stables?

Inline with the [Collective Intents for S7](https://gov.optimism.io/t/season-7-intent/9292) of [Optimism Governance](https://community.optimism.io/welcome/welcome-overview), this program aims to experiment with point program incentives as a means of supporting Superchain TVL. All the assets incentivised are SuperchainERC20s, meaning they are ready for when [Superchain Interop](https://docs.optimism.io/interop/explainer) goes live. Learn more about [Superchain Interop](https://docs.optimism.io/interop/explainer).

### How many OP tokens are in the SuperStacks program?

Optimism is starting with a small, flexible amount of OP and a plan to scale it from there.
Optimism is introducing SuperStacks as a small pilot program to test the hypothesis that proactive points programs can have a stronger impact than retroactive airdrop campaigns. Optimism and its community are known for their openness to experimentation and iteration.

### Why is my smart wallet/Safe not getting points?

Only EOAs are indexed, so smart wallets and Gnosis Safes are not eligible for XP. 

## Bugs & Issues

### The leaderboard is not loading, how can I check my points?

Check your XP with our [data provider](https://www.obl.dev/) directly or on the [SuperStacks Leaderboard](https://app.optimism.io/superstacks).

### Points Self Check / Missing points

Think you should have gotten points but it’s not reflecting on the leaderboard? Follow these steps:
1. Wait 24 hours, in case the points system is lagging.
2. Check with our [data provider](https://www.obl.dev/) directly. 
3. Check that the chain, protocol and the pool you are in is included in the program. Make sure you are using the correct version of the protocol (for example, Uniswap V3 vs Uniswap V4 may both have the pool, but only Uni V4 is incentivised).
4. Double check your tokens were in the pool for at least 24 hours consecutivly.
5. Still sure you should have points? Join our [Community Discord](https://discord.gg/optimism) and head to the SuperStack channel. Share the transaction hash of your assets entering the pool with the SupNERDs in the channel, and they will double check everything for you, and escalate if needed. 

### Having trouble loading the leaderboard? Need help?

If you are having trouble with a protocol, have questions about which pools, chains or protocols are included or need more information, you can get help from a real person in our [Community Discord](https://discord.gg/optimism)! Get help from our amazing Support NERDs, and maybe even stick around for the optimistic vibes 🔴✨\(^-^)/✨🔴

# SuperStacks Allocation

SuperStacks was a pilot program designed to show how the Superchain can create a more connected onchain experience. This was Optimism’s first GTM campaign in Superchain-wide incentives, a live test of how to reward the adoption of interoperable assets across chains focusing on usage, liquidity, and long-term contribution.

Addresses that provided liquidity in DEX pools, supplied liquidity on lending markets, or deposited into vaults between 2025-04-16 16:00:00 UTC and 2025-06-30 23:59:00 UTC were considered for the final OP delivery on OP Mainnet. SuperStacks allocated 2,500,000 OP to unique 6,387 addresses based on activity on Base, Unichain, Ink, World, Soneium, and OP Mainnet. The list of eligible pools and vaults is shown below.

<Callout type="info">
SuperStacks rewards need to be claimed. Go to [app.optimism.io/superstacks](https://app.optimism.io/superstacks) to claim.
</Callout>

<Callout type="warning">
Users that deposited into the WETH vault on Morpho through the Morpho mini app on World will claim their rewards in a SuperStacks mini app that goes live on World on July 22, 2025.
</Callout>

## XP Calculation

Addresses earned 10 XP per $1 in liquidity¹ for every 24 hours, and XP started accruing after the first full 24-hour period. For every $1 that remained in the qualifying pool or vault for 24 consecutive hours, addresses earned 10 XP per pool or vault. XP was calculated separately for each qualifying pool or vault. Furthermore, additional temporary XP multipliers were applied, which increased the XP accrual rate above the baseline. In total, 208,530,214,850 XP was earned.



¹ Only direct liquidity provision was included. Automated liquidity managers (ALMs) were excluded, except for Mellow, which was directly integrated into the Velodrome frontend.

## OP Conversion

A conversion of XP to OP tokens was used, calculated as:

```
(User XP / Total XP) × Total OP Allocated
```

Addresses with final rewards below the minimum threshold of 20 OP were not eligible to claim². The OP tokens that would have gone to these addresses were reallocated proportionally to eligible addresses³. Rewards were allocated in a strictly linear fashion using the conversion rate outlined below. Moreover, no caps were enforced to ensure a fair and transparent allocation for all participants.

| OP-to-XP Conversion Rate | XP-to-OP Conversion Rate |
| ---- | ------------------------------ |
| 1 OP = 70,094.8 - 86,988.3 XP | 1 XP = 0.0000114958 OP - 0.0000142664 OP |

<Callout type="info">
The rewards conversion rate was calculated with support from Open Block Labs
</Callout>

² See full list of addresses and allocation amounts [here](https://github.com/ethereum-optimism/op-analytics/blob/main/reference_data/address_lists/op_superstacks_addresses_detailed_list.csv).

³ Smart wallets, with the exception of natively embedded Morpho mini app wallets on World, were not eligible for the OP or WLD claim.

## How to Claim

| Phase                 | Description                    | Number of Eligible Addresses | Where To Claim?                                                         | Claim Period                |
| --------------------- | ------------------------------ | ------------------ | ------------------------------------------------------------------ | --------------------------- |
| SuperStacks OP Claims | Addresses that participated in SuperStacks through the regular frontend and not the Morpho mini app on World.             | 6,384              | [app.optimism.io/superstacks](https://app.optimism.io/superstacks) | Jul 15, 2025 – Jul 15, 2026 |
| Mini App WLD Claims⁴  | Addresses that participated in SuperStacks through the Morpho mini app on World. | 3                  | [World Mini App](https://world.org/ecosystem/app_bf12ea9b2d5c92d11d3c1a838fe16feb)                             | Jul 22, 2025 – Jul 22, 2026 |

⁴ WLD allocations were converted to their OP equivalents using market exchange rates as of July 15, 2025.

## SuperStacks Pools and Vaults

| Chain      | Protocol     | Pool/Vault                 | Criteria                                                |
| ---------- | ------------ | -------------------------- | ------------------------------------------------------- |
| Unichain   | Uniswap V4   | ETH/USD₮0 (0.05%)          | Provided ≥ $0.01 in DEX liquidity for ≥ 1 hour between Apr 16, 2025, and Jun 30, 2025. |
| OP Mainnet | Velodrome    | CL100-USD₮0/WETH           | Provided ≥ $0.01 in DEX liquidity for ≥ 1 hour between Apr 16, 2025, and Jun 30, 2025. |
| Ink        | Velodrome    | CL100-USD₮0/WETH           | Provided ≥ $0.01 in DEX liquidity for ≥ 1 hour between Apr 16, 2025, and Jun 30, 2025. |
| OP Mainnet | Velodrome    | CL1-USD₮0/USDT             | Provided ≥ $0.01 in DEX liquidity for ≥ 1 hour between Apr 16, 2025, and Jun 30, 2025. |
| Ink        | Velodrome    | CL100-WETH/kBTC            | Provided ≥ $0.01 in DEX liquidity for ≥ 1 hour between Apr 16, 2025, and Jun 30, 2025. |
| Soneium    | Velodrome    | CL100-USD₮0 (Bridged)/WETH | Provided ≥ $0.01 in DEX liquidity for ≥ 1 hour between Apr 16, 2025, and Jun 30, 2025. |
| World      | Uniswap V4   | ETH/USD₮0 (0.05%)          | Provided ≥ $0.01 in DEX liquidity for ≥ 1 hour between Apr 16, 2025, and Jun 30, 2025. |
| Base       | Uniswap V4   | ETH/USD₮0 (0.05%)          | Provided ≥ $0.01 in DEX liquidity for ≥ 1 hour between Apr 16, 2025, and Jun 30, 2025. |
| OP Mainnet | Velodrome    | CL100-USD₮0/kBTC           | Provided ≥ $0.01 in DEX liquidity for ≥ 1 hour between May 6, 2025, and Jun 30, 2025. |
| OP Mainnet | Velodrome    | CL1-USD₮0/USDC             | Provided ≥ $0.01 in DEX liquidity for ≥ 1 hour between May 6, 2025, and Jun 30, 2025. |
| Soneium    | Sake Finance | USD₮0                      | Supplied ≥ $0.01 in lending liquidity for ≥ 1 hour between May 7, 2025, and Jun 30, 2025. |
| Base       | Uniswap V4   | USD₮0/USDC (0.01%)         | Provided ≥ $0.01 in DEX liquidity for ≥ 1 hour between May 8, 2025, and Jun 30, 2025. |
| Unichain   | Uniswap V4   | ETH/weETH (0.01%)          | Provided ≥ $0.01 in DEX liquidity for ≥ 1 hour between May 12, 2025, and Jun 30, 2025. |
| OP Mainnet | Velodrome    | WETH/weETH                 | Provided ≥ $0.01 in DEX liquidity for ≥ 1 hour between May 12, 2025, and Jun 30, 2025. |
| OP Mainnet | Moonwell     | USD₮0                      | Supplied ≥ $0.01 in lending liquidity for ≥ 1 hour between May 16, 2025, and Jun 30, 2025. |
| Unichain   | Euler        | USD₮0                      | Supplied ≥ $0.01 in lending liquidity for ≥ 1 hour between May 28, 2025, and Jun 30, 2025. |
| Unichain   | Euler        | weETH                      | Supplied ≥ $0.01 in lending liquidity for ≥ 1 hour between May 28, 2025, and Jun 30, 2025. |
| OP Mainnet | Velodrome    | CL1-sUSDC/USD₮0            | Provided ≥ $0.01 in DEX liquidity for ≥ 1 hour between Jun 5, 2025, and Jun 30, 2025. |
| Unichain   | Uniswap V4   | sUSDC/USD₮0                | Provided ≥ $0.01 in DEX liquidity for ≥ 1 hour between Jun 5, 2025, and Jun 30, 2025. |
| Unichain   | Velodrome    | CL1-sUSDC/USD₮0            | Provided ≥ $0.01 in DEX liquidity for ≥ 1 hour between Jun 5, 2025, and Jun 30, 2025. |
| World      | Morpho       | WETH                       | Deposited ≥ $0.01 into the vault for ≥ 1 hour between Jun 11, 2025, and Jun 30, 2025. |
| OP Mainnet | Velodrome    | CL1-WETH/ultraETH          | Provided ≥ $0.01 in DEX liquidity for ≥ 1 hour between Jun 16, 2025, and Jun 30, 2025. |
| Ink        | Velodrome    | CL1-WETH/ultraETH          | Provided ≥ $0.01 in DEX liquidity for ≥ 1 hour between Jun 16, 2025, and Jun 30, 2025. |
| Unichain   | Morpho       | Re7 USD₮0                  | Deposited ≥ $0.01 into the vault for ≥ 1 hour between Jun 16, 2025, and Jun 30, 2025. |
| Unichain   | Morpho       | Gauntlet USD₮0             | Deposited ≥ $0.01 into the vault for ≥ 1 hour between Jun 16, 2025, and Jun 30, 2025. |

---
