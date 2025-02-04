interop
=======

Objective
---------

Find how many tokens of different bridgeable types are created each day. 


Methodology
-----------

We consider 3 token types:

- ERC7802
- NTT 
- OFT

### Token Contract Addresses

We filter raw logs to find when the token is transferred/sent/delivered and to find the contract
address executing the action. The filtered logs are stored on the `fact_` tables for each event
type. We consider the following token types:

Note that ERC20 transfers events are already available on the 
`blockbatch.token_transfers__erc20_transfers_v1` table so we reuse that instead of maintaing a
corresponding `transforms_interop` fact table.

For each of the event types tracked we maintain a dimension table to keep track of the first
time an event was observed for each token contract.

| Token Type  | Event      | Fact Table                                         | First Seen Table                           |
| ----------- | ---------- | -------------------------------------------------- | ------------------------------------------ |
| ERC20       | Transfer   | blockbatch.token_transfers__erc20_transfers_v1     | transforms_interop.dim_erc20_first_seen_v1 |
| NTT         | Delivery   | transforms_interop.fact_ntt_delivery_events_v1     | transforms_interop.dim_ntt_first_seen_v1   |
| OFT         | OFTSent    | transforms_interop.fact_oft_sent_events_v1         | transforms_interop.dim_oft_first_seen_v1   |

The dimension tables help us categorize the token contracts.

The purpose of each dimension table is to hold a list of contract addresses that we are sure
belong to each of the three different token types.

### Contract Creation Traces

For NTT and OFT we inner join all create traces with the token type first seen table to find the
creation trace for each token contract.

For ERC-7802 the analysis is more nuanced. ERC-7802 transfers are regular ERC-20 `Transfer` events. 
So we start by filtering create traces to identify contracts that have the `croschainBurn` and 
`croschainMint` methods and then join the crosschain-enabled traces with the 
`dim_erc20_first_seen_v1` dimension table to narrow down to contracts that are also transferrable
tokens.

The SuperchainERC20 token type is a subset of ERC-7802 where permissiosn to `crosschainBurn` and
`crosschainMint` are given to a specific bridge address. We are still investigating how to identify
these.


Time Window
-----------

We report for a period of time starting on 2024/10/01 until the present. For that period of time
we have to first accumulate all of the distinct token contract addresses into the dimension tables.
With the dimension tables ready we go ahead and scan the contract creation traces for the full
time range.

The strict approach defined above requires that we reprocess the entire range of creation traces
on each day, since every day we get new transfers data that may affect our knowledge about a
contract creation trace from way in the past.

Since the creation trace data is in Clickhouse and the dimension tables are fairly small we find
that rescanning the entire window from 2024/10/01 takes ~ 30s to run. 

If rescanning the entire window were more intensive we could consider capping the time between
contract creation and first transfer to e.g. 7 days. Luckily we don't have to do that for the time
being, doing so would make life more complicated.


Export
------

A single table including the contract creation traces for the identified token contracts of all
three different types is exported.

The exported table covers the full time range from 2024/10/01 up until the date on which the export
is executed.
