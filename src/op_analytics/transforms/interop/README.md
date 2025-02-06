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

If a contract emits both an ERC-20 `Transfer` event and a protocol's interop event in the same
transaction we consider it an interop-enabled token. 

At the moment we are tracking the [NTT](https://wormhole.com/docs/learn/messaging/native-token-transfers/overview/)
and [OFT](https://docs.layerzero.network/v2/home/token-standards/oft-standard) interop protocols. 


We first store raw NTT (`Delivery`) and OFT (`OFTSent`) logs on dedicated `fact_` tables. We then
find ERC-20 transfers where these interop events also show up in the transaction. A `dim_` table
helps us track distinct addresses of interop-enabled tokens and the first time they were seen:

| Token Type  | Event      | Fact Table                                         | First Seen Table                           |
| ----------- | ---------- | -------------------------------------------------- | ------------------------------------------ |
| NTT         | Delivery   | transforms_interop.fact_ntt_delivery_events_v1     | transforms_interop.dim_erc20_with_ntt_first_seen_v1   |
| OFT         | OFTSent    | transforms_interop.fact_oft_sent_events_v1         | transforms_interop.dim_erc20_with_oft_first_seen_v1   |


### Contract Creation Traces

We consider the set of contract creation traces that create ERC-20 Tokens. For each ERC-20
token we add the following boolean flags.

| Flag        | Description |
| ------------| ----------- |
| is_erc7802  | Does this contract have the `croschainBurn` and `croschainMint` methods? |
| has_erc20   | Have we seen this contract emit an ERC-20 `Transfer` event? |
| has_oft     | Have we seen this contract emit an OFT `OFTSent` event? |
| has_ntt     | Have we seen this contract emit an NTT `Delivery` event? |


To efficiently compute these boolean flags we rely on the dimension tables described on the
previous section.

Note that the SuperchainERC20 token type is a subset of ERC-7802 where permissions to 
`crosschainBurn` and `crosschainMint` are given to a specific bridge address. We are still 
investigating how to identify these. For now all we know is that SuperchainERC20 will be a
subset of `is_erc7802`.


Time Window
-----------

We report for a period of time starting on 2024/10/01 until the present. For that period of time
we have to first accumulate all of the distinct token contract addresses into the dimension tables.
With the dimension tables ready we go ahead and scan the contract creation traces for the full
time range.

The strict approach defined above requires that we reprocess the entire range of creation traces
on each day, since every day we get new transfers that may affect the boolean flags for a contract
creation trace from way in the past.

Since the creation trace data is in Clickhouse and the dimension tables are fairly small we find
that rescanning the entire window from 2024/10/01 takes ~ 30s to run. 

If rescanning the entire window were more intensive we could consider capping the time between
contract creation and first transfer to e.g. 7 days. Luckily we don't have to do that for the time
being, doing so would make life more complicated.


Export
------

A single table with the ERC-20 contract creation traces and the boolean flags is exported to
GCS and BigQuery to power our interop dashboards and metrics.

The exported table covers the full time range from 2024/10/01 up until the date on which the export
is executed.
