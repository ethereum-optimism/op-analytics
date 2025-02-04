interop
=======

Objective
---------

Find how many tokens of different bridgeable types are created each day. 


Methodology
-----------

We consider 3 token types:

- SuperchainERC20
- NTT 
- OFT

### Token Contract Addresses

For each of these we first focus on transfers. We look at logs to find when the token is
transferred/sent/deliverd and to find the contract address executing the transfer.

Using the historical events (fact tables) we maintain a first seen dimension table. This table
contains the contract address for each token and the first time a related event was emitted for it.

The purpose of each dimension table is to hold a list of contract addresses that we are sure
belong to each of the three different token types.

### Contract Creation Traces

We use traces to find out when a token contract was created. 

For NTT and OFT we inner join with the token contract address dimension table to keep only the
create traces for those token contracts.

For SuperchainERC20 contracts it is more nuanced. The only events we have are transfer events, which
are the same as for regular ERC-20 tokens. So we use the contract bytecode to identify create
traces for contracts that have the `croschainBurn` and `croschainMint` methods. Then we join
with the erc20 token addresses dimension table to further verify that these are ERC-20 contracts.
The combination of being ERC-20 transferable and having the crosschain methods is what identifies
a SuperchainERC20 token.


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
