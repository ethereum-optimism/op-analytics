# Retroactive Public Goods Funding (Retro Funding) 5 Voting Calculator

This directory contains the scripts, files and methodology used to calculate Retro Funding allocations based on verified ballots. 

## Getting Started
 
The following directory contains the method, ballot details, dummy inputs and final results from the Retro Funding voting. The scripts used for this process are written in python and are contained within a detailed notebook for review and testing. All results have been validated within different teams to ensure accurate allocations to recipient projects.

For any validation, please clone the directory and follow the instructions detailed.

### Required inputs 

- Voter ballots

## Methodology

### Vote and voter authentication

In order to verify the final allocation results, the initial goal of this process starts with an authentication of:

1.  **Badgeholder address**: Only votes submitted by a badgeholder addresses are considered
2.  **Signature/Attestation verification**: All vote signatures must be verified and return as valid
3.  **Valid ballot submission**: All category and project allocations must sum to 100, and only projects without conflicts of interest (COIs) should receive votes

### Round allocation calculation

Each badgeholder provides their ideal total amount for RPGF Round 5. The choices made ranged between 2M and 8M OP, with only increments of 50k allowed. The round allocations are gathered from the ballots and the median found is used as the final total amount for calculating category and project allocations.


### Category allocation calculation

The category budget for this round was decided through an aggregation of each badgeholder's vote. Each badgeholder votes on how to allocate OP to all categories (e.g. [Category1: 33%; Category2: 33%; Category3: 34%]). These are isolated form the ballot and the median of each category is calculated. Following this calculation, the category allocations are adjusted to make sure they add up 100%.

### Project score calculation

Following input authentication and allocation confirmation, the project scores are then calculated by badgeholder and finally aggregated to the final allocation by finding the median. The exact process is as follows:
1. Isolating the project votes
2. Removing project votes with a COI
3. Calculate the median scores of projects
4. Adjust the scores to ensure their sum is equal to 100%
5. Ensuring this process occurs for all three categories

### Calculating the final result

The final project allocations were determined using a weighted proportional distribution that involved:
1. Calculate total allocation for each category
2. Sort projects within each category
3. Calculate project allocations using weighted proportional distribution
4. Remove projects below with an allocation below the minimum amount
5. Normalize allocations and then cap the new allocations at then maximum amount. The results should be re-normalised as needed

In order to ensure consistent reporting of the allocations, the final figures were rounded to 2 decimal place before reporting.


## Acknowledgments

* **Jonas**, _Optimism Foundation_
* **Emily**, _Optimism Foundation_
* **Stepan**, _Agora_
* **Carl**, _OS Observer_
* **Dennis**, _OP Labs_
* **Bella**, _OP Labs_


