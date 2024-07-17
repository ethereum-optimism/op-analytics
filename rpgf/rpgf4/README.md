# Retroactive Public Goods Funding (Retro Funding) 4 Voting Calculator

This directory contains the scripts, files and methodology used to calculate Retro Funding allocations based on verified ballots. 

## Getting Started
 
The following directory contains the method, necessary inputs and final results from the Retro Funding voting. The scripts used for this process are written in python and are contained within a detailed notebook for review and testing. All results have been validated within different teams to ensure accurate allocations to recipient projects.

For any validation, please clone the directory and and follow the instructions detailed.

### Required inputs 

- Voter ballots
- List of verified badgeholders
- Retro Funding impact metrics by project

## Methodology

### Vote and voter authentication

In order to verify the final allocation results, the initial goal of this process starts with an authentication of:

1.  **Badgeholder address**: Only votes submitted by a badgeholder addresses should be considered
2.  **Signature verification**: All signatures of the votes need to be verified and need to return valid
3.  **Valid ballot submission**: All metric weights need to sum up to 100%, all metrics need to valid

### Project score calculation

Following input authentication, project scores are then calculated by badgeholder and finally aggregated to the final, allocation by finding the median. The exact process is as follows:
- Generating of the impact metric share by project and badgeholder
- Finding the total score by project and badgeholder
- Calculating the median allocations per project
- Final filtering and adjustments 

#### Generating impact metric share by project and badgeholder

The first step in calculation project allocation begins with calculating each projects `impact_metric_share` which is defined as the score of a project on a given metric devided by the sum of all projects scores on the metric. This result is multiplied by the open source (OS) multiplier when the project is noted as open source. This calculation is then repeated for each impact metric individually by project and badgeholder.

```
impact_metric_share = (project1_metric_1_score /Sum(project1_metric_1_scores))* IF(is_os, os_multiplier, 1)

```

For example, looking at the impact metric `gas_fees` for test project_1:

| project_name | is_oss | gas_fees | badgeholder |os_multiplier | weighted_metric | impact_metric_share |
|--|--|--|--|--|--|--|
| preject_1 | TRUE |10.3540933 | badgeholder_1 | 1.8 | = gas_fees *IF(EQ(is_oss,TRUE),1.8,1) | = weighted_metric /sum(project_1_weighted_metric)|


#### Finding the total score by project and badgeholder

Now that there is an impact metric share claculated for each metric, the next step is calculating each badgeholder's total project score. This is completed by multiplying the project's metric `impact_metric_share` by each badgeholder's impact metric weighting and adding toegther the results.

```

score(P) = impact_metric1_share*metric1_weight + impact_metric2_share*metric2_weight....

```  

The total score for each project/badgeholder combo is then capped to a maximnum score of 500k OP per project with any remaining OP above this amount distributed to remaining badgeholder's projects. The redistribution of OP is proportional to the score they've received. The capped badgeholder scores are then normalised to sum up to 10M OP. 

To faciltate the capping and normalisation process, capping and normalisation iterates through descending list of project scores. This process will be repeated during later steps so the following function is used for ease and consistency

```python
def cap_and_normalise(df, column):
	total_allocation = df[column].sum()
	adjusted_allocation = 1
	capped_list = []

	for index, row in df.iterrows():
		capped_allocation = min((row[column] * adjusted_allocation)/total_allocation,  0.05)
		total_allocation = total_allocation - row[column]
		adjusted_allocation = adjusted_allocation - capped_allocation
		capped_list.append(capped_allocation*10000000)

	return capped_list
```

#### Calculating the median allocations per project

Before calculating the median allocations per project, the adjusted badgeholder allocations are first re-capped to 500 K OP and re-normalised to 10M OP before aggregation. The median of the final values is found each project before the results are again capped and normalised.


### Final filtering and adjustments 

The closing step for the calculation of the final project allocations then involves the removal of recipients with an allocation less than 1,000 OP and the final steps of normalising the final list of allocations to 10M OP. Before the removal of smaller allocations, the list of scores is ordered, capped and normalised. The resulting list of allocations is then reordered and normalised for the last time.

In order to ensure consistent reporting of the allocations, the final figures were rounded to 1 decimal place before reporting.


## Acknowledgments

* **Jonas**, _Optimism Foundation_
* **Stepan**, _Agora_
* **Carl**, _Kariba Labs_
* **Michael**, _OP Labs_
* **Chuxin**, _OP Labs_
* **Bella**, _OP Labs_
