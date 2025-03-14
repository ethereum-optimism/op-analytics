INSERT INTO 08_dim_proposals_v1

SELECT
    c.proposal_id,
    c.ordinal,
    c.created_block,
    c.created_block_ts,
    c.proposal_creator,
    c.proposal_description,
    c.proposal_type,
    c.proposal_results,
    c.proposal_name,
    c.quorum_perc,
    c.proposal_type_id,
    c.approval_threshold_perc,

    s.start_block_ts,

    e.end_block_ts,
    e.total_against_votes,
    e.total_for_votes,
    e.total_abstain_votes,
    e.total_votes,
    e.pct_against,
    e.pct_for,
    e.pct_abstain,

    x.cancelled_block_ts

FROM 04_fact_proposals_created_v1 c
LEFT JOIN 05_fact_proposals_started_v1 s
    ON c.proposal_id = s.proposal_id
LEFT JOIN 06_fact_proposals_ended_v1 e
    ON c.proposal_id = e.proposal_id
LEFT JOIN 07_fact_proposals_executed_v1 x
    ON c.proposal_id = x.proposal_id