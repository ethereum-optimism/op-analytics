SELECT
  c.proposal_id AS proposal_id,
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

  executed.executed_block_ts
  , cancelled.cancelled_block_ts
  , queued.queued_block_ts

FROM transforms_governance.fact_proposals_created_v1 AS c

LEFT JOIN transforms_governance.fact_proposals_started_v1 s
    ON c.proposal_id = s.proposal_id

LEFT JOIN transforms_governance.fact_proposals_ended_v1 e
    ON c.proposal_id = e.proposal_id

LEFT JOIN transforms_governance.fact_proposals_executed_v1 executed
    ON c.proposal_id = executed.proposal_id

LEFT JOIN transforms_governance.fact_proposals_cancelled_v1 AS  cancelled
    ON c.proposal_id = cancelled.proposal_id

LEFT JOIN transforms_governance.fact_proposals_queued_v1 AS queued
    ON c.proposal_id = queued.proposal_id    
