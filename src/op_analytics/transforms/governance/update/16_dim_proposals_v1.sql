SELECT
  c.proposal_id AS proposal_id
  , c.ordinal
  , c.created_block
  , c.created_block_ts
  , c.proposal_creator
  , c.proposal_description
  , c.proposal_type
  , c.proposal_results
  , c.proposal_name
  , c.quorum_perc
  , c.proposal_type_id
  , c.approval_threshold_perc

  , started.start_block_ts

  , ended.end_block_ts
  , ended.total_against_votes
  , ended.total_for_votes
  , ended.total_abstain_votes
  , ended.total_votes
  , ended.pct_against
  , ended.pct_for
  , ended.pct_abstain

  , executed.executed_block_ts
  , cancelled.cancelled_block_ts
  , queued.queued_block_ts

FROM transforms_governance.fact_proposals_created_v1 AS c

LEFT JOIN transforms_governance.fact_proposals_started_v1 AS started
  ON c.proposal_id = started.proposal_id

LEFT JOIN transforms_governance.fact_proposals_ended_v1 AS ended
  ON c.proposal_id = ended.proposal_id

LEFT JOIN transforms_governance.fact_proposals_executed_v1 AS executed
  ON c.proposal_id = executed.proposal_id

LEFT JOIN transforms_governance.fact_proposals_cancelled_v1 AS cancelled
  ON c.proposal_id = cancelled.proposal_id

LEFT JOIN transforms_governance.fact_proposals_queued_v1 AS queued
  ON c.proposal_id = queued.proposal_id
