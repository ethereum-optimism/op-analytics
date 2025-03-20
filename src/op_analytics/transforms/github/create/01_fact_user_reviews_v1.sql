CREATE TABLE IF NOT EXISTS _placeholder_
(
    `pr_number` Int64,
    `id` Int64,
    `repo` String,
    `submitted_at` DateTime,
    `author_association` String,
    `state` String,
    `user_login` String,
    INDEX submitted_at_idx submitted_at TYPE minmax GRANULARITY 1,
    INDEX user_login_idx user_login TYPE minmax GRANULARITY 1,
)
ENGINE = ReplacingMergeTree
ORDER BY (repo, submitted_at, pr_number, id)
