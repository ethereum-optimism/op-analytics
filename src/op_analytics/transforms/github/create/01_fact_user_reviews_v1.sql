CREATE TABLE IF NOT EXISTS _placeholder_
(
    `pr_number` Int64,
    `id` Int64,
    `repo` String,
    `submitted_at` DateTime,
    `author_association` String,
    `state` String,
    `user_login` String
)
ENGINE = ReplacingMergeTree
ORDER BY (pr_number, id, repo, submitted_at)
