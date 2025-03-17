SELECT
  pr_number
  , id
  , repo
  , parseDateTime(submitted_at,'%Y-%m-%dT%H:%i:%sZ') AS submitted_at
  , author_association
  , state
  , user.login AS user_login
FROM
  dailydata_gcs.read_date(
    rootpath = 'github/github_pr_reviews_v2',
    dt = { dtparam: Date }
)
