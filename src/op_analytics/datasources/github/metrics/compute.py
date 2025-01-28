import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.time import parse_isoformat

log = structlog.get_logger()


def compute_pr_metrics(
    prs_df: pl.DataFrame,
    comments_df: pl.DataFrame,
    reviews_df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Compute daily GitHub PR metrics using three dataframes (PRs, Comments, Reviews).

    1) PRs (prs_df)
       Schema({
           "html_url": pl.Utf8,
           "number": pl.Int64,
           "state": pl.Utf8,
           "labels": pl.List(
               pl.Struct({
                   "id": pl.Int64,
                   "name": pl.Utf8,
                   "description": pl.Utf8
               })
           ),
           "draft": pl.Boolean,
           "created_at": pl.Utf8,
           "updated_at": pl.Utf8,
           "closed_at": pl.Utf8,
           "merged_at": pl.Utf8,
           "author_association": pl.Utf8,
           "user": pl.Struct({"login": pl.Utf8, "id": pl.Int64}),
           "title": pl.Utf8,
           "body": pl.Utf8,
           "base": pl.Struct({"label": pl.Utf8, "ref": pl.Utf8}),
           "head": pl.Struct({"label": pl.Utf8, "ref": pl.Utf8}),
           "assignee": pl.Struct({"login": pl.Utf8, "id": pl.Int64}),
           "assignees": pl.List(pl.Struct({"login": pl.Utf8, "id": pl.Int64})),
           "requested_reviewers": pl.List(pl.Struct({"login": pl.Utf8, "id": pl.Int64})),
           "requested_teams": pl.List(
               pl.Struct({
                   "id": pl.Int64,
                   "name": pl.Utf8,
                   "parent": pl.Struct({"id": pl.Int64, "name": pl.Utf8})
               })
           ),
           "repo": pl.Utf8,
           "dt": pl.Date
       })

    2) Comments (comments_df)
       Schema({
           "pr_number": pl.Int64,
           "id": pl.Int64,
           "body": pl.Utf8,
           "author_association": pl.Utf8,
           "created_at": pl.Utf8,
           "updated_at": pl.Utf8,
           "user": pl.Struct({"login": pl.Utf8, "id": pl.Int64}),
           "repo": pl.Utf8,
           "dt": pl.Date
       })

    3) Reviews (reviews_df)
       Schema({
           "pr_number": pl.Int64,
           "id": pl.Int64,
           "body": pl.Utf8,
           "author_association": pl.Utf8,
           "state": pl.Utf8,           # e.g. "APPROVED", "COMMENTED", "CHANGES_REQUESTED"
           "submitted_at": pl.Utf8,    # Time the review was submitted
           "user": pl.Struct({"login": pl.Utf8, "id": pl.Int64}),
           "repo": pl.Utf8,
           "dt": pl.Date
       })

    The goal is to compute daily PR metrics for each (repo, dt) partition:
    ----------------------------------------------------------------------
      - number_of_prs
      - avg_time_to_approval_days
      - avg_time_to_first_non_bot_comment_days
      - avg_time_to_merge_days
      - approval_ratio       (# PRs with an approval vs total)
      - avg_comments_per_pr
      - merged_ratio         (# PRs merged vs total)
      - active_contributors  (unique logins who opened PRs on that date)

    Steps:
    ------
    1) Convert relevant PR datetime columns to Polars Datetime (created_at, merged_at).
    2) Convert comment created_at.
    3) Convert review submitted_at.
    4) Compute earliest approval for each PR from reviews.
    5) Compute earliest non-bot comment for each PR from comments.
    6) Compute intervals from PR creation (time_to_approval_days, time_to_first_non_bot_comment_days, time_to_merge_days).
    7) Count comments per PR.
    8) Aggregate daily metrics at (repo, dt).
    """

    # 1. Convert PR 'created_at' & 'merged_at' to polars.Datetime
    prs_df = prs_df.with_columns(
        [
            pl.col("created_at")
            .map_elements(parse_isoformat, return_dtype=pl.Datetime)
            .alias("created_at"),
            pl.col("merged_at")
            .map_elements(parse_isoformat, return_dtype=pl.Datetime)
            .alias("merged_at"),
        ]
    )

    # 2. Convert comment 'created_at' to polars.Datetime
    comments_df = comments_df.with_columns(
        [
            pl.col("created_at")
            .map_elements(parse_isoformat, return_dtype=pl.Datetime)
            .alias("created_at")
        ]
    )

    # 3. Convert review 'submitted_at' to polars.Datetime
    reviews_df = reviews_df.with_columns(
        [
            pl.col("submitted_at")
            .map_elements(parse_isoformat, return_dtype=pl.Datetime)
            .alias("submitted_at")
        ]
    )

    # 4. Compute earliest approval from reviews (where state = "APPROVED")
    #    For each (repo, pr_number): earliest "submitted_at" => "approved_at"
    approved_reviews = reviews_df.filter(pl.col("state").str.to_uppercase() == "APPROVED")
    earliest_approval = approved_reviews.group_by(["repo", "pr_number"]).agg(
        pl.col("submitted_at").min().alias("approved_at")
    )

    # Join earliest approval into PR data, matching pr_number -> number
    enriched_prs = prs_df.join(
        earliest_approval.rename({"pr_number": "number"}),
        on=["repo", "number"],
        how="left",
    )

    # 5. Compute earliest non-bot comment
    #    Filter out comments whose user.login ends with [bot]
    non_bot_comments = comments_df.filter(
        pl.col("user").struct.field("login").str.contains(r"(?i)\[bot\]$").not_()
    )

    earliest_non_bot_comment = non_bot_comments.group_by(["repo", "pr_number"]).agg(
        pl.col("created_at").min().alias("earliest_non_bot_comment_at")
    )

    # Join earliest non-bot comment into PR data
    enriched_prs = enriched_prs.join(
        earliest_non_bot_comment.rename({"pr_number": "number"}),
        on=["repo", "number"],
        how="left",
    )

    # 6. Compute intervals from PR creation date
    #    - time_to_approval_days  = (approved_at - created_at).days
    #    - time_to_first_non_bot_comment_days
    #    - time_to_merge_days
    enriched_prs = enriched_prs.with_columns(
        [
            ((pl.col("approved_at") - pl.col("created_at")).dt.total_days()).alias(
                "time_to_approval_days"
            ),
            ((pl.col("earliest_non_bot_comment_at") - pl.col("created_at")).dt.total_days()).alias(
                "time_to_first_non_bot_comment_days"
            ),
            ((pl.col("merged_at") - pl.col("created_at")).dt.total_days()).alias(
                "time_to_merge_days"
            ),
        ]
    )

    # Booleans for 'is_approved', 'is_merged'
    enriched_prs = enriched_prs.with_columns(
        [
            pl.col("approved_at").is_not_null().alias("is_approved"),
            pl.col("merged_at").is_not_null().alias("is_merged"),
        ]
    )

    # 7. Count comments per PR
    #    For each (repo, pr_number) => total # of comments
    comment_counts = comments_df.group_by(["repo", "pr_number"]).agg(
        pl.count().alias("comment_count")
    )

    enriched_prs = enriched_prs.join(
        comment_counts.rename({"pr_number": "number"}),
        on=["repo", "number"],
        how="left",
    ).with_columns(pl.col("comment_count").fill_null(0))

    # 8. Aggregate daily metrics at (repo, dt)
    daily_metrics = (
        enriched_prs.group_by(["repo", "dt"])
        .agg(
            [
                pl.count().alias("number_of_prs"),
                pl.col("time_to_approval_days").mean().alias("avg_time_to_approval_days"),
                pl.col("time_to_first_non_bot_comment_days")
                .mean()
                .alias("avg_time_to_first_non_bot_comment_days"),
                pl.col("time_to_merge_days").mean().alias("avg_time_to_merge_days"),
                (pl.col("is_approved").sum() / pl.count()).alias("approval_ratio"),
                (pl.col("comment_count").sum() / pl.count()).alias("avg_comments_per_pr"),
                (pl.col("is_merged").sum() / pl.count()).alias("merged_ratio"),
                pl.col("user").struct.field("login").n_unique().alias("active_contributors"),
            ]
        )
        .sort(["repo", "dt"])
    )

    return daily_metrics
