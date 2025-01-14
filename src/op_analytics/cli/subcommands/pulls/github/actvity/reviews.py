import polars as pl
from github.PullRequestReview import PullRequestReview

from .pullrequests import user_to_row

REVIEWS_SCHEMA = pl.Schema(
    [
        ("pr_number", pl.Int64),
        ("id", pl.Int64),
        ("body", pl.String),
        ("author_association", pl.String),
        ("created_at", pl.String),
        ("updated_at", pl.String),
        ("user", pl.Struct({"login": pl.String, "id": pl.Int64})),
    ]
)


def review_to_row(pr_number: int, review: PullRequestReview) -> dict:
    return {
        "pr_number": pr_number,
        "id": review._rawData["id"],
        "body": review._rawData["body"],
        "author_association": review._rawData["author_association"],
        "state": review._rawData["state"],
        "submitted_at": review._rawData["submitted_at"],
        "user": user_to_row(review._rawData["user"]),
    }
