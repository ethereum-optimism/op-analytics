import polars as pl
from github.PullRequestComment import PullRequestComment

from .pullrequests import user_to_row

COMMENTS_SCHEMA = pl.Schema(
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


def comment_to_row(pr_number: int, comment: PullRequestComment) -> dict:
    return {
        "pr_number": pr_number,
        "id": comment._rawData["id"],
        "body": comment._rawData["body"],
        "author_association": comment._rawData["author_association"],
        "created_at": comment._rawData["created_at"],
        "updated_at": comment._rawData["updated_at"],
        "user": user_to_row(comment._rawData["user"]),
    }
