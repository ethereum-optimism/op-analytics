import polars as pl
from github.Issue import Issue

from .pullrequests import label_to_row, user_to_row

ISSUES_SCHEMA = pl.Schema(
    [
        ("html_url", pl.String),
        ("number", pl.Int64),
        ("state", pl.String),
        (
            "labels",
            pl.List(pl.Struct({"id": pl.Int64, "name": pl.String, "description": pl.String})),
        ),
        ("created_at", pl.String),
        ("updated_at", pl.String),
        ("closed_at", pl.String),
        ("author_association", pl.String),
        ("user", pl.Struct({"login": pl.String, "id": pl.Int64})),
        ("title", pl.String),
        ("body", pl.String),
        ("comments", pl.Int64),
    ]
)


def issue_to_row(pr: Issue) -> dict:
    """Defines the schema that we extract from Github API PullRequest responses."""

    return {
        "html_url": pr._rawData["html_url"],
        "number": pr._rawData["number"],
        "state": pr._rawData["state"],
        "labels": [label_to_row(_) for _ in pr._rawData["labels"]],
        #
        # Timestamps
        "created_at": pr._rawData["created_at"],
        "updated_at": pr._rawData["updated_at"],
        "closed_at": pr._rawData["closed_at"],
        #
        # Author
        "author_association": pr._rawData["author_association"],
        "user": user_to_row(pr._rawData["user"]),
        #
        # Details
        "title": pr._rawData["title"],
        "body": pr._rawData["body"],
        #
        # Comments
        "comments": pr._rawData.get("comments"),
    }
