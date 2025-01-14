import polars as pl
from github.PullRequest import PullRequest

PRS_SCHEMA = pl.Schema(
    [
        ("html_url", pl.String),
        ("number", pl.Int64),
        ("state", pl.String),
        (
            "labels",
            pl.List(
                pl.Struct(
                    {
                        "id": pl.Int64,
                        "name": pl.String,
                        "description": pl.String,
                    }
                )
            ),
        ),
        ("draft", pl.Boolean),
        ("created_at", pl.String),
        ("updated_at", pl.String),
        ("closed_at", pl.String),
        ("merged_at", pl.String),
        ("author_association", pl.String),
        (
            "user",
            pl.Struct(
                {
                    "login": pl.String,
                    "id": pl.Int64,
                }
            ),
        ),
        ("title", pl.String),
        ("body", pl.String),
        (
            "base",
            pl.Struct(
                {
                    "label": pl.String,
                    "ref": pl.String,
                }
            ),
        ),
        (
            "head",
            pl.Struct(
                {
                    "label": pl.String,
                    "ref": pl.String,
                }
            ),
        ),
        (
            "assignee",
            pl.Struct(
                {
                    "login": pl.String,
                    "id": pl.Int64,
                }
            ),
        ),
        (
            "assignees",
            pl.List(
                pl.Struct(
                    {
                        "login": pl.String,
                        "id": pl.Int64,
                    }
                )
            ),
        ),
        (
            "requested_reviewers",
            pl.List(
                pl.Struct(
                    {
                        "login": pl.String,
                        "id": pl.Int64,
                    }
                )
            ),
        ),
        (
            "requested_teams",
            pl.List(
                pl.Struct(
                    {
                        "id": pl.Int64,
                        "name": pl.String,
                        "parent": pl.Struct(
                            {
                                "id": pl.Int64,
                                "name": pl.String,
                            }
                        ),
                    }
                )
            ),
        ),
    ]
)


def user_to_row(user: dict | None) -> dict | None:
    if user is None:
        return None
    return {
        "login": user["login"],
        "id": user["id"],
    }


def team_to_row(team: dict | None) -> dict | None:
    if team is None:
        return None

    team_parent = team["parent"]
    if team_parent is not None:
        parent = {
            "id": team["parent"]["id"],
            "name": team["parent"]["name"],
        }
    else:
        parent = None

    return {
        "id": team["id"],
        "name": team["name"],
        "parent": parent,
    }


def label_to_row(label: dict) -> dict:
    return {
        "id": label["id"],
        "name": label["name"],
        "description": label["description"],
    }


def pr_to_row(pr: PullRequest) -> dict:
    """Defines the schema that we extract from Github API PullRequest responses."""

    return {
        "html_url": pr._rawData["html_url"],
        "number": pr._rawData["number"],
        "state": pr._rawData["state"],
        "labels": [label_to_row(_) for _ in pr._rawData["labels"]],
        "draft": pr._rawData["draft"],
        #
        # Timestamps
        "created_at": pr._rawData["created_at"],
        "updated_at": pr._rawData["updated_at"],
        "closed_at": pr._rawData["closed_at"],
        "merged_at": pr._rawData["merged_at"],
        #
        # Author
        "author_association": pr._rawData["author_association"],
        "user": user_to_row(pr._rawData["user"]),
        #
        # Details
        "title": pr._rawData["title"],
        "body": pr._rawData["body"],
        #
        # Base
        "base": {
            "label": pr._rawData["base"]["label"],
            "ref": pr._rawData["base"]["ref"],
        },
        #
        # Head
        "head": {
            "label": pr._rawData["head"]["label"],
            "ref": pr._rawData["head"]["ref"],
        },
        #
        # Reviewers
        "assignee": user_to_row(pr._rawData["assignee"]),
        "assignees": [user_to_row(_) for _ in pr._rawData["assignees"]],
        "requested_reviewers": [user_to_row(_) for _ in pr._rawData["requested_reviewers"]],
        "requested_teams": [team_to_row(_) for _ in pr._rawData["requested_teams"]],
    }
