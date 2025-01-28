import polars as pl
from github.PullRequestReview import PullRequestReview

from .pullrequests import user_to_row

REVIEWS_SCHEMA = pl.Schema(
    [
        ("pr_number", pl.Int64),
        ("id", pl.Int64),
        ("body", pl.String),
        ("author_association", pl.String),
        ("state", pl.String),
        ("submitted_at", pl.String),
        ("user", pl.Struct({"login": pl.String, "id": pl.Int64})),
    ]
)


def review_to_row(pr_number: int, review: PullRequestReview) -> dict:
    """Extract data from review response.

    {
        "id": 2424518933,
        "node_id": "PRR_kwDOM-7cI86QgzkV",
        "user": {
            "login": "tremarkley",
            "id": 15234569,
            "node_id": "MDQ6VXNlcjE1MjM0NTY5",
            "avatar_url": "https://avatars.githubusercontent.com/u/15234569?u=b2a30602eb3a6a6fbd8e7b2ee2eef68888738704&v=4",
            "gravatar_id": "",
            "url": "https://api.github.com/users/tremarkley",
            "html_url": "https://github.com/tremarkley",
            "followers_url": "https://api.github.com/users/tremarkley/followers",
            "following_url": "https://api.github.com/users/tremarkley/following{/other_user}",
            "gists_url": "https://api.github.com/users/tremarkley/gists{/gist_id}",
            "starred_url": "https://api.github.com/users/tremarkley/starred{/owner}{/repo}",
            "subscriptions_url": "https://api.github.com/users/tremarkley/subscriptions",
            "organizations_url": "https://api.github.com/users/tremarkley/orgs",
            "repos_url": "https://api.github.com/users/tremarkley/repos",
            "events_url": "https://api.github.com/users/tremarkley/events{/privacy}",
            "received_events_url": "https://api.github.com/users/tremarkley/received_events",
            "type": "User",
            "user_view_type": "public",
            "site_admin": false
        },
        "body": "",
        "state": "APPROVED",
        "html_url": "https://github.com/ethereum-optimism/superchainerc20-starter/pull/56#pullrequestreview-2424518933",
        "pull_request_url": "https://api.github.com/repos/ethereum-optimism/superchainerc20-starter/pulls/56",
        "author_association": "MEMBER",
        "_links": {
            "html": {
                "href": "https://github.com/ethereum-optimism/superchainerc20-starter/pull/56#pullrequestreview-2424518933"
            },
            "pull_request": {
                "href": "https://api.github.com/repos/ethereum-optimism/superchainerc20-starter/pulls/56"
            }
        },
        "submitted_at": "2024-11-08T17:38:46Z",
        "commit_id": "552234515ccf440b5253564443d942d9f9e98339"
    }
    """
    return {
        "pr_number": pr_number,
        "id": review._rawData["id"],
        "body": review._rawData["body"],
        "author_association": review._rawData["author_association"],
        "state": review._rawData["state"],
        "submitted_at": review._rawData["submitted_at"],
        "user": user_to_row(review._rawData["user"]),
    }
