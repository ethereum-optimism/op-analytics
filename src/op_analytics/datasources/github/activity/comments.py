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
    """Extract data from comment response.

    Example response:

    {
        "url": "https://api.github.com/repos/ethereum-optimism/supersim/pulls/comments/1887274724",
        "pull_request_review_id": 2506911434,
        "id": 1887274724,
        "node_id": "PRRC_kwDOMMiGhs5wfYbk",
        "diff_hunk": "@@ -95,6 +97,12 @@ func BaseCLIFlags(envPrefix string) []cli.Flag {\n \t\t\tUsage:   \"Delay before relaying messages sent to the L2ToL2CrossDomainMessenger\",\n \t\t\tEnvVars: opservice.PrefixEnvVar(envPrefix, \"INTEROP_DELAY\"),\n \t\t},\n+\t\t&cli.BoolFlag{",
        "path": "config/cli.go",
        "commit_id": "7cb43f56e029924623ee5472042be50a329fb199",
        "original_commit_id": "eae38020e2dbbd2aabef9ffd7f5ac47d3417d33f",
        "user": {
            "login": "tremarkley",
            "id": 15234569,
            "node_id": "MDQ6VXNlcjE1MjM0NTY5",
            "avatar_url": "https://avatars.githubusercontent.com/u/15234569?v=4",
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
        "body": "nit: can we add documentation on this flag to the README or maybe a link to the docs in the help? I just dont think many devs will know what this flag does just by the name",
        "created_at": "2024-12-16T18:02:22Z",
        "updated_at": "2024-12-16T18:02:26Z",
        "html_url": "https://github.com/ethereum-optimism/supersim/pull/302#discussion_r1887274724",
        "pull_request_url": "https://api.github.com/repos/ethereum-optimism/supersim/pulls/302",
        "author_association": "MEMBER",
        "_links": {
            "self": {
            "href": "https://api.github.com/repos/ethereum-optimism/supersim/pulls/comments/1887274724"
            },
            "html": {
            "href": "https://github.com/ethereum-optimism/supersim/pull/302#discussion_r1887274724"
            },
            "pull_request": {
            "href": "https://api.github.com/repos/ethereum-optimism/supersim/pulls/302"
            }
        },
        "reactions": {
            "url": "https://api.github.com/repos/ethereum-optimism/supersim/pulls/comments/1887274724/reactions",
            "total_count": 1,
            "+1": 1,
            "-1": 0,
            "laugh": 0,
            "hooray": 0,
            "confused": 0,
            "heart": 0,
            "rocket": 0,
            "eyes": 0
        },
        "start_line": null,
        "original_start_line": null,
        "start_side": null,
        "line": 100,
        "original_line": 100,
        "side": "RIGHT",
        "original_position": 13,
        "position": 13,
        "subject_type": "line"
    }

    """
    return {
        "pr_number": pr_number,
        "id": comment._rawData["id"],
        "body": comment._rawData["body"],
        "author_association": comment._rawData["author_association"],
        "created_at": comment._rawData["created_at"],
        "updated_at": comment._rawData["updated_at"],
        "user": user_to_row(comment._rawData["user"]),
    }
