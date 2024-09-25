import os

import git


def repo_root():
    repo = git.Repo(search_parent_directories=True)
    return repo.git.rev_parse("--show-toplevel")


def repo_path(path: str) -> str:
    return os.path.join(repo_root(), path)
