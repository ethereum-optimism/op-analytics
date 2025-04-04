---
myst:
  html_meta:
    "description lang=en": |
      Developer setup for the op-analytics repo
---

# Dev Setup

This section will walk you through setting up your development environment for the op-analytics
repo.


## Virtual Environment

We use [uv](https://docs.astral.sh/uv/) to manage the virtual environment and project dependencies.
After [installing uv](https://docs.astral.sh/uv/getting-started/installation/#standalone-installer)
you can create your development virtualenv by running:
```
$ uv sync
```

## Command line interface


The `opdata` CLI  (defined in `src/op_analytics/cli`) is used to expose the many utilities that are
defined as part of our project member packages. Some of this utilities are internal. For example to
help us autogenerate code, documentation, or to provide a simple way to execute some logic during
CI/CD. We also have utilities that have external use cases. For example fetching onchain data from
RPC nodes. Ideally every tool that we write in python should be exposed as a CLI subcommand.

You can see the CLI help message by running:
```
$ uv run opdata --help
```

Or get help about a specific subcommand:
```
$ uv run opdata rpc --help
```

`opdata` subcommands are defined as python modules under `src/op_analytics/cli/subcommands`. If you
want to develop a new subcommand look in there for examples of how to get things set up.


## Directory Structure

```{warning}
We are currently in the process of migrating to our new directory structure for the project.
There are a lot of directories in the top-level at the moment that we hope to reorganize
over time.
```

### `src/`

The project is structured as a [uv Workspace](https://docs.astral.sh/uv/concepts/workspaces/).
This means there is one top-level ``src/`` directory and multiple individual workspace member
packages under ``packages/``.

Any python implementation that we leverage for data work will be contained inside a package.
At the top-level we only define a command-line interface, which is the default way by which we
interact with our functionality.


### `sphinx/`

We use [sphinx](https://www.sphinx-doc.org/en/master/) to write documentation for our project.
The `sphinx` directory contains our sphinx configuration and markdown files. The html build output
is written to `docs/`. Our github-pages configuration is set up to serve a static site from the
repo root directory.  We have an `index.html` file at the root which redirects to
`docs/sphinx/html/index.html`.


### `notebooks`

Directory for storing notebooks and associated data. We are in the process of migrating contents
here. There are two subdirectories here `adhoc` and `scheduled` to distinguish between the type
of work.


## Google Cloud Credentials

Some functionality on this repo requires you to be authenticated with google cloud. You should
install the [gcloud](https://cloud.google.com/sdk/docs/install) cli and then run:
```
gcloud auth application-default login
gcloud config set project oplabs-tools-data
```

Note, if you see the following error then either you are not authenticated or you do not have
access to the required resource:
```
DefaultCredentialsError: Your default credentials were not found. To set up Application Default Credentials
```